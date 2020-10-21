package cloudgateproxy

import (
	"context"
	"errors"
	"fmt"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"sync"
	"time"
)

type ClusterConnectionInfo struct {
	ipAddress         string
	port              int
	isOriginCassandra bool
	username          string
	password          string
}

type ClusterType string

const (
	OriginCassandra = ClusterType("originCassandra")
	TargetCassandra = ClusterType("targetCassandra")
)

type ClusterConnector struct {
	connection              net.Conn
	clusterResponseChannels map[int16]chan *Frame // map of channels, keyed on streamID, on which to send the response to a request
	clusterType             ClusterType

	username                string
	password                string
	lock                    *sync.RWMutex // TODO do we need a lock here?
	metricsHandler          metrics.IMetricsHandler
	waitGroup               *sync.WaitGroup
	clientHandlerContext    context.Context
	clientHandlerCancelFunc context.CancelFunc
}

func NewClusterConnectionInfo(ipAddress string, port int, isOriginCassandra bool, username string, password string) *ClusterConnectionInfo {
	return &ClusterConnectionInfo{
		ipAddress:         ipAddress,
		port:              port,
		isOriginCassandra: isOriginCassandra,
		username:          username,
		password:          password,
	}
}

func NewClusterConnector(connInfo *ClusterConnectionInfo,
	metricsHandler metrics.IMetricsHandler,
	waitGroup *sync.WaitGroup,
	clientHandlerContext context.Context,
	clientHandlerCancelFunc context.CancelFunc) (*ClusterConnector, error) {

	var clusterType ClusterType
	if connInfo.isOriginCassandra {
		clusterType = OriginCassandra
	} else {
		clusterType = TargetCassandra
	}

	conn, err := openConnectionToCluster(connInfo, clientHandlerContext, metricsHandler)
	if err != nil {
		return nil, err
	}

	go func() {
		<-clientHandlerContext.Done()
		closeConnectionToCluster(conn, clusterType, metricsHandler)
	}()

	return &ClusterConnector{
		connection:              conn,
		clusterResponseChannels: make(map[int16]chan *Frame),
		clusterType:             clusterType,
		username:                connInfo.username,
		password:                connInfo.password,
		lock:                    &sync.RWMutex{},
		metricsHandler:          metricsHandler,
		waitGroup:               waitGroup,
		clientHandlerContext:    clientHandlerContext,
		clientHandlerCancelFunc: clientHandlerCancelFunc,
	}, nil
}

func (cc *ClusterConnector) run() {
	cc.runResponseListeningLoop()
}

func openConnectionToCluster(connInfo *ClusterConnectionInfo, context context.Context, metricsHandler metrics.IMetricsHandler) (net.Conn, error) {
	conn, err := establishConnection(connInfo.getConnectionString(), context)
	if err != nil {
		return nil, err
	}

	if connInfo.isOriginCassandra {
		metricsHandler.IncrementCountByOne(metrics.OpenOriginConnections)
	} else {
		metricsHandler.IncrementCountByOne(metrics.OpenTargetConnections)
	}
	return conn, nil
}

func closeConnectionToCluster(conn net.Conn, clusterType ClusterType, metricsHandler metrics.IMetricsHandler) {
	err := conn.Close()
	if err != nil {
		log.Warnf("error closing connection to %s", conn.RemoteAddr().String())
	}

	if clusterType == OriginCassandra {
		metricsHandler.DecrementCountByOne(metrics.OpenOriginConnections)
	} else {
		metricsHandler.DecrementCountByOne(metrics.OpenTargetConnections)
	}

}

/**
 *	Starts a long-running loop that listens for replies being sent by the cluster
 */
func (cc *ClusterConnector) runResponseListeningLoop() {

	cc.waitGroup.Add(1)
	log.Debugf("Listening to replies sent by node %s", cc.connection.RemoteAddr())
	go func() {
		defer cc.waitGroup.Done()
		for {
			response, err := readAndParseFrame(cc.connection, cc.clientHandlerContext)

			if err != nil {
				if err == ShutdownErr {
					return
				}

				if err == io.EOF {
					log.Infof("in runResponseListeningLoop: %s disconnected", cc.connection.RemoteAddr())
				} else {
					log.Errorf("in runResponseListeningLoop: error reading: %s", err)
				}

				cc.clientHandlerCancelFunc()
				break
			}

			log.Debugf(
				"Received response from %s (%s), opcode=%d, stream id=%d",
				cc.clusterType, cc.connection.RemoteAddr(), response.Opcode, response.StreamId)
			log.Tracef("Response content: %v", string(*&response.RawBytes))
			cc.forwardResponseToChannel(response)
		}
	}()
}

func (cc *ClusterConnector) forwardResponseToChannel(response *Frame) {
	cc.lock.RLock()
	defer cc.lock.RUnlock()
	if responseChannel, ok := cc.clusterResponseChannels[response.StreamId]; !ok {
		select {
		case <-cc.clientHandlerContext.Done():
			return
		default:
			log.Errorf("could not find stream id %d in clusterResponseChannels for cluster %v", response.StreamId, cc.clusterType)
		}
	} else {
		// Note: the boolean response is sent on the channel here - this will unblock the forwardToCluster goroutine waiting on this
		responseChannel <- response
	}
}

/**
 *	Submits the request on cluster connection.
 *	Sends the response to the request being handled to the caller (handleRequest) on the channel responseToCallerChan.
 *	Adds a channel to a map (clusterResponseChannels) keyed on streamID. This channel is used by the dequeuer to communicate the response back to this goroutine.
 *	It is this goroutine that has to receive the response, so it can enforce the timeout in case of connection disruption
 */
func (cc *ClusterConnector) forwardToCluster(rawBytes []byte, streamId int16) chan *Frame {
	responseToCallerChan := make(chan *Frame, 1)

	go func() {
		defer close(responseToCallerChan)

		responseFromClusterChan, err := cc.createChannelForClusterResponse(streamId)
		if err != nil {
			log.Errorf("Error creating cluster response channel for stream id %d: %s", streamId, err.Error())
			return
		}

		// once the response has been sent to the caller, remove the channel from the map as it has served its purpose
		defer cc.deleteChannelForClusterResponse(streamId)

		err = cc.sendRequestToCluster(rawBytes)
		if err != nil {
			log.Errorf("Error while sending request to %s: %s", cc.connection.RemoteAddr().String(), err)
			return
		}

		select {
		case <-cc.clientHandlerContext.Done():
			return
		case response, ok := <-responseFromClusterChan:
			if !ok {
				log.Debugf("response from cluster channel was closed, connection: %s", cc.connection.RemoteAddr().String())
				return
			}
			log.Tracef("Received response from %s for query with stream id %d", cc.clusterType, response.StreamId)
			responseToCallerChan <- response
		case <-time.After(queryTimeout):
			log.Debugf("Timeout for query %d from %s", streamId, cc.clusterType)
			if cc.clusterType == OriginCassandra {
				cc.metricsHandler.IncrementCountByOne(metrics.TimeOutsProxyOrigin)
			} else {
				cc.metricsHandler.IncrementCountByOne(metrics.TimeOutsProxyTarget)
			}
		}
	}()

	return responseToCallerChan
}

/**
 *	Creates channel on which the dequeuer will send the response to the request with this streamId and adds it to the map
 */
func (cc *ClusterConnector) createChannelForClusterResponse(streamId int16) (chan *Frame, error) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	if _, ok := cc.clusterResponseChannels[streamId]; ok {
		return nil, errors.New(fmt.Sprintf("streamid collision: %d", streamId))
	}

	cc.clusterResponseChannels[streamId] = make(chan *Frame, 1)
	return cc.clusterResponseChannels[streamId], nil
}

/**
 *	Removes the response channel for this streamId from the map
 */
func (cc *ClusterConnector) deleteChannelForClusterResponse(streamId int16) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	if channel, ok := cc.clusterResponseChannels[streamId]; ok {
		close(channel)
		delete(cc.clusterResponseChannels, streamId)
	} else {
		log.Warnf("could not find cluster response channel for streamid %d, skipping...", streamId)
	}
}

func (cc *ClusterConnector) sendRequestToCluster(rawBytes []byte) error {
	log.Debugf("Executing %x on cluster with address %v, len=%d", rawBytes[:9], cc.connection.RemoteAddr(), len(rawBytes))
	err := writeToConnection(cc.connection, rawBytes)
	return err
}

func (cci *ClusterConnectionInfo) getConnectionString() string {
	return fmt.Sprintf("%s:%d", cci.ipAddress, cci.port)
}
