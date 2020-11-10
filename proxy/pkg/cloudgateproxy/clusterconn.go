package cloudgateproxy

import (
	"context"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
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
}

type ClusterType string

const (
	OriginCassandra = ClusterType("originCassandra")
	TargetCassandra = ClusterType("targetCassandra")
)

type ClusterConnector struct {
	connection              net.Conn
	clusterResponseChannels map[int16]chan *frame.RawFrame // map of channels, keyed on streamID, on which to send the response to a request
	clusterType             ClusterType

	clusterConnEventsChan   chan *frame.RawFrame
	lock                    *sync.RWMutex
	metricsHandler          metrics.IMetricsHandler
	waitGroup               *sync.WaitGroup
	clientHandlerContext    context.Context
	clientHandlerCancelFunc context.CancelFunc
}

func NewClusterConnectionInfo(ipAddress string, port int, isOriginCassandra bool) *ClusterConnectionInfo {
	return &ClusterConnectionInfo{
		ipAddress:         ipAddress,
		port:              port,
		isOriginCassandra: isOriginCassandra,
	}
}

func NewClusterConnector(
	connInfo *ClusterConnectionInfo,
	connectionOpenTimeout time.Duration,
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

	timeoutCtx, _ := context.WithTimeout(clientHandlerContext, connectionOpenTimeout)
	conn, err := openConnectionToCluster(connInfo, timeoutCtx, metricsHandler)
	if err != nil {
		if errors.Is(err, ShutdownErr) {
			if timeoutCtx.Err() != nil {
				return nil, fmt.Errorf("context timed out or cancelled while opening connection to %v: %w", clusterType, timeoutCtx.Err())
			}
		}
		return nil, fmt.Errorf("could not open connection to %v: %w", clusterType, err)
	}

	go func() {
		<-clientHandlerContext.Done()
		closeConnectionToCluster(conn, clusterType, metricsHandler)
	}()

	return &ClusterConnector{
		connection:              conn,
		clusterResponseChannels: make(map[int16]chan *frame.RawFrame),
		clusterType:             clusterType,
		clusterConnEventsChan:   make(chan *frame.RawFrame),
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
	log.Infof("closing connection to %v", conn.RemoteAddr())
	err := conn.Close()
	if err != nil {
		log.Warnf("error closing connection to %v", conn.RemoteAddr())
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
	log.Debugf("Listening to replies sent by node %v", cc.connection.RemoteAddr())
	go func() {
		defer cc.waitGroup.Done()
		defer close(cc.clusterConnEventsChan)
		for {
			response, err := readRawFrame(cc.connection, cc.clientHandlerContext)

			if err != nil {
				if errors.Is(err, ShutdownErr) {
					break
				}

				if errors.Is(err, io.EOF) {
					log.Infof("in runResponseListeningLoop: %v disconnected", cc.connection.RemoteAddr())
				} else {
					log.Errorf("in runResponseListeningLoop: error reading: %v", err)
				}

				cc.clientHandlerCancelFunc()
				break
			}

			log.Debugf("Received response from %v (%v): %v",
				cc.clusterType, cc.connection.RemoteAddr(), response.Header)

			if response.Header.OpCode == primitive.OpCodeEvent {
				cc.clusterConnEventsChan <- response
			} else {
				cc.forwardResponseToChannel(response)
			}
		}
		log.Infof("shutting down response listening loop from %v", cc.connection.RemoteAddr())
	}()
}

func (cc *ClusterConnector) forwardResponseToChannel(response *frame.RawFrame) {
	cc.lock.RLock()
	defer cc.lock.RUnlock()
	if responseChannel, ok := cc.clusterResponseChannels[response.Header.StreamId]; !ok {
		if cc.clientHandlerContext.Err() == nil {
			log.Errorf("could not find stream id %d in clusterResponseChannels for cluster %v", response.Header.StreamId, cc.clusterType)
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
func (cc *ClusterConnector) forwardToCluster(rawFrame *frame.RawFrame) chan *frame.RawFrame {
	responseToCallerChan := make(chan *frame.RawFrame, 1)
	streamId := rawFrame.Header.StreamId
	go func() {
		defer close(responseToCallerChan)

		responseFromClusterChan, err := cc.createChannelForClusterResponse(streamId)
		if err != nil {
			log.Errorf("Error creating cluster response channel for stream id %d: %v", streamId, err)
			return
		}

		// once the response has been sent to the caller, remove the channel from the map as it has served its purpose
		defer cc.deleteChannelForClusterResponse(streamId)

		err = cc.sendRequestToCluster(cc.clientHandlerContext, rawFrame)

		if errors.Is(err, ShutdownErr) {
			return
		} else if err != nil {
			log.Errorf("Error while sending request to %v: %v", cc.connection.RemoteAddr(), err)
			return
		}

		select {
		case <-cc.clientHandlerContext.Done():
			return
		case response, ok := <-responseFromClusterChan:
			if !ok {
				log.Debugf("response from cluster channel was closed, connection: %v", cc.connection.RemoteAddr())
				return
			}
			log.Tracef("Received response from %v for query with stream id %d", cc.clusterType, response.Header.StreamId)
			responseToCallerChan <- response
		case <-time.After(queryTimeout):
			log.Debugf("Timeout for query %d from %v", streamId, cc.clusterType)
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
func (cc *ClusterConnector) createChannelForClusterResponse(streamId int16) (chan *frame.RawFrame, error) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	if _, ok := cc.clusterResponseChannels[streamId]; ok {
		return nil, fmt.Errorf("streamid collision: %d", streamId)
	}

	cc.clusterResponseChannels[streamId] = make(chan *frame.RawFrame, 1)
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

func (cc *ClusterConnector) sendRequestToCluster(clientHandlerContext context.Context, frame *frame.RawFrame) error {
	log.Debugf("Executing %v on cluster %v with address %v", frame.Header, cc.clusterType, cc.connection.RemoteAddr())
	return writeRawFrame(cc.connection, clientHandlerContext, frame)
}

func (cci *ClusterConnectionInfo) getConnectionString() string {
	return fmt.Sprintf("%s:%d", cci.ipAddress, cci.port)
}
