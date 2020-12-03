package cloudgateproxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
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
	conf                    *config.Config
	clusterResponseChannels *sync.Map // map of channels, keyed on streamID, on which to send the response to a request
	clusterType             ClusterType

	clusterConnEventsChan   chan *frame.RawFrame
	metricsHandler          metrics.IMetricsHandler
	waitGroup               *sync.WaitGroup
	clientHandlerContext    context.Context
	clientHandlerCancelFunc context.CancelFunc

	writeCoalescer          *writeCoalescer
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
	conf *config.Config,
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

	connectionOpenTimeout := time.Duration(conf.ClusterConnectionTimeoutMs)*time.Millisecond
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
		conf:                    conf,
		clusterResponseChannels: &sync.Map{},
		clusterType:             clusterType,
		clusterConnEventsChan:   make(chan *frame.RawFrame, conf.EventQueueSizeFrames),
		metricsHandler:          metricsHandler,
		waitGroup:               waitGroup,
		clientHandlerContext:    clientHandlerContext,
		clientHandlerCancelFunc: clientHandlerCancelFunc,
		writeCoalescer:          NewWriteCoalescer(
			conf,
			conn,
			metricsHandler,
			waitGroup,
			clientHandlerContext,
			clientHandlerCancelFunc,
			"ClusterConnector"),
	}, nil
}

func (cc *ClusterConnector) run() {
	cc.runResponseListeningLoop()
	cc.writeCoalescer.RunWriteQueueLoop()
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

		bufferedReader := bufio.NewReaderSize(cc.connection, cc.conf.ReadBufferSizeBytes)
		connectionAddr := cc.connection.RemoteAddr().String()
		for {
			response, err := readRawFrame(bufferedReader, connectionAddr, cc.clientHandlerContext)
			if err != nil {
				handleConnectionError(
					err, cc.clientHandlerCancelFunc, fmt.Sprintf("ClusterConnector %v", cc.clusterType), "reading", connectionAddr)
				break
			}

			log.Debugf("Received response from %v (%v): %v",
				cc.clusterType, connectionAddr, response.Header)

			if response.Header.OpCode == primitive.OpCodeEvent {
				cc.clusterConnEventsChan <- response
			} else {
				cc.forwardResponseToChannel(response)
			}
		}
		log.Infof("Shutting down response listening loop from %v", connectionAddr)
	}()
}

func (cc *ClusterConnector) forwardResponseToChannel(response *frame.RawFrame) {
	responseChan := cc.getResponseChannel(response.Header.StreamId)
	if responseChan == nil {
		log.Errorf(
			"Could not find response channel with stream id %d for cluster %v",
			response.Header.StreamId, cc.clusterType)
		return
	}

	err := responseChan.WriteResponse(response)
	if err != nil {
		log.Infof(
			"Could not write response to response channel with stream id %d for cluster %v: %v. Most likely the request timed out.",
			response.Header.StreamId, cc.clusterType, err)
	}
}

func (cc *ClusterConnector) getResponseChannel(streamId int16) *responseChannel {
	responseChannelValue, ok := cc.clusterResponseChannels.Load(streamId)
	if ok {
		return responseChannelValue.(*responseChannel)
	} else {
		channel, _ := cc.clusterResponseChannels.LoadOrStore(streamId, NewResponseChannel())
		return channel.(*responseChannel)
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

		startTime := time.Now()

		ok := cc.sendRequestToCluster(cc.clientHandlerContext, rawFrame)
		if !ok {
			return
		}

		timeoutTimer := time.NewTimer(queryTimeout)
		defer timeoutTimer.Stop()

		select {
		case <-cc.clientHandlerContext.Done():
			return
		case response, ok := <-responseFromClusterChan:
			if !ok {
				log.Debugf("response from cluster channel was closed, connection: %v", cc.connection.RemoteAddr())
				return
			}
			log.Tracef("Received response from %v for query with stream id %d", cc.clusterType, response.Header.StreamId)

			cc.trackClusterLevelDuration(startTime)
			cc.trackClusterErrorMetrics(response)

			responseToCallerChan <- response
		case <-timeoutTimer.C:
			log.Debugf("Timeout for query %d from %v", streamId, cc.clusterType)

			cc.trackClusterLevelDuration(startTime)
			if cc.clusterType == OriginCassandra {
				cc.metricsHandler.IncrementCountByOne(metrics.OriginClientTimeouts)
			} else {
				cc.metricsHandler.IncrementCountByOne(metrics.TargetClientTimeouts)
			}
		}
	}()

	return responseToCallerChan
}

/**
 *	Creates channel on which the dequeuer will send the response to the request with this streamId and adds it to the map
 */
func (cc *ClusterConnector) createChannelForClusterResponse(streamId int16) (<-chan *frame.RawFrame, error) {
	responseChan := cc.getResponseChannel(streamId)
	if responseChan == nil {
		return nil, fmt.Errorf("could not find response channel with stream id %d for cluster %v", streamId, cc.clusterType)
	}

	newChannel, err := responseChan.Open()
	if err != nil {
		return nil, fmt.Errorf("stream id collision (%d) for cluster %v", streamId, cc.clusterType)
	}

	return newChannel, nil
}

/**
 *	Removes the response channel for this streamId from the map
 */
func (cc *ClusterConnector) deleteChannelForClusterResponse(streamId int16) {
	responseChannelValue := cc.getResponseChannel(streamId)
	if responseChannelValue == nil {
		log.Errorf("could not delete response channel because no channel with stream id %d exist for cluster %v", streamId, cc.clusterType)
		return
	}

	err := responseChannelValue.Close()
	if err != nil {
		log.Errorf("Unexpected error closing response channel: %v", err)
	}
}

func (cc *ClusterConnector) sendRequestToCluster(clientHandlerContext context.Context, frame *frame.RawFrame) bool {
	return cc.writeCoalescer.Enqueue(clientHandlerContext, frame)
}

func (cci *ClusterConnectionInfo) getConnectionString() string {
	return fmt.Sprintf("%s:%d", cci.ipAddress, cci.port)
}

/**
Updates cluster level error metrics based on the outcome in the response
*/
func (cc *ClusterConnector) trackClusterErrorMetrics(response *frame.RawFrame) {
	if !isResponseSuccessful(response) {
		errorMsg, err := decodeErrorResult(response)
		if err != nil {
			log.Errorf("could not track read response: %v", err)
			return
		}

		switch cc.clusterType {
		case OriginCassandra:
			switch errorMsg.GetErrorCode() {
			case primitive.ErrorCodeUnprepared:
				cc.metricsHandler.IncrementCountByOne(metrics.OriginUnpreparedErrors)
			case primitive.ErrorCodeReadTimeout:
				cc.metricsHandler.IncrementCountByOne(metrics.OriginReadTimeouts)
			case primitive.ErrorCodeWriteTimeout:
				cc.metricsHandler.IncrementCountByOne(metrics.OriginWriteTimeouts)
			default:
				cc.metricsHandler.IncrementCountByOne(metrics.OriginOtherErrors)
			}
		case TargetCassandra:
			switch errorMsg.GetErrorCode() {
			case primitive.ErrorCodeUnprepared:
				cc.metricsHandler.IncrementCountByOne(metrics.TargetUnpreparedErrors)
			case primitive.ErrorCodeReadTimeout:
				cc.metricsHandler.IncrementCountByOne(metrics.TargetReadTimeouts)
			case primitive.ErrorCodeWriteTimeout:
				cc.metricsHandler.IncrementCountByOne(metrics.TargetWriteTimeouts)
			default:
				cc.metricsHandler.IncrementCountByOne(metrics.TargetOtherErrors)
			}
		default:
			log.Errorf("unexpected clusterType %v, unable to track cluster metrics", cc.clusterType)
		}
	}
}

func (cc *ClusterConnector) trackClusterLevelDuration(startTime time.Time) {
	switch cc.clusterType {
	case OriginCassandra:
		cc.metricsHandler.TrackInHistogram(metrics.OriginRequestDuration, startTime)
	case TargetCassandra:
		cc.metricsHandler.TrackInHistogram(metrics.TargetRequestDuration, startTime)
	default:
		log.Errorf("Unknown cluster type %v, will not track this request duration.", cc.clusterType)
	}
}

// Checks if the error was due to a shutdown request, triggering the cancellation function if it was not.
// Also logs the error appropriately.
func handleConnectionError(err error, cancelFn context.CancelFunc, logPrefix string, operation string, connectionAddr string) {
	if errors.Is(err, ShutdownErr) {
		return
	}

	if errors.Is(err, io.EOF) {
		log.Infof("[%v] %v disconnected", logPrefix, connectionAddr)
	} else {
		log.Errorf("[%v] error %v: %v", logPrefix, operation, err)
	}

	cancelFn()
}