package zdmproxy

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/proxy/pkg/common"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type ClusterConnectionInfo struct {
	connConfig        ConnectionConfig
	endpoint          Endpoint
	isOriginCassandra bool
}

type ClusterConnectorType string

const (
	ClusterConnectorTypeNone   = ClusterConnectorType("")
	ClusterConnectorTypeOrigin = ClusterConnectorType("ORIGIN-CONNECTOR")
	ClusterConnectorTypeTarget = ClusterConnectorType("TARGET-CONNECTOR")
	ClusterConnectorTypeAsync  = ClusterConnectorType("ASYNC-CONNECTOR")
)

type ConnectorState = int32

const (
	ConnectorStateHandshake int32 = iota
	ConnectorStateReady
	ConnectorStateShutdown
)

type ClusterConnector struct {
	conf *config.Config

	connection    net.Conn
	clusterType   common.ClusterType
	connectorType ClusterConnectorType

	psCache *PreparedStatementCache

	clusterConnEventsChan  chan *frame.RawFrame
	nodeMetrics            *metrics.NodeMetrics
	clientHandlerWg        *sync.WaitGroup
	clientHandlerRequestWg *sync.WaitGroup
	clusterConnContext     context.Context
	cancelFunc             context.CancelFunc
	responseChan           chan<- *Response

	responseReadBufferSizeBytes int
	writeCoalescer              *writeCoalescer
	doneChan                    chan bool

	handshakeDone *atomic.Value

	asyncConnector       bool
	asyncConnectorState  ConnectorState
	asyncPendingRequests *pendingRequests

	readScheduler *Scheduler
}

func NewClusterConnectionInfo(connConfig ConnectionConfig, endpointConfig Endpoint, isOriginCassandra bool) *ClusterConnectionInfo {
	return &ClusterConnectionInfo{
		connConfig:        connConfig,
		endpoint:          endpointConfig,
		isOriginCassandra: isOriginCassandra,
	}
}

func NewClusterConnector(
	connInfo *ClusterConnectionInfo,
	conf *config.Config,
	psCache *PreparedStatementCache,
	nodeMetrics *metrics.NodeMetrics,
	clientHandlerWg *sync.WaitGroup,
	clientHandlerRequestWg *sync.WaitGroup,
	clientHandlerContext context.Context,
	clientHandlerCancelFunc context.CancelFunc,
	responseChan chan<- *Response,
	readScheduler *Scheduler,
	writeScheduler *Scheduler,
	requestsDoneCtx context.Context,
	asyncConnector bool,
	asyncPendingRequests *pendingRequests,
	handshakeDone *atomic.Value) (*ClusterConnector, error) {

	var connectorType ClusterConnectorType
	var clusterType common.ClusterType
	if connInfo.isOriginCassandra {
		clusterType = common.ClusterTypeOrigin
		connectorType = ClusterConnectorTypeOrigin
	} else {
		clusterType = common.ClusterTypeTarget
		connectorType = ClusterConnectorTypeTarget
	}

	if asyncConnector {
		connectorType = ClusterConnectorTypeAsync
	}

	conn, timeoutCtx, err := openConnectionToCluster(connInfo, clientHandlerContext, connectorType, nodeMetrics)
	if err != nil {
		if errors.Is(err, ShutdownErr) {
			if timeoutCtx.Err() != nil {
				return nil, fmt.Errorf("%s context timed out or cancelled while opening connection to %v: %w", connectorType, clusterType, timeoutCtx.Err())
			}
		}
		return nil, fmt.Errorf("%s could not open connection to %v: %w", connectorType, clusterType, err)
	}

	clusterConnCtx, clusterConnCancelFn := context.WithCancel(clientHandlerContext)

	go func() {
		select {
		case <-requestsDoneCtx.Done():
			clusterConnCancelFn()
		case <-clusterConnCtx.Done():
		}
		closeConnectionToCluster(conn, clusterType, connectorType, nodeMetrics)
	}()

	cancelFn := clusterConnCancelFn
	var clusterConnEventsChan chan *frame.RawFrame
	if !asyncConnector {
		cancelFn = clientHandlerCancelFunc
		clusterConnEventsChan = make(chan *frame.RawFrame, conf.EventQueueSizeFrames)
	}

	return &ClusterConnector{
		conf:                   conf,
		connection:             conn,
		clusterType:            clusterType,
		connectorType:          connectorType,
		clusterConnEventsChan:  clusterConnEventsChan,
		psCache:                psCache,
		nodeMetrics:            nodeMetrics,
		clientHandlerWg:        clientHandlerWg,
		clientHandlerRequestWg: clientHandlerRequestWg,
		clusterConnContext:     clusterConnCtx,
		cancelFunc:             cancelFn,
		writeCoalescer: NewWriteCoalescer(
			conf,
			conn,
			clientHandlerWg,
			clusterConnCtx,
			cancelFn,
			string(connectorType),
			true,
			asyncConnector,
			writeScheduler),
		responseChan:                responseChan,
		responseReadBufferSizeBytes: conf.ResponseReadBufferSizeBytes,
		doneChan:                    make(chan bool),
		readScheduler:               readScheduler,
		asyncConnector:              asyncConnector,
		asyncConnectorState:         ConnectorStateHandshake,
		asyncPendingRequests:        asyncPendingRequests,
		handshakeDone:               handshakeDone,
	}, nil
}

func (cc *ClusterConnector) run() {
	cc.runResponseListeningLoop()
	cc.writeCoalescer.RunWriteQueueLoop()
}

func openConnectionToCluster(connInfo *ClusterConnectionInfo, context context.Context, connectorType ClusterConnectorType, nodeMetrics *metrics.NodeMetrics) (net.Conn, context.Context, error) {
	clusterType := connInfo.connConfig.GetClusterType()
	log.Infof("[%s] Opening request connection to %v (%v).", connectorType, clusterType, connInfo.endpoint.GetEndpointIdentifier())
	conn, timeoutCtx, err := openConnection(connInfo.connConfig, connInfo.endpoint, context, true)
	if err != nil {
		return nil, timeoutCtx, err
	}

	nodeMetricsInstance, err := GetNodeMetricsByClusterConnector(nodeMetrics, connectorType)
	if err != nil {
		log.Errorf("Failed to track open connection metrics for conn %v: %v.", conn.RemoteAddr().String(), err)
	} else {
		nodeMetricsInstance.OpenConnections.Add(1)
	}

	log.Infof("[%s] Request connection to %v (%v) has been opened.", connectorType, clusterType, conn.RemoteAddr())
	return conn, timeoutCtx, nil
}

func closeConnectionToCluster(conn net.Conn, clusterType common.ClusterType, connectorClusterType ClusterConnectorType, nodeMetrics *metrics.NodeMetrics) {
	log.Infof("[%s] Closing request connection to %v (%v)", connectorClusterType, clusterType, conn.RemoteAddr())
	err := conn.Close()
	if err != nil {
		log.Warnf("[%s] Error closing connection to %v (%v): %v.", connectorClusterType, clusterType, conn.RemoteAddr(), err.Error())
	}

	nodeMetricsInstance, err := GetNodeMetricsByClusterConnector(nodeMetrics, connectorClusterType)
	if err != nil {
		log.Errorf("Failed to subtract open connection metrics for conn %v: %v.", conn.RemoteAddr().String(), err.Error())
	} else {
		nodeMetricsInstance.OpenConnections.Subtract(1)
	}

	log.Infof("[%s] Request connection to %v (%v) has been closed", connectorClusterType, clusterType, conn.RemoteAddr())
}

/**
 *	Starts a long-running loop that listens for replies being sent by the cluster
 */
func (cc *ClusterConnector) runResponseListeningLoop() {

	cc.clientHandlerWg.Add(1)
	log.Debugf("[%s] Listening to replies sent by node %v", cc.connectorType, cc.connection.RemoteAddr())
	go func() {
		defer cc.clientHandlerWg.Done()
		if cc.clusterConnEventsChan != nil {
			defer close(cc.clusterConnEventsChan)
		}
		defer close(cc.doneChan)
		defer atomic.StoreInt32(&cc.asyncConnectorState, ConnectorStateShutdown)

		bufferedReader := bufio.NewReaderSize(cc.connection, cc.responseReadBufferSizeBytes)
		connectionAddr := cc.connection.RemoteAddr().String()
		wg := &sync.WaitGroup{}
		defer wg.Wait()
		protocolErrOccurred := false
		for {
			response, err := readRawFrame(bufferedReader, connectionAddr, cc.clusterConnContext)

			protocolErrResponseFrame, err := checkProtocolError(response, err, protocolErrOccurred, string(cc.connectorType))
			if err != nil {
				handleConnectionError(
					err, cc.clusterConnContext, cc.cancelFunc, string(cc.connectorType), "reading", connectionAddr)
				break
			} else {
				if protocolErrOccurred {
					log.Debugf("[%v] Data received after protocol error occured, ignoring it.", string(cc.connectorType))
					continue
				} else if protocolErrResponseFrame != nil {
					response = protocolErrResponseFrame
					protocolErrOccurred = true
				}
			}

			wg.Add(1)
			cc.readScheduler.Schedule(func() {
				defer wg.Done()
				log.Tracef("[%s] Received response from %v (%v): %v",
					cc.connectorType, cc.clusterType, connectionAddr, response.Header)

				if cc.asyncConnector {
					response = cc.handleAsyncResponse(response)
					if response == nil {
						return
					}
				}

				if response.Header.OpCode == primitive.OpCodeEvent {
					cc.clusterConnEventsChan <- response
				} else {
					cc.responseChan <- NewResponse(response, cc.connectorType)
				}
				log.Tracef("[%s] Response sent to response channel: %v", cc.connectorType, response.Header)
			})
		}
		log.Debugf("[%s] Shutting down response listening loop from %v", cc.connectorType, connectionAddr)
	}()
}

func (cc *ClusterConnector) handleAsyncResponse(response *frame.RawFrame) *frame.RawFrame {
	errMsg, err := decodeError(response)
	if err != nil {
		log.Errorf("[%s] Error occured while checking if error is a protocol error: %v.", cc.connectorType, err)
		cc.Shutdown()
		return nil
	}

	if errMsg != nil && errMsg.GetErrorCode() == primitive.ErrorCodeProtocolError {
		if cc.handshakeDone.Load() != nil {
			log.Errorf("[%s] Protocol error occured in async connector, async requests will not be forwarded: %v.", cc.connectorType, errMsg)
		} else {
			log.Debugf("[%s] Protocol version downgrade detected in async connector, async requests will not be forwarded: %v.", cc.connectorType, errMsg)
		}
		cc.Shutdown()
		return nil
	}

	if response.Header.OpCode == primitive.OpCodeEvent {
		return nil
	}

	reqCtx, done := cc.asyncPendingRequests.markAsDone(response.Header.StreamId, response, cc.clusterType, cc.connectorType)
	if done {
		typedReqCtx, ok := reqCtx.(*asyncRequestContextImpl)
		if !ok {
			log.Errorf("Failed to finish async request because request context conversion failed. "+
				"This is most likely a bug, please report. AsyncRequestContext: %v", reqCtx)
		} else if typedReqCtx.expectedResponse {
			response.Header.StreamId = typedReqCtx.requestStreamId
			return response
		} else {
			callDone := true
			if errMsg != nil {
				if reqCtx.GetRequestInfo().ShouldBeTrackedInMetrics() {
					trackClusterErrorMetricsFromErrorMessage(errMsg, cc.connectorType, cc.nodeMetrics)
				}
				switch msg := errMsg.(type) {
				case *message.Unprepared:
					var preparedData PreparedData
					var ok bool
					if cc.clusterType == common.ClusterTypeTarget {
						preparedData, ok = cc.psCache.GetByTargetPreparedId(msg.Id)
					} else {
						preparedData, ok = cc.psCache.Get(msg.Id)
					}
					if !ok {
						log.Warnf("Received UNPREPARED for async request with prepare ID %v "+
							"but could not find prepared data.", hex.EncodeToString(msg.Id))
					} else {
						prepare := &message.Prepare{
							Query:    preparedData.GetPrepareRequestInfo().GetQuery(),
							Keyspace: preparedData.GetPrepareRequestInfo().GetKeyspace(),
						}
						prepareFrame := frame.NewFrame(response.Header.Version, response.Header.StreamId, prepare)
						prepareRawFrame, err := defaultCodec.ConvertToRawFrame(prepareFrame)
						if err != nil {
							log.Errorf("Could not send async PREPARE because convert raw frame failed: %v.", err.Error())
						} else {
							sent := cc.sendAsyncRequest(
								preparedData.GetPrepareRequestInfo(), prepareRawFrame, false, time.Now(),
								time.Duration(cc.conf.ProxyRequestTimeoutMs)*time.Millisecond,
								func() {
									cc.clientHandlerRequestWg.Done()
								})
							if sent {
								callDone = false
							}
						}
					}
				default:
					log.Warnf("Async Request failed with error code %v. Error message: %v", errMsg.GetErrorCode(), errMsg.GetErrorMessage())
				}
			}

			if callDone {
				cc.clientHandlerRequestWg.Done()
			}
		}
	}
	return nil
}

func (cc *ClusterConnector) sendRequestToCluster(frame *frame.RawFrame) {
	cc.writeCoalescer.Enqueue(frame)
}

func (cc *ClusterConnector) validateAsyncStateForRequest(frame *frame.RawFrame) bool {
	state := atomic.LoadInt32(&cc.asyncConnectorState)
	switch state {
	case ConnectorStateShutdown:
		log.Tracef("[%s] Discarding async %v request because async connector is shut down.",
			cc.connectorType, frame.Header.OpCode.String())
		return false
	case ConnectorStateHandshake:
		switch frame.Header.OpCode {
		case primitive.OpCodeStartup, primitive.OpCodeAuthResponse, primitive.OpCodeOptions:
			return true
		default:
			log.Debugf("[%s] Discarding async %v request because async connector is not ready.",
				cc.connectorType, frame.Header.OpCode.String())
			return false
		}
	case ConnectorStateReady:
		return true
	default:
		log.Errorf("Unknown cluster connector state: %v. This is a bug, please report.", state)
		return false
	}
}

func (cc *ClusterConnector) sendAsyncRequestToCluster(frame *frame.RawFrame) bool {
	return cc.writeCoalescer.EnqueueAsync(frame)
}

func (cc *ClusterConnector) SetReady() bool {
	return atomic.CompareAndSwapInt32(&cc.asyncConnectorState, ConnectorStateHandshake, ConnectorStateReady)
}

func (cc *ClusterConnector) IsShutdown() bool {
	return atomic.LoadInt32(&cc.asyncConnectorState) == ConnectorStateShutdown
}

func (cc *ClusterConnector) Shutdown() {
	cc.cancelFunc()
}

// Checks if the error was due to a shutdown request, triggering the cancellation function if it was not.
// Also logs the error appropriately.
func handleConnectionError(err error, ctx context.Context, cancelFn context.CancelFunc, logPrefix string, operation string, connectionAddr string) {
	if errors.Is(err, ShutdownErr) {
		return
	}
	if errors.Is(err, io.EOF) || IsPeerDisconnect(err) || IsClosingErr(err) {
		log.Infof("[%v] %v disconnected", logPrefix, connectionAddr)
	} else {
		log.Errorf("[%v] error %v: %v", logPrefix, operation, err)
	}

	if ctx.Err() == nil {
		cancelFn()
	}
}

func (cc *ClusterConnector) sendAsyncRequest(
	requestInfo RequestInfo,
	asyncRequest *frame.RawFrame,
	expectedResponse bool,
	overallRequestStartTime time.Time,
	requestTimeout time.Duration,
	onTimeout func()) bool {

	if !cc.validateAsyncStateForRequest(asyncRequest) {
		return false
	}

	asyncReqCtx := NewAsyncRequestContext(requestInfo, asyncRequest.Header.StreamId, expectedResponse, overallRequestStartTime)
	var newStreamId int16
	newStreamId, err := cc.asyncPendingRequests.store(asyncReqCtx)
	storedAsync := err == nil
	if err != nil {
		log.Warnf("Could not send async request due to an error while storing the request state: %v.", err.Error())
	} else {
		if requestInfo.ShouldBeTrackedInMetrics() {
			cc.nodeMetrics.AsyncMetrics.InFlightRequests.Add(1)
		}
		asyncRequest.Header.StreamId = newStreamId
		timer := time.AfterFunc(requestTimeout, func() {
			if cc.asyncPendingRequests.timeOut(newStreamId, asyncReqCtx, asyncRequest) {
				log.Warnf(
					"Async Request (%v) timed out after %v ms.",
					asyncRequest.Header.OpCode.String(), requestTimeout.Milliseconds())
				onTimeout()
			}
			return
		})
		asyncReqCtx.SetTimer(timer)
	}

	if err == nil {
		log.Tracef("Forwarding ASYNC request with opcode %v for stream %v to %v",
			asyncRequest.Header.OpCode, asyncRequest.Header.StreamId, cc.clusterType)
		if !cc.sendAsyncRequestToCluster(asyncRequest) {
			err = errors.New("async request was not sent")
		}
	}

	if err != nil {
		if storedAsync {
			cc.asyncPendingRequests.cancel(newStreamId, asyncReqCtx) // wg.Done will be called by caller
		}
	}

	return err == nil
}
