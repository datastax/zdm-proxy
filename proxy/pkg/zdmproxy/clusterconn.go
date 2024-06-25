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
	frameProcessor              FrameProcessor

	handshakeDone *atomic.Value

	asyncConnector       bool
	asyncConnectorState  ConnectorState
	asyncPendingRequests *pendingRequests

	readScheduler *Scheduler

	lastHeartbeatTime *atomic.Value
	lastHeartbeatLock sync.Mutex
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
	handshakeDone *atomic.Value,
	frameProcessor FrameProcessor) (*ClusterConnector, error) {

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

	// Initialize heartbeat time
	lastHeartbeatTime := &atomic.Value{}
	lastHeartbeatTime.Store(time.Now())

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
		frameProcessor:              frameProcessor,
		responseReadBufferSizeBytes: conf.ResponseReadBufferSizeBytes,
		doneChan:                    make(chan bool),
		readScheduler:               readScheduler,
		asyncConnector:              asyncConnector,
		asyncConnectorState:         ConnectorStateHandshake,
		asyncPendingRequests:        asyncPendingRequests,
		handshakeDone:               handshakeDone,
		lastHeartbeatTime:           lastHeartbeatTime,
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
			protocolErrResponseFrame, err, errCode := checkProtocolError(response, err, protocolErrOccurred, string(cc.connectorType))

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

			// when there's a protocol error, we cannot rely on the returned stream id, the only exception is
			// when it's a UnsupportedVersion error, which means the Frame was properly parsed by the native protocol library
			// but the proxy doesn't support the protocol version and in that case we can proceed with releasing the stream id in the mapper
			if response != nil && response.Header.StreamId >= 0 && (err == nil || errCode == ProtocolErrorUnsupportedVersion) {
				var releaseErr error
				response, releaseErr = cc.frameProcessor.ReleaseId(response)
				if releaseErr != nil {
					// if releasing the stream id failed, check if it's a protocol error response
					// if it is then ignore the release error and forward the response to the client handler so that
					// it can be handled correctly
					parsedResponse, parseErr := defaultCodec.ConvertFromRawFrame(response)
					if parseErr != nil {
						log.Errorf("[%v] Error converting frame when releasing stream id: %v. Original error: %v.", string(cc.connectorType), parseErr, releaseErr)
						continue
					}
					_, isProtocolErr := parsedResponse.Body.Message.(*message.ProtocolError)
					if !isProtocolErr {
						log.Errorf("[%v] Error releasing stream id: %v.", string(cc.connectorType), releaseErr)
						continue
					}
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
					var preparedEntry PreparedEntry
					var ok bool
					if cc.clusterType == common.ClusterTypeTarget {
						preparedEntry, ok = cc.psCache.GetByTargetPreparedId(msg.Id)
					} else {
						preparedEntry, ok = cc.psCache.GetByOriginPreparedId(msg.Id)
					}
					if !ok {
						log.Warnf("Received UNPREPARED for async request with prepare ID %v "+
							"but could not find prepared data.", hex.EncodeToString(msg.Id))
					} else {
						prepare := &message.Prepare{
							Query:    preparedEntry.GetPrepareRequestInfo().GetQuery(),
							Keyspace: preparedEntry.GetPrepareRequestInfo().GetKeyspace(),
						}
						prepareFrame := frame.NewFrame(response.Header.Version, response.Header.StreamId, prepare)
						prepareRawFrame, err := defaultCodec.ConvertToRawFrame(prepareFrame)
						if err != nil {
							log.Errorf("Could not send async PREPARE because convert raw frame failed: %v.", err.Error())
						} else {
							sent := cc.sendAsyncRequestToCluster(
								preparedEntry.GetPrepareRequestInfo(), prepareRawFrame, false, time.Now(),
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
			} else {
				decodedMsg, err := defaultCodec.ConvertFromRawFrame(response)
				if err != nil {
					log.Warnf("Could not decode async result: %v", err)
				}
				switch msg := decodedMsg.Body.Message.(type) {
				case *message.PreparedResult:
					prepareRequestInfo, ok := reqCtx.GetRequestInfo().(*PrepareRequestInfo)
					if !ok {
						log.Errorf("Received prepared result on async connector but request info is not prepared: %T.", reqCtx.GetRequestInfo())
					}
					switch cc.clusterType {
					case common.ClusterTypeTarget:
						_ = cc.psCache.StorePreparedOnTarget(msg, prepareRequestInfo)
					case common.ClusterTypeOrigin:
						_ = cc.psCache.StorePreparedOnOrigin(msg, prepareRequestInfo)
					}
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
	var err error
	if cc.frameProcessor != nil {
		frame, err = cc.frameProcessor.AssignUniqueId(frame)
	}
	if err != nil {
		log.Errorf("[%v] Couldn't assign stream id to frame %v: %v", string(cc.connectorType), frame.Header.OpCode, err)
		return
	} else {
		cc.writeCoalescer.Enqueue(frame)
	}
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

func (cc *ClusterConnector) sendAsyncRequestToCluster(
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

	var err error
	asyncRequest, err = cc.frameProcessor.AssignUniqueId(asyncRequest)
	if err == nil {
		err = cc.asyncPendingRequests.store(asyncReqCtx, asyncRequest)
	}

	storedAsync := err == nil
	if err != nil {
		log.Warnf("Could not send async request due to an error while storing the request state: %v.", err.Error())
	} else {
		if requestInfo.ShouldBeTrackedInMetrics() {
			cc.nodeMetrics.AsyncMetrics.InFlightRequests.Add(1)
		}
		timer := time.AfterFunc(requestTimeout, func() {
			if cc.asyncPendingRequests.timeOut(asyncRequest.Header.StreamId, asyncReqCtx, asyncRequest) {
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
		if !cc.writeCoalescer.EnqueueAsync(asyncRequest) {
			err = errors.New("async request was not sent")
		}
	}

	if err != nil {
		if storedAsync {
			cc.asyncPendingRequests.cancel(asyncRequest.Header.StreamId, asyncReqCtx) // wg.Done will be called by caller
		}
	}

	return err == nil
}

func (cc *ClusterConnector) sendHeartbeat(version primitive.ProtocolVersion, heartbeatIntervalMs int) {
	if version == 0 || !cc.shouldSendHeartbeat(heartbeatIntervalMs) {
		return
	}
	cc.lastHeartbeatLock.Lock()
	defer cc.lastHeartbeatLock.Unlock()
	if !cc.shouldSendHeartbeat(heartbeatIntervalMs) {
		return
	}
	cc.lastHeartbeatTime.Store(time.Now())
	optionsMsg := &message.Options{}
	heartBeatFrame := frame.NewFrame(version, -1, optionsMsg)
	rawFrame, err := defaultCodec.ConvertToRawFrame(heartBeatFrame)
	if err != nil {
		log.Errorf("Cannot convert heartbeat frame to raw frame: %v", err)
		return
	}
	log.Debugf("Sending heartbeat to cluster %v", cc.clusterType)
	cc.sendRequestToCluster(rawFrame)
}

// shouldSendHeartbeat looks up the value of the last heartbeat time in the atomic value
// and returns true if more time has passed than the configured interval, otherwise returns false.
func (cc *ClusterConnector) shouldSendHeartbeat(heartbeatIntervalMs int) bool {
	lastHeartbeatTime := cc.lastHeartbeatTime.Load().(time.Time)
	return time.Since(lastHeartbeatTime) > time.Duration(heartbeatIntervalMs)*time.Millisecond
}
