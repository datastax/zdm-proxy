package cloudgateproxy

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

type ClusterConnectionInfo struct {
	connConfig        ConnectionConfig
	endpoint          Endpoint
	isOriginCassandra bool
}

type ClusterType string

const (
	ClusterTypeNone   = ClusterType("")
	ClusterTypeOrigin = ClusterType("ORIGIN")
	ClusterTypeTarget = ClusterType("TARGET")
)

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
	connection  net.Conn
	clusterType ClusterType
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

	asyncConnector      bool
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
	asyncPendingRequests *pendingRequests) (*ClusterConnector, error) {

	var connectorType ClusterConnectorType
	var clusterType ClusterType
	if connInfo.isOriginCassandra {
		clusterType = ClusterTypeOrigin
		connectorType = ClusterConnectorTypeOrigin
	} else {
		clusterType = ClusterTypeTarget
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

	if clusterType == ClusterTypeOrigin {
		nodeMetrics.OriginMetrics.OpenOriginConnections.Add(1)
	} else {
		nodeMetrics.TargetMetrics.OpenTargetConnections.Add(1)
	}

	log.Infof("[%s] Request connection to %v (%v) has been opened.", connectorType, clusterType, conn.RemoteAddr())
	return conn, timeoutCtx, nil
}

func closeConnectionToCluster(conn net.Conn, clusterType ClusterType, connectorClusterType ClusterConnectorType, nodeMetrics *metrics.NodeMetrics) {
	log.Infof("[%s] Closing request connection to %v (%v)", connectorClusterType, clusterType, conn.RemoteAddr())
	err := conn.Close()
	if err != nil {
		log.Warnf("[%s] Error closing connection to %v (%v)", connectorClusterType, clusterType, conn.RemoteAddr())
	}

	if clusterType == ClusterTypeOrigin {
		nodeMetrics.OriginMetrics.OpenOriginConnections.Subtract(1)
	} else {
		nodeMetrics.TargetMetrics.OpenTargetConnections.Subtract(1)
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
		for {
			response, err := readRawFrame(bufferedReader, connectionAddr, cc.clusterConnContext)
			if err != nil {
				handleConnectionError(
					err, cc.clusterConnContext, cc.cancelFunc, string(cc.connectorType), "reading", connectionAddr)
				break
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
	errorResponseBody, protocolErr, err := decodeProtocolError(response)
	if err != nil {
		log.Errorf("[%s] Error occured while checking if error is a protocol error: %v.", cc.connectorType, err)
		cc.Shutdown()
		return nil
	}

	if protocolErr != nil {
		log.Errorf("[%s] Protocol error occured in async connector, async requests will not be forwarded: %v.", cc.connectorType, err)
		cc.Shutdown()
		return nil
	}

	if response.Header.OpCode == primitive.OpCodeEvent {
		return nil
	}

	reqCtx, done := cc.asyncPendingRequests.markAsDone(response.Header.StreamId, cc.nodeMetrics, response, cc.clusterType, cc.connectorType)
	if done {
		typedReqCtx, ok := reqCtx.(*asyncRequestContextImpl)
		if !ok {
			log.Errorf("Failed to finish async request because request context conversion failed. " +
				"This is most likely a bug, please report. AsyncRequestContext: %v", reqCtx)
		} else if typedReqCtx.expectedResponse {
			response.Header.StreamId = typedReqCtx.requestStreamId
			return response
		} else {
			cc.clientHandlerRequestWg.Done()
			if errorResponseBody != nil {
				switch msg := errorResponseBody.Message.(type) {
				case *message.Unprepared:
					var preparedData PreparedData
					var ok bool
					if cc.clusterType == ClusterTypeTarget {
						preparedData, ok = cc.psCache.GetByTargetPreparedId(msg.Id)
					} else {
						preparedData, ok = cc.psCache.Get(msg.Id)
					}
					if !ok {
						log.Warnf("Received UNPREPARED for async request with prepare ID %v " +
							"but could not find prepared data.", hex.EncodeToString(msg.Id))
					} else {
						prepare := &message.Prepare{
							Query: preparedData.GetPreparedStatementInfo().GetQuery(),
							Keyspace: preparedData.GetPreparedStatementInfo().GetKeyspace(),
						}
						prepareFrame := frame.NewFrame(response.Header.Version, response.Header.StreamId, prepare)
						asyncReqCtx := NewAsyncRequestContext(prepareFrame.Header.StreamId, false)
						newStreamId, err := cc.asyncPendingRequests.store(asyncReqCtx)
						if err != nil {
							log.Warnf("Could not send async PREPARE due to an error while storing the request state: %v.", err.Error())
						} else {
							prepareFrame.Header.StreamId = newStreamId
							prepareRawFrame, err := defaultCodec.ConvertToRawFrame(prepareFrame)
							if err != nil {
								log.Errorf("Could not send async PREPARE because convert raw frame failed: %v.", err.Error())
							} else if !cc.sendAsyncRequestToCluster(prepareRawFrame) {
								log.Warnf("Could not send async PREPARE because either connector wasn't ready " +
									"(current state: %v) or write queue was full.", atomic.LoadInt32(&cc.asyncConnectorState))
							}
						}
					}
				default:
					typedErr, ok := errorResponseBody.Message.(message.Error)
					if ok {
						log.Warnf("Async Request failed with error code %v. Error message: %v", typedErr.GetErrorCode(), typedErr.GetErrorMessage())
					}
				}
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