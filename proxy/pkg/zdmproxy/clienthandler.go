package zdmproxy

import (
	"bytes"
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
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

/*
	ClientHandler holds the 1:1:1 pairing:
    	- a client connector (+ a channel on which the connector sends the requests coming from the client)
    	- a connector to OC
    	- a connector to TC

	Additionally, it has:
    - a global metricHandler object (must be a reference to the one created in the proxy)
	- the prepared statement cache
    - the connection's keyspace, if a USE statement has been issued

*/

type ClientHandler struct {
	clientConnector *ClientConnector

	originCassandraConnector *ClusterConnector
	targetCassandraConnector *ClusterConnector
	asyncConnector           *ClusterConnector

	originControlConn *ControlConn
	targetControlConn *ControlConn

	preparedStatementCache *PreparedStatementCache

	metricHandler *metrics.MetricHandler
	nodeMetrics   *metrics.NodeMetrics

	clientHandlerContext    context.Context
	clientHandlerCancelFunc context.CancelFunc

	currentKeyspaceName *atomic.Value
	handshakeDone       *atomic.Value

	authErrorMessage *message.AuthenticationError

	startupRequest           *atomic.Value
	secondaryStartupResponse *frame.RawFrame
	secondaryHandshakeCreds  *AuthCredentials
	asyncHandshakeCreds      *AuthCredentials

	targetUsername string
	targetPassword string

	originUsername string
	originPassword string

	// map of request context holders that store the contexts for the active requests, keyed on streamID
	requestContextHolders *sync.Map

	// map of request context holders that store the contexts for the active requests that are sent to async connector, keyed on streamID
	asyncRequestContextHolders *sync.Map

	// pending requests map of "fire and forget" requests (kept here so that they can be timed out)
	asyncPendingRequests *pendingRequests

	reqChannel  <-chan *frame.RawFrame
	respChannel chan *Response

	clientHandlerRequestWaitGroup *sync.WaitGroup

	closedRespChannel     bool
	closedRespChannelLock *sync.RWMutex

	responsesDoneChan chan<- bool
	eventsDoneChan    chan<- bool

	requestsDoneCancelFn context.CancelFunc

	requestResponseScheduler  *Scheduler
	clientConnectorScheduler  *Scheduler
	clusterConnectorScheduler *Scheduler

	conf           *config.Config
	topologyConfig *common.TopologyConfig

	localClientHandlerWg *sync.WaitGroup

	originHost *Host
	targetHost *Host

	originObserver *protocolEventObserverImpl
	targetObserver *protocolEventObserverImpl

	primaryCluster               common.ClusterType
	forwardSystemQueriesToTarget bool
	forwardAuthToTarget          bool
	targetCredsOnClientRequest   bool

	queryModifier     *QueryModifier
	parameterModifier *ParameterModifier
	timeUuidGenerator TimeUuidGenerator

	// not used atm but should be used when a protocol error occurs after #68 has been addressed
	clientHandlerShutdownRequestCancelFn context.CancelFunc

	clientHandlerShutdownRequestContext context.Context
}

func NewClientHandler(
	clientTcpConn net.Conn,
	originCassandraConnInfo *ClusterConnectionInfo,
	targetCassandraConnInfo *ClusterConnectionInfo,
	originControlConn *ControlConn,
	targetControlConn *ControlConn,
	conf *config.Config,
	topologyConfig *common.TopologyConfig,
	targetUsername string,
	targetPassword string,
	originUsername string,
	originPassword string,
	psCache *PreparedStatementCache,
	metricHandler *metrics.MetricHandler,
	globalClientHandlersWg *sync.WaitGroup,
	requestResponseScheduler *Scheduler,
	readScheduler *Scheduler,
	writeScheduler *Scheduler,
	numWorkers int,
	globalShutdownRequestCtx context.Context,
	originHost *Host,
	targetHost *Host,
	timeUuidGenerator TimeUuidGenerator,
	readMode common.ReadMode,
	primaryCluster common.ClusterType,
	systemQueriesMode common.SystemQueriesMode) (*ClientHandler, error) {

	originEndpointId := originCassandraConnInfo.endpoint.GetEndpointIdentifier()
	targetEndpointId := targetCassandraConnInfo.endpoint.GetEndpointIdentifier()
	asyncEndpointId := ""
	if readMode == common.ReadModeDualAsyncOnSecondary {
		if primaryCluster == common.ClusterTypeTarget {
			asyncEndpointId = originEndpointId
		} else {
			asyncEndpointId = targetEndpointId
		}
	}

	nodeMetrics, err := metricHandler.GetNodeMetrics(originEndpointId, targetEndpointId, asyncEndpointId)
	if err != nil {
		return nil, fmt.Errorf("failed to create node metrics: %w", err)
	}

	clientHandlerContext, clientHandlerCancelFunc := context.WithCancel(context.Background())
	clientHandlerShutdownRequestContext, clientHandlerShutdownRequestCancelFn := context.WithCancel(globalShutdownRequestCtx)
	requestsDoneCtx, requestsDoneCancelFn := context.WithCancel(context.Background())

	// Initialize stream id processors to manage the ids sent to the clusters
	originFrameProcessor := newFrameProcessor(conf, nodeMetrics, ClusterConnectorTypeOrigin)
	targetFrameProcessor := newFrameProcessor(conf, nodeMetrics, ClusterConnectorTypeTarget)
	asyncFrameProcessor := newFrameProcessor(conf, nodeMetrics, ClusterConnectorTypeAsync)

	closeFrameProcessors := func() {
		originFrameProcessor.Close()
		targetFrameProcessor.Close()
		asyncFrameProcessor.Close()
	}

	localClientHandlerWg := &sync.WaitGroup{}
	globalClientHandlersWg.Add(1)
	go func() {
		defer globalClientHandlersWg.Done()
		<-clientHandlerContext.Done()
		clientHandlerShutdownRequestCancelFn()
		localClientHandlerWg.Wait()
		closeFrameProcessors()
		requestsDoneCancelFn() // make sure this ctx is not leaked but it should be canceled before this
		log.Debugf("Client Handler is shutdown.")
	}()

	respChannel := make(chan *Response, numWorkers)
	clientHandlerRequestWg := &sync.WaitGroup{}
	handshakeDone := &atomic.Value{}

	originConnector, err := NewClusterConnector(
		originCassandraConnInfo, conf, psCache, nodeMetrics, localClientHandlerWg, clientHandlerRequestWg,
		clientHandlerContext, clientHandlerCancelFunc, respChannel, readScheduler, writeScheduler, requestsDoneCtx,
		false, nil, handshakeDone, originFrameProcessor)
	if err != nil {
		clientHandlerCancelFunc()
		return nil, err
	}

	targetConnector, err := NewClusterConnector(
		targetCassandraConnInfo, conf, psCache, nodeMetrics, localClientHandlerWg, clientHandlerRequestWg,
		clientHandlerContext, clientHandlerCancelFunc, respChannel, readScheduler, writeScheduler, requestsDoneCtx,
		false, nil, handshakeDone, targetFrameProcessor)
	if err != nil {
		clientHandlerCancelFunc()
		return nil, err
	}

	asyncPendingRequests := newPendingRequests(nodeMetrics)
	var asyncConnector *ClusterConnector
	if readMode == common.ReadModeDualAsyncOnSecondary {
		var asyncConnInfo *ClusterConnectionInfo
		if primaryCluster == common.ClusterTypeTarget {
			asyncConnInfo = originCassandraConnInfo
		} else {
			asyncConnInfo = targetCassandraConnInfo
		}
		asyncConnector, err = NewClusterConnector(
			asyncConnInfo, conf, psCache, nodeMetrics, localClientHandlerWg, clientHandlerRequestWg,
			clientHandlerContext, clientHandlerCancelFunc, respChannel, readScheduler, writeScheduler, requestsDoneCtx,
			true, asyncPendingRequests, handshakeDone, asyncFrameProcessor)
		if err != nil {
			log.Errorf("Could not create async cluster connector to %s, async requests will not be forwarded: %s", asyncConnInfo.connConfig.GetClusterType(), err.Error())
			asyncConnector = nil
		}
	}

	responsesDoneChan := make(chan bool, 1)
	eventsDoneChan := make(chan bool, 1)
	requestsChannel := make(chan *frame.RawFrame, numWorkers)

	var originObserver, targetObserver *protocolEventObserverImpl
	if originHost != nil {
		originObserver = NewProtocolEventObserver(clientHandlerShutdownRequestCancelFn, originHost)
	}
	if targetHost != nil {
		targetObserver = NewProtocolEventObserver(clientHandlerShutdownRequestCancelFn, targetHost)
	}

	forwardAuthToTarget, targetCredsOnClientRequest := forwardAuthToTarget(
		originControlConn, targetControlConn, conf.ForwardClientCredentialsToOrigin)

	return &ClientHandler{
		clientConnector: NewClientConnector(
			clientTcpConn,
			conf,
			localClientHandlerWg,
			requestsChannel,
			clientHandlerContext,
			clientHandlerCancelFunc,
			responsesDoneChan,
			requestsDoneCtx,
			eventsDoneChan,
			readScheduler,
			writeScheduler,
			clientHandlerShutdownRequestContext,
			clientHandlerShutdownRequestCancelFn),

		asyncConnector:                       asyncConnector,
		originCassandraConnector:             originConnector,
		targetCassandraConnector:             targetConnector,
		originControlConn:                    originControlConn,
		targetControlConn:                    targetControlConn,
		preparedStatementCache:               psCache,
		metricHandler:                        metricHandler,
		nodeMetrics:                          nodeMetrics,
		clientHandlerContext:                 clientHandlerContext,
		clientHandlerCancelFunc:              clientHandlerCancelFunc,
		currentKeyspaceName:                  &atomic.Value{},
		handshakeDone:                        handshakeDone,
		authErrorMessage:                     nil,
		startupRequest:                       &atomic.Value{},
		targetUsername:                       targetUsername,
		targetPassword:                       targetPassword,
		originUsername:                       originUsername,
		originPassword:                       originPassword,
		requestContextHolders:                &sync.Map{},
		asyncRequestContextHolders:           &sync.Map{},
		asyncPendingRequests:                 asyncPendingRequests,
		reqChannel:                           requestsChannel,
		respChannel:                          respChannel,
		clientHandlerRequestWaitGroup:        clientHandlerRequestWg,
		closedRespChannel:                    false,
		closedRespChannelLock:                &sync.RWMutex{},
		responsesDoneChan:                    responsesDoneChan,
		eventsDoneChan:                       eventsDoneChan,
		requestsDoneCancelFn:                 requestsDoneCancelFn,
		requestResponseScheduler:             requestResponseScheduler,
		conf:                                 conf,
		localClientHandlerWg:                 localClientHandlerWg,
		topologyConfig:                       topologyConfig,
		originHost:                           originHost,
		targetHost:                           targetHost,
		originObserver:                       originObserver,
		targetObserver:                       targetObserver,
		primaryCluster:                       primaryCluster,
		forwardSystemQueriesToTarget:         systemQueriesMode == common.SystemQueriesModeTarget,
		forwardAuthToTarget:                  forwardAuthToTarget,
		targetCredsOnClientRequest:           targetCredsOnClientRequest,
		queryModifier:                        NewQueryModifier(timeUuidGenerator),
		parameterModifier:                    NewParameterModifier(timeUuidGenerator),
		timeUuidGenerator:                    timeUuidGenerator,
		clientHandlerShutdownRequestCancelFn: clientHandlerShutdownRequestCancelFn,
		clientHandlerShutdownRequestContext:  clientHandlerShutdownRequestContext,
	}, nil
}

/**
 *	Initialises all components and launches all listening loops that they have.
 */
func (ch *ClientHandler) run(activeClients *int32) {
	ch.clientConnector.run(activeClients)
	ch.originCassandraConnector.run()
	ch.targetCassandraConnector.run()
	if ch.asyncConnector != nil {
		ch.asyncConnector.run()
	}
	ch.requestLoop()
	ch.listenForEventMessages()
	ch.responseLoop()

	addObserver(ch.originObserver, ch.originControlConn)
	addObserver(ch.targetObserver, ch.targetControlConn)

	go func() {
		<-ch.originCassandraConnector.doneChan
		<-ch.targetCassandraConnector.doneChan
		if ch.asyncConnector != nil {
			<-ch.asyncConnector.doneChan
		}
		ch.closedRespChannelLock.Lock()
		defer ch.closedRespChannelLock.Unlock()
		close(ch.respChannel)
		ch.closedRespChannel = true

		removeObserver(ch.originObserver, ch.originControlConn)
		removeObserver(ch.targetObserver, ch.targetControlConn)
	}()
}

func addObserver(observer *protocolEventObserverImpl, controlConn *ControlConn) {
	if observer != nil {
		host := observer.GetHost()
		controlConn.RegisterObserver(observer)
		// check if host was possibly removed before observer was registered
		hosts, err := controlConn.GetHostsInLocalDatacenter()
		if err == nil {
			if _, exists := hosts[host.HostId]; !exists {
				observer.OnHostRemoved(host)
			}
		}
	}
}

func removeObserver(observer *protocolEventObserverImpl, controlConn *ControlConn) {
	if observer != nil {
		controlConn.RemoveObserver(observer)
	}
}

// Infinite loop that blocks on receiving from the requests channel.
func (ch *ClientHandler) requestLoop() {
	ready := false
	var err error
	ch.localClientHandlerWg.Add(1)
	log.Debugf("requestLoop starting now")
	go func() {
		defer ch.localClientHandlerWg.Done()
		connectionAddr := ch.clientConnector.connection.RemoteAddr().String()
		defer log.Debugf("Client Handler request loop %v shutdown.", connectionAddr)
		defer ch.requestsDoneCancelFn()
		defer ch.originCassandraConnector.writeCoalescer.Close()
		defer log.Debugf("Waiting for origin write coalescer to finish...")
		defer ch.targetCassandraConnector.writeCoalescer.Close()
		defer log.Debugf("Waiting for target write coalescer to finish...")
		if ch.asyncConnector != nil {
			defer ch.asyncConnector.writeCoalescer.Close()
			defer log.Debugf("Waiting for async %s write coalescer to finish...", ch.asyncConnector.clusterType)
		}

		wg := &sync.WaitGroup{}
		for {
			f, ok := <-ch.reqChannel
			if !ok {
				break
			}

			if ch.clientHandlerShutdownRequestContext.Err() != nil {
				ch.clientConnector.sendOverloadedToClient(f)
				continue
			}

			log.Tracef("Request received on client handler: %v", f.Header)
			if !ready {
				log.Tracef("not ready")
				// Handle client authentication
				ready, err = ch.handleHandshakeRequest(f, wg)
				if err != nil && !errors.Is(err, ShutdownErr) {
					log.Error(err)
				}
				if ready {
					ch.handshakeDone.Store(true)
					log.Infof(
						"Handshake successful with client %s", connectionAddr)
				}
				log.Tracef("ready? %t", ready)
			} else {
				wg.Add(1)
				ch.requestResponseScheduler.Schedule(func() {
					defer wg.Done()
					ch.handleRequest(f)
				})
			}
		}

		log.Debugf("Shutting down client handler request listener %v.", connectionAddr)

		wg.Wait()

		go func() {
			<-ch.clientHandlerContext.Done()
			ch.clearRequestContexts(ch.requestContextHolders)
			ch.clearRequestContexts(ch.asyncRequestContextHolders)
			if ch.asyncPendingRequests != nil {
				ch.asyncPendingRequests.clear(func(ctx RequestContext) {
					typedReqCtx, ok := ctx.(*asyncRequestContextImpl)
					if !ok {
						log.Errorf("Failed to cancel async request because request context conversion failed. "+
							"This is most likely a bug, please report. AsyncRequestContext: %v", ctx)
					} else {
						if !typedReqCtx.expectedResponse {
							ch.clientHandlerRequestWaitGroup.Done()
						}
					}
				})
			}
		}()

		log.Debugf("Waiting for all in flight requests from %v to finish.", connectionAddr)
		ch.clientHandlerRequestWaitGroup.Wait()
	}()
}

func (ch *ClientHandler) clearRequestContexts(contextHoldersMap *sync.Map) {
	contextHoldersMap.Range(func(key, value interface{}) bool {
		reqCtxHolder := value.(*requestContextHolder)
		reqCtx := reqCtxHolder.Get()
		if reqCtx == nil {
			return true
		}
		canceled := reqCtx.Cancel(ch.nodeMetrics)
		if canceled {
			typedReqCtx, ok := reqCtx.(*requestContextImpl)
			if !ok {
				log.Errorf("Failed to cancel request because request context conversion failed. "+
					"This is most likely a bug, please report. RequestContext: %v", reqCtx)
			} else {
				ch.cancelRequest(reqCtxHolder, typedReqCtx)
			}
		}
		return true
	})
}

// Infinite loop that blocks on receiving from both cluster connector event channels.
//
// Event messages that come through will only be routed if
//   - it's a schema change from origin
func (ch *ClientHandler) listenForEventMessages() {
	ch.localClientHandlerWg.Add(1)
	log.Debugf("listenForEventMessages loop starting now")
	go func() {
		defer ch.localClientHandlerWg.Done()
		defer close(ch.eventsDoneChan)
		shutDownChannels := 0
		targetChannel := ch.targetCassandraConnector.clusterConnEventsChan
		originChannel := ch.originCassandraConnector.clusterConnEventsChan
		for {
			if shutDownChannels >= 2 {
				break
			}

			var event *frame.RawFrame
			var ok bool
			var fromTarget bool

			//goland:noinspection ALL
			select {
			case event, ok = <-targetChannel:
				if !ok {
					log.Debugf("Target event channel closed")
					shutDownChannels++
					targetChannel = nil
					continue
				}
				fromTarget = true
			case event, ok = <-originChannel:
				if !ok {
					log.Debugf("Origin event channel closed")
					shutDownChannels++
					originChannel = nil
					continue
				}
				fromTarget = false
			}

			log.Debugf("Message received (fromTarget: %v) on event listener of the client handler: %v", fromTarget, event.Header)

			body, err := defaultCodec.DecodeBody(event.Header, bytes.NewReader(event.Body))
			if err != nil {
				log.Warnf("Error decoding event response: %v", err)
				continue
			}

			switch msgType := body.Message.(type) {
			case *message.ProtocolError:
				log.Debug("Received protocol error on event body listener, forwarding to client: ", body.Message)
			case *message.SchemaChangeEvent:
				if fromTarget {
					log.Infof("Received schema change event from target, skipping: %v", msgType)
					continue
				}
			case *message.StatusChangeEvent:
				if ch.topologyConfig.VirtualizationEnabled {
					log.Infof("Received status change event (fromTarget=%v) but virtualization is enabled, skipping: %v", fromTarget, msgType)
					continue
				}
				if !fromTarget {
					log.Infof("Received status change event from origin, skipping: %v", msgType)
					continue
				}
			case *message.TopologyChangeEvent:
				if ch.topologyConfig.VirtualizationEnabled {
					log.Infof("Received topology change event (fromTarget=%v) but virtualization is enabled, skipping: %v", fromTarget, msgType)
					continue
				}
				if !fromTarget {
					log.Infof("Received topology change event from origin, skipping: %v", msgType)
					continue
				}
			default:
				log.Infof("Expected event body (fromTarget: %v) but got: %v", fromTarget, msgType)
				continue
			}

			ch.clientConnector.sendResponseToClient(event)
		}

		log.Debugf("Shutting down client event messages listener.")
	}()
}

// Infinite loop that blocks on receiving from the response channel
// (which is written by both cluster connectors).
func (ch *ClientHandler) responseLoop() {
	ch.localClientHandlerWg.Add(1)
	log.Debugf("responseLoop starting now")
	go func() {
		defer ch.localClientHandlerWg.Done()
		defer close(ch.responsesDoneChan)

		wg := &sync.WaitGroup{}
		defer wg.Wait()

		protocolErrOccurred := int32(0)

		for {
			response, ok := <-ch.respChannel
			if !ok {
				break
			}

			wg.Add(1)
			ch.requestResponseScheduler.Schedule(func() {
				defer wg.Done()

				var responseClusterType common.ClusterType
				switch response.connectorType {
				case ClusterConnectorTypeAsync:
					responseClusterType = ch.asyncConnector.clusterType
				case ClusterConnectorTypeOrigin:
					responseClusterType = ch.originCassandraConnector.clusterType
				case ClusterConnectorTypeTarget:
					responseClusterType = ch.targetCassandraConnector.clusterType
				}

				if response.connectorType != ClusterConnectorTypeAsync {
					if ch.tryProcessProtocolError(response, &protocolErrOccurred) {
						return
					}
				}

				streamId := response.GetStreamId()
				var contextHoldersMap *sync.Map
				if response.connectorType == ClusterConnectorTypeAsync {
					contextHoldersMap = ch.asyncRequestContextHolders
				} else {
					contextHoldersMap = ch.requestContextHolders
				}
				holder := getOrCreateRequestContextHolder(contextHoldersMap, streamId)
				reqCtx := holder.Get()
				if reqCtx == nil {
					if ch.clientHandlerContext.Err() == nil {
						log.Warnf("Could not find request context for stream id %d received from %v. "+
							"It either timed out or a protocol error occurred.", streamId, response.connectorType)
					}
					return
				}

				finished := false
				if response.responseFrame == nil {
					finished = reqCtx.SetTimeout(ch.nodeMetrics, response.requestFrame)
				} else {
					finished = reqCtx.SetResponse(ch.nodeMetrics, response.responseFrame, responseClusterType, response.connectorType)
					if reqCtx.GetRequestInfo().ShouldBeTrackedInMetrics() {
						trackClusterErrorMetrics(response.responseFrame, response.connectorType, ch.nodeMetrics)
					}
				}

				if finished {
					typedReqCtx, ok := reqCtx.(*requestContextImpl)
					if !ok {
						log.Errorf("Failed to finish request because request context conversion failed. "+
							"This is most likely a bug, please report. RequestContext: %v", reqCtx)
					} else {
						ch.finishRequest(ch.LoadCurrentKeyspace(), holder, typedReqCtx)
					}
				}
			})
		}

		log.Debugf("Shutting down responseLoop.")
	}()
}

// Checks if response is a protocol error. Returns true if it processes this response. If it returns false,
// then the response wasn't processed and it should be processed by another function.
func (ch *ClientHandler) tryProcessProtocolError(response *Response, protocolErrOccurred *int32) bool {
	errMsg, err := decodeError(response.responseFrame)
	if err != nil {
		log.Errorf("Could not check if error from %v was protocol error: %v, skipping it.",
			response.connectorType, response.responseFrame.Header)
		return false
	} else if errMsg != nil && errMsg.GetErrorCode() == primitive.ErrorCodeProtocolError {
		if atomic.CompareAndSwapInt32(protocolErrOccurred, 0, 1) {
			if ch.handshakeDone.Load() != nil {
				log.Errorf("[ClientHandler] Protocol error detected (%v) on %v, forwarding it to the client.",
					errMsg, response.connectorType)
			} else {
				log.Debugf("[ClientHandler] Protocol version downgrade detected (%v) on %v, forwarding it to the client.",
					errMsg, response.connectorType)
			}
			ch.clientConnector.sendResponseToClient(response.responseFrame)
		}
		return true
	}

	return false
}

func decodeError(responseFrame *frame.RawFrame) (message.Error, error) {
	if responseFrame != nil &&
		responseFrame.Header.OpCode == primitive.OpCodeError {
		body, err := defaultCodec.DecodeBody(
			responseFrame.Header, bytes.NewReader(responseFrame.Body))

		if err != nil {
			return nil, err
		} else {
			errorMsg, ok := body.Message.(message.Error)
			if !ok {
				return nil, fmt.Errorf("expected error message but conversion was not successful: %v", body.Message)
			}
			return errorMsg, nil
		}
	}

	return nil, nil
}

// should only be called after SetTimeout or SetResponse returns true
func (ch *ClientHandler) finishRequest(currentKeyspace string, holder *requestContextHolder, reqCtx *requestContextImpl) {
	defer ch.clientHandlerRequestWaitGroup.Done()

	err := holder.Clear(reqCtx)
	if err != nil {
		log.Debugf("Could not free stream id: %v", err)
	}

	if reqCtx.requestInfo.ShouldBeTrackedInMetrics() {
		proxyMetrics := ch.metricHandler.GetProxyMetrics()
		switch reqCtx.requestInfo.GetForwardDecision() {
		case forwardToBoth:
			proxyMetrics.ProxyWritesDuration.Track(reqCtx.startTime)
			proxyMetrics.InFlightWrites.Subtract(1)
		case forwardToOrigin:
			proxyMetrics.ProxyReadsOriginDuration.Track(reqCtx.startTime)
			proxyMetrics.InFlightReadsOrigin.Subtract(1)
		case forwardToTarget:
			proxyMetrics.ProxyReadsTargetDuration.Track(reqCtx.startTime)
			proxyMetrics.InFlightReadsTarget.Subtract(1)
		case forwardToAsyncOnly, forwardToNone:
		default:
			log.Errorf("unexpected forwardDecision %v, unable to track proxy level metrics", reqCtx.requestInfo.GetForwardDecision())
		}
	}

	aggregatedResponse, responseClusterType, err := ch.computeClientResponse(reqCtx)
	finalResponse := aggregatedResponse
	if err == nil && reqCtx.requestInfo.GetForwardDecision() != forwardToAsyncOnly {
		// async only requests can't have "PREPARED", "SETKEYSPACE" or "UNPREPARED" responses so skip this
		finalResponse, err = ch.processClientResponse(currentKeyspace, aggregatedResponse, responseClusterType, reqCtx)
	}

	if err != nil {
		if reqCtx.customResponseChannel != nil {
			close(reqCtx.customResponseChannel)
		}
		log.Errorf("Error handling request (%v): %v", reqCtx.request.Header, err)
		return
	}

	reqCtx.request = nil
	originResponse := reqCtx.originResponse
	reqCtx.originResponse = nil
	targetResponse := reqCtx.targetResponse
	reqCtx.targetResponse = nil

	if reqCtx.customResponseChannel != nil {
		reqCtx.customResponseChannel <- &customResponse{
			originResponse:     originResponse,
			targetResponse:     targetResponse,
			aggregatedResponse: finalResponse,
		}
	} else {
		ch.clientConnector.sendResponseToClient(finalResponse)
	}
}

// should only be called after Cancel returns true
func (ch *ClientHandler) cancelRequest(holder *requestContextHolder, reqCtx *requestContextImpl) {
	defer ch.clientHandlerRequestWaitGroup.Done()

	err := holder.Clear(reqCtx)
	if err != nil {
		log.Debugf("Could not free stream id: %v", err)
	}

	if reqCtx.requestInfo.ShouldBeTrackedInMetrics() {
		proxyMetrics := ch.metricHandler.GetProxyMetrics()
		switch reqCtx.requestInfo.GetForwardDecision() {
		case forwardToBoth:
			proxyMetrics.InFlightWrites.Subtract(1)
		case forwardToOrigin:
			proxyMetrics.InFlightReadsOrigin.Subtract(1)
		case forwardToTarget:
			proxyMetrics.InFlightReadsTarget.Subtract(1)
		case forwardToAsyncOnly, forwardToNone:
		default:
			log.Errorf("unexpected forwardDecision %v, unable to track proxy level metrics", reqCtx.requestInfo.GetForwardDecision())
		}
	}

	if reqCtx.customResponseChannel != nil {
		close(reqCtx.customResponseChannel)
	}

	log.Tracef("Canceled request %v.", reqCtx.request.Header)
}

// Computes the response to be sent to the client based on the forward decision of the request.
func (ch *ClientHandler) computeClientResponse(requestContext *requestContextImpl) (*frame.RawFrame, common.ClusterType, error) {
	fwdDecision := requestContext.requestInfo.GetForwardDecision()
	switch fwdDecision {
	case forwardToOrigin:
		if requestContext.originResponse == nil {
			return nil, common.ClusterTypeNone, fmt.Errorf(
				"did not receive response from origin cassandra channel, stream: %d",
				requestContext.request.Header.StreamId)
		}
		log.Tracef("Forward to origin: just returning the response received from %v: %d",
			common.ClusterTypeOrigin, requestContext.originResponse.Header.OpCode)

		if requestContext.requestInfo.ShouldBeTrackedInMetrics() && !isResponseSuccessful(requestContext.originResponse) {
			ch.metricHandler.GetProxyMetrics().FailedReadsOrigin.Add(1)
		}
		return requestContext.originResponse, common.ClusterTypeOrigin, nil
	case forwardToTarget:
		if requestContext.targetResponse == nil {
			return nil, common.ClusterTypeNone, fmt.Errorf(
				"did not receive response from target cassandra channel, stream: %d",
				requestContext.request.Header.StreamId)
		}
		log.Tracef("Forward to target: just returning the response received from %v: %d",
			common.ClusterTypeTarget, requestContext.targetResponse.Header.OpCode)

		if requestContext.requestInfo.ShouldBeTrackedInMetrics() && !isResponseSuccessful(requestContext.targetResponse) {
			ch.metricHandler.GetProxyMetrics().FailedReadsTarget.Add(1)
		}
		return requestContext.targetResponse, common.ClusterTypeTarget, nil
	case forwardToBoth:
		if requestContext.originResponse == nil {
			return nil, common.ClusterTypeNone, fmt.Errorf(
				"did not receive response from original cassandra channel, stream: %d",
				requestContext.request.Header.StreamId)
		}
		if requestContext.targetResponse == nil {
			return nil, common.ClusterTypeNone, fmt.Errorf(
				"did not receive response from target cassandra channel, stream: %d",
				requestContext.request.Header.StreamId)
		}
		aggregatedResponse, responseClusterType := ch.aggregateAndTrackResponses(
			requestContext.requestInfo, requestContext.request, requestContext.originResponse, requestContext.targetResponse)
		return aggregatedResponse, responseClusterType, nil
	case forwardToAsyncOnly:
		switch ch.asyncConnector.clusterType {
		case common.ClusterTypeTarget:
			if requestContext.targetResponse == nil {
				return nil, common.ClusterTypeNone, fmt.Errorf(
					"did not receive response from async target cassandra channel, stream: %d",
					requestContext.request.Header.StreamId)
			}
			log.Tracef("Forward to async: just returning the response received from %v: %d",
				common.ClusterTypeTarget, requestContext.targetResponse.Header.OpCode)
			return requestContext.targetResponse, common.ClusterTypeTarget, nil
		case common.ClusterTypeOrigin:
			if requestContext.originResponse == nil {
				return nil, common.ClusterTypeNone, fmt.Errorf(
					"did not receive response from async origin cassandra channel, stream: %d",
					requestContext.request.Header.StreamId)
			}
			log.Tracef("Forward to async: just returning the response received from %v: %d",
				common.ClusterTypeOrigin, requestContext.originResponse.Header.OpCode)
			return requestContext.originResponse, common.ClusterTypeOrigin, nil
		default:
			log.Errorf("Unknown cluster type: %v. This is a bug, please report.", ch.asyncConnector.clusterType)
			return nil, common.ClusterTypeNone, fmt.Errorf("unknown cluster type: %v; this is a bug, please report", ch.asyncConnector.clusterType)
		}
	case forwardToNone:
		return nil, common.ClusterTypeNone, fmt.Errorf(
			"%v is not expected to reach this code, this is a BUG, please report (request context: %v)",
			fwdDecision, requestContext)
	default:
		return nil, common.ClusterTypeNone, fmt.Errorf(
			"unknown forward decision %v, request context: %v", fwdDecision, requestContext)
	}
}

// Modifies internal state based on the provided aggregated response (e.g. storing prepared IDs)
func (ch *ClientHandler) processClientResponse(
	currentKeyspace string, response *frame.RawFrame, responseClusterType common.ClusterType, reqCtx *requestContextImpl) (*frame.RawFrame, error) {

	var newFrame *frame.Frame
	switch response.Header.OpCode {
	case primitive.OpCodeResult, primitive.OpCodeError:
		decodedFrame, err := defaultCodec.ConvertFromRawFrame(response)
		if err != nil {
			return nil, fmt.Errorf("error decoding response: %w", err)
		}

		switch bodyMsg := decodedFrame.Body.Message.(type) {
		case *message.PreparedResult:
			newFrame, err = ch.processPreparedResponse(currentKeyspace, decodedFrame, responseClusterType, bodyMsg, reqCtx)
			if err != nil {
				return nil, fmt.Errorf("failed to handle prepared result: %w", err)
			}
		case *message.SetKeyspaceResult:
			if bodyMsg.Keyspace == "" {
				log.Warnf("unexpected set keyspace empty")
			} else {
				ch.StoreCurrentKeyspace(bodyMsg.Keyspace)
			}
		case *message.Unprepared:
			var preparedEntry PreparedEntry
			var ok bool
			switch responseClusterType {
			case common.ClusterTypeOrigin:
				preparedEntry, ok = ch.preparedStatementCache.GetByOriginPreparedId(bodyMsg.Id)
				if !ok {
					return nil, fmt.Errorf("could not get PreparedData by OriginPreparedId: %v", hex.EncodeToString(bodyMsg.Id))
				}
			case common.ClusterTypeTarget:
				preparedEntry, ok = ch.preparedStatementCache.GetByTargetPreparedId(bodyMsg.Id)
				if !ok {
					return nil, fmt.Errorf("could not get PreparedData by TargetPreparedId: %v", hex.EncodeToString(bodyMsg.Id))
				}
			default:
				return nil, fmt.Errorf("invalid cluster type: %v", responseClusterType)
			}
			unpreparedId := preparedEntry.GetClientPreparedId()
			newFrame = decodedFrame.Clone()
			newUnprepared := &message.Unprepared{
				ErrorMessage: fmt.Sprintf("Prepared query with ID %s not found (either the query was not prepared "+
					"on this host (maybe the host has been restarted?) or you have prepared too many queries and it has "+
					"been evicted from the internal cache)", hex.EncodeToString(unpreparedId[:])),
				Id: unpreparedId[:],
			}
			newFrame.Body.Message = newUnprepared

			log.Infof("Received UNPREPARED from %v, generating UNPREPARED response with prepared ID %s. "+
				"Prepared ID in response from %v: %v. Original error: %v",
				responseClusterType, hex.EncodeToString(unpreparedId[:]),
				responseClusterType, hex.EncodeToString(bodyMsg.Id), bodyMsg.ErrorMessage)
		}
	}

	if newFrame == nil {
		return response, nil
	}

	newRawFrame, err := defaultCodec.ConvertToRawFrame(newFrame)
	if err != nil {
		return nil, fmt.Errorf("could not convert new response: %w", err)
	}
	return newRawFrame, nil
}

func (ch *ClientHandler) processPreparedResponse(
	currentKeyspace string, response *frame.Frame, responseClusterType common.ClusterType,
	bodyMsg *message.PreparedResult, reqCtx *requestContextImpl) (*frame.Frame, error) {
	if bodyMsg.PreparedQueryId == nil {
		return nil, errors.New("unexpected prepared query id nil")
	} else if reqCtx.requestInfo == nil {
		return nil, errors.New("unexpected statement info nil on request context")
	} else if prepareRequestInfo, ok := reqCtx.requestInfo.(*PrepareRequestInfo); !ok {
		return nil, errors.New("unexpected request context statement info is not prepared statement info")
	} else if reqCtx.targetResponse == nil && reqCtx.originResponse == nil {
		return nil, errors.New("unexpected target response and origin response nil")
	} else {
		newResponse := response.Clone()
		newPreparedBody, ok := newResponse.Body.Message.(*message.PreparedResult)
		if !ok {
			return nil, fmt.Errorf("expected PREPARED RESULT targetBody in target result response but got %v",
				newResponse.Body.Message.GetOpCode().String())
		}

		err := ch.replaceTermsOnPreparedBody(newPreparedBody, prepareRequestInfo, bodyMsg)
		if err != nil {
			return nil, fmt.Errorf("error replacing terms on prepared result: %w", err)
		}

		var originPreparedResult *message.PreparedResult
		var targetPreparedResult *message.PreparedResult

		if reqCtx.originResponse != nil {
			var preparedBodyMsg *message.PreparedResult
			if responseClusterType == common.ClusterTypeOrigin {
				preparedBodyMsg = bodyMsg
			} else {
				decodedFrame, err := defaultCodec.ConvertFromRawFrame(reqCtx.originResponse)
				if err != nil {
					return nil, fmt.Errorf("error decoding origin response: %w", err)
				}
				typedResult, ok := decodedFrame.Body.Message.(*message.PreparedResult)
				if !ok {
					return nil, fmt.Errorf("received prepare response on origin but couldnt decode the result: %v",
						decodedFrame.Body.Message.GetOpCode().String())
				}
				preparedBodyMsg = typedResult
			}
			originPreparedResult = preparedBodyMsg
		}

		if reqCtx.targetResponse != nil {
			var preparedBodyMsg *message.PreparedResult
			if responseClusterType == common.ClusterTypeTarget {
				preparedBodyMsg = bodyMsg
			} else {
				decodedFrame, err := defaultCodec.ConvertFromRawFrame(reqCtx.targetResponse)
				if err != nil {
					return nil, fmt.Errorf("error decoding target response: %w", err)
				}
				typedResult, ok := decodedFrame.Body.Message.(*message.PreparedResult)
				if !ok {
					return nil, fmt.Errorf("received prepare response on target but couldnt decode the result: %v",
						decodedFrame.Body.Message.GetOpCode().String())
				}
				preparedBodyMsg = typedResult
			}
			targetPreparedResult = preparedBodyMsg
		}

		var preparedEntry PreparedEntry
		if originPreparedResult != nil && targetPreparedResult != nil {
			preparedEntry = ch.preparedStatementCache.StorePreparedOnBoth(originPreparedResult, targetPreparedResult, prepareRequestInfo)
		} else if originPreparedResult != nil {
			preparedEntry = ch.preparedStatementCache.StorePreparedOnOrigin(originPreparedResult, prepareRequestInfo)
		} else if targetPreparedResult != nil {
			preparedEntry = ch.preparedStatementCache.StorePreparedOnTarget(targetPreparedResult, prepareRequestInfo)
		} else {
			return nil, errors.New("could not retrieve client prepared id because both prepared results from origin and target are nil")
		}

		clientId := preparedEntry.GetClientPreparedId()
		newPreparedBody.PreparedQueryId = clientId[:]

		return newResponse, nil
	}
}

func (ch *ClientHandler) replaceTermsOnPreparedBody(newPreparedBody *message.PreparedResult,
	prepareRequestInfo *PrepareRequestInfo,
	bodyMsg *message.PreparedResult) error {

	if len(prepareRequestInfo.replacedTerms) > 0 {
		if bodyMsg.VariablesMetadata == nil {
			return fmt.Errorf("replaced terms in the prepared statement but prepared result doesn't have variables metadata: %v", bodyMsg)
		}

		if prepareRequestInfo.ContainsPositionalMarkers() {
			positionalMarkersToRemove := make([]int, 0, len(prepareRequestInfo.replacedTerms))
			positionalMarkerOffset := 0
			for _, replacedTerm := range prepareRequestInfo.replacedTerms {
				positionalMarkersToRemove = append(
					positionalMarkersToRemove,
					positionalMarkerOffset+replacedTerm.previousPositionalIndex+1)
				positionalMarkerOffset++
			}

			if len(newPreparedBody.VariablesMetadata.Columns) < len(positionalMarkersToRemove) {
				return fmt.Errorf("prepared response variables metadata has less parameters than the number of markers to remove: %v", newPreparedBody)
			}

			newColumns := make([]*message.ColumnMetadata, 0, len(newPreparedBody.VariablesMetadata.Columns)-len(positionalMarkersToRemove))
			indicesToRemove := make([]int, 0, len(positionalMarkersToRemove))
			start := 0
			for _, positionalIndexToRemove := range positionalMarkersToRemove {
				if positionalIndexToRemove < len(newPreparedBody.VariablesMetadata.Columns)-1 {
					indicesToRemove = append(indicesToRemove, positionalIndexToRemove)
					newColumns = append(newColumns, newPreparedBody.VariablesMetadata.Columns[start:positionalIndexToRemove]...)
					start = positionalIndexToRemove + 1
				}
			}
			newColumns = append(newColumns, newPreparedBody.VariablesMetadata.Columns[start:]...)

			if len(indicesToRemove) > 0 && len(newPreparedBody.VariablesMetadata.PkIndices) > 0 {
				sort.Ints(indicesToRemove)
				var newPkIndices []uint16
				for _, pkIndex := range newPreparedBody.VariablesMetadata.PkIndices {
					foundIndex := sort.SearchInts(indicesToRemove, int(pkIndex))
					if foundIndex == len(indicesToRemove) {
						newPkIndices = append(newPkIndices, pkIndex)
					}
				}

				newPreparedBody.VariablesMetadata.PkIndices = newPkIndices
			}

			newPreparedBody.VariablesMetadata.Columns = newColumns
		} else {
			namedMarkersToRemove := GetSortedZdmNamedMarkers()
			newCols := make([]*message.ColumnMetadata, 0, len(newPreparedBody.VariablesMetadata.Columns))
			indicesToRemove := make([]int, 0, len(namedMarkersToRemove))
			start := 0
			for idx, col := range newPreparedBody.VariablesMetadata.Columns {
				if col.Name == "" {
					continue
				}

				if sort.SearchStrings(namedMarkersToRemove, col.Name) != len(namedMarkersToRemove) {
					indicesToRemove = append(indicesToRemove, idx)
					newCols = append(newCols, newPreparedBody.VariablesMetadata.Columns[start:idx]...)
					start = idx + 1
				}
			}

			newCols = append(newCols, newPreparedBody.VariablesMetadata.Columns[start:]...)

			if len(indicesToRemove) > 0 && len(newPreparedBody.VariablesMetadata.PkIndices) > 0 {
				sort.Ints(indicesToRemove)
				var newPkIndices []uint16
				for _, pkIndex := range newPreparedBody.VariablesMetadata.PkIndices {
					foundIndex := sort.SearchInts(indicesToRemove, int(pkIndex))
					if foundIndex == len(indicesToRemove) {
						newPkIndices = append(newPkIndices, pkIndex)
					}
				}

				newPreparedBody.VariablesMetadata.PkIndices = newPkIndices
			}

			newPreparedBody.VariablesMetadata.Columns = newCols
		}
	}
	return nil
}

type handshakeRequestResult struct {
	authSuccess        bool
	err                error
	customResponseChan chan *customResponse
}

type startHandshakeResult struct {
	secondaryHandshakeCh chan error
	asyncHandshakeCh     chan error
	err                  error
}

// Handles requests while the handshake has not been finalized.
//
// Forwards certain requests that are part of the handshake to Origin only.
//
// When the Origin handshake ends, this function blocks, waiting until Target handshake is done.
// This ensures that the client connection is Ready only when both Cluster Connector connections are ready.
func (ch *ClientHandler) handleHandshakeRequest(request *frame.RawFrame, wg *sync.WaitGroup) (bool, error) {
	scheduledTaskChannel := make(chan *handshakeRequestResult, 1)
	wg.Add(1)
	ch.requestResponseScheduler.Schedule(func() {
		defer wg.Done()
		defer close(scheduledTaskChannel)
		if ch.authErrorMessage != nil {
			secondaryClusterType := common.ClusterTypeTarget
			if ch.forwardAuthToTarget {
				secondaryClusterType = common.ClusterTypeOrigin
			}
			scheduledTaskChannel <- &handshakeRequestResult{
				authSuccess: false,
				err:         ch.sendAuthErrorToClient(request, secondaryClusterType),
			}
			return
		}

		if request.Header.OpCode == primitive.OpCodeAuthResponse {
			newAuthFrame, err := ch.handleClientCredentials(request)
			if err != nil {
				scheduledTaskChannel <- &handshakeRequestResult{
					authSuccess: false,
					err:         err,
				}
				return
			}

			if newAuthFrame != nil {
				request = newAuthFrame
			}
		}

		responseChan := make(chan *customResponse, 1)
		err := ch.forwardRequest(request, responseChan)
		if err != nil {
			scheduledTaskChannel <- &handshakeRequestResult{
				authSuccess: false,
				err:         err,
			}
			return
		}

		scheduledTaskChannel <- &handshakeRequestResult{
			authSuccess:        false,
			err:                err,
			customResponseChan: responseChan,
		}
	})

	result, ok := <-scheduledTaskChannel
	if !ok {
		return false, errors.New("unexpected scheduledTaskChannel closure in handle handshake request")
	}

	if result.customResponseChan == nil {
		return result.authSuccess, result.err
	}

	var response *customResponse
	select {
	case response, _ = <-result.customResponseChan:
	case <-ch.clientHandlerContext.Done():
		return false, ShutdownErr
	}

	if response == nil {
		return false, fmt.Errorf("no response received for handshake request %v", request)
	}

	aggregatedResponse := response.aggregatedResponse

	if request.Header.OpCode == primitive.OpCodeStartup {
		var secondaryResponse *frame.RawFrame
		var secondaryCluster common.ClusterType
		if ch.forwardAuthToTarget {
			// secondary is ORIGIN

			if response.originResponse == nil {
				return false, fmt.Errorf("no response received from %v for startup %v", common.ClusterTypeOrigin, request)
			}
			secondaryResponse = response.originResponse
			aggregatedResponse = response.targetResponse
			secondaryCluster = common.ClusterTypeOrigin
		} else {
			// secondary is TARGET

			if response.targetResponse == nil {
				return false, fmt.Errorf("no response received from %v for startup %v", common.ClusterTypeTarget, request)
			}
			secondaryResponse = response.targetResponse
			aggregatedResponse = response.originResponse
			secondaryCluster = common.ClusterTypeTarget
		}

		ch.secondaryStartupResponse = secondaryResponse
		ch.startupRequest.Store(request)

		err := validateSecondaryStartupResponse(secondaryResponse, secondaryCluster)
		if err != nil {
			return false, fmt.Errorf("unsuccessful startup on %v: %w", secondaryCluster, err)
		}
	}

	startHandshakeCh := make(chan *startHandshakeResult, 1)
	wg.Add(1)
	ch.requestResponseScheduler.Schedule(func() {
		defer wg.Done()
		defer close(startHandshakeCh)
		tempResult := &startHandshakeResult{
			secondaryHandshakeCh: nil,
			asyncHandshakeCh:     nil,
			err:                  nil,
		}
		if aggregatedResponse.Header.OpCode == primitive.OpCodeReady || aggregatedResponse.Header.OpCode == primitive.OpCodeAuthSuccess {
			// target handshake must happen within a single client request lifetime
			// to guarantee that no other request with the same
			// stream id goes to target in the meantime

			// if we add stream id mapping logic in the future, then
			// we can start the secondary handshake earlier and wait for it to end here
			secondaryHandshakeChannel, err := ch.startSecondaryHandshake(false)
			if err != nil {
				tempResult.err = err
				startHandshakeCh <- tempResult
				return
			}
			var asyncConnectorHandshakeChannel chan error
			if ch.asyncConnector != nil {
				asyncConnectorHandshakeChannel, err = ch.startSecondaryHandshake(true)
				if err != nil {
					log.Errorf("Error occured in async connector (%v) handshake: %v. "+
						"Async requests will not be forwarded.", ch.asyncConnector.clusterType, err.Error())
					ch.asyncConnector.Shutdown()
					asyncConnectorHandshakeChannel = nil
				}
			}
			tempResult.secondaryHandshakeCh = secondaryHandshakeChannel
			tempResult.asyncHandshakeCh = asyncConnectorHandshakeChannel
			startHandshakeCh <- tempResult
		}
	})

	var errAsync error
	var errSecondary error
	handshakeInitiated := false
	secondaryHandshakeResult, ok := <-startHandshakeCh
	if ok {
		handshakeInitiated = true
		if secondaryHandshakeResult.err != nil {
			return false, secondaryHandshakeResult.err
		}
		asyncConnectorHandshakeChannel := secondaryHandshakeResult.asyncHandshakeCh
		secondaryHandshakeChannel := secondaryHandshakeResult.secondaryHandshakeCh
		for asyncConnectorHandshakeChannel != nil || secondaryHandshakeChannel != nil {
			select {
			case errAsync, _ = <-asyncConnectorHandshakeChannel:
				asyncConnectorHandshakeChannel = nil
			case errSecondary, _ = <-secondaryHandshakeChannel:
				secondaryHandshakeChannel = nil
			}
		}
	}

	scheduledTaskChannel = make(chan *handshakeRequestResult, 1)
	wg.Add(1)
	ch.requestResponseScheduler.Schedule(func() {
		defer wg.Done()
		defer close(scheduledTaskChannel)
		tempResult := &handshakeRequestResult{
			authSuccess:        false,
			err:                nil,
			customResponseChan: nil,
		}
		secondaryClusterType := common.ClusterTypeTarget
		if ch.forwardAuthToTarget {
			secondaryClusterType = common.ClusterTypeOrigin
		}
		if handshakeInitiated {
			if errAsync != nil {
				log.Errorf("Async connector (%v) handshake failed, async requests will not be forwarded: %s",
					ch.asyncConnector.clusterType, errAsync.Error())
				ch.asyncConnector.Shutdown()
			}

			if errSecondary != nil {
				var authError *AuthError
				if errors.As(errSecondary, &authError) {
					ch.authErrorMessage = authError.errMsg
					tempResult.err = ch.sendAuthErrorToClient(request, secondaryClusterType)
					scheduledTaskChannel <- tempResult
					return
				}

				log.Errorf("Secondary (%v) handshake failed (client: %v), shutting down the client handler and connectors: %s",
					secondaryClusterType, ch.clientConnector.connection.RemoteAddr().String(), errSecondary.Error())
				ch.clientHandlerCancelFunc()
				tempResult.err = fmt.Errorf("handshake failed: %w", ShutdownErr)
				scheduledTaskChannel <- tempResult
				return
			}

			tempResult.authSuccess = true
			ch.clientConnector.sendResponseToClient(aggregatedResponse)
			scheduledTaskChannel <- tempResult
			return
		}

		// send overall response back to client
		ch.clientConnector.sendResponseToClient(aggregatedResponse)
		scheduledTaskChannel <- tempResult
	})

	result, ok = <-scheduledTaskChannel
	if !ok {
		return false, errors.New("unexpected scheduledTaskChannel closure in handle handshake request")
	}
	return result.authSuccess, result.err
}

// Builds auth error response and sends it to the client.
func (ch *ClientHandler) sendAuthErrorToClient(requestFrame *frame.RawFrame, secondaryClusterType common.ClusterType) error {
	authErrorResponse, err := ch.buildAuthErrorResponse(requestFrame, ch.authErrorMessage)
	if err == nil {
		log.Warnf("Secondary (%v) handshake failed with an auth error, returning %v to client.", secondaryClusterType, ch.authErrorMessage)
		ch.clientConnector.sendResponseToClient(authErrorResponse)
		return nil
	} else {
		return fmt.Errorf("secondary handshake failed with an auth error but could not create response frame: %w", err)
	}
}

// Build authentication error response to return to client
func (ch *ClientHandler) buildAuthErrorResponse(
	requestFrame *frame.RawFrame, authenticationError *message.AuthenticationError) (*frame.RawFrame, error) {
	f := frame.NewFrame(requestFrame.Header.Version, requestFrame.Header.StreamId, authenticationError)
	if requestFrame.Header.Flags.Contains(primitive.HeaderFlagCompressed) {
		f.SetCompress(true)
	}

	return defaultCodec.ConvertToRawFrame(f)
}

// Starts the secondary handshake in the background (goroutine).
//
// Returns error if the handshake could not be started.
//
// If the handshake is started but fails, the returned channel will contain the error.
//
// If the returned channel is closed before a value could be read, then the handshake has failed as well.
//
// The handshake was successful if the returned channel contains a "nil" value.
func (ch *ClientHandler) startSecondaryHandshake(asyncConnector bool) (chan error, error) {
	startupFrameInterface := ch.startupRequest.Load()
	if startupFrameInterface == nil {
		return nil, errors.New("can not start secondary handshake before a Startup request was received")
	}
	startupFrame := startupFrameInterface.(*frame.RawFrame)
	startupResponse := ch.secondaryStartupResponse
	if startupResponse == nil {
		return nil, errors.New("can not start secondary handshake before a Startup response was received")
	}

	channel := make(chan error)
	ch.clientHandlerRequestWaitGroup.Add(1)
	go func() {
		defer ch.clientHandlerRequestWaitGroup.Done()
		defer close(channel)
		var err error
		err = ch.handleSecondaryHandshakeStartup(startupFrame, startupResponse, asyncConnector)
		channel <- err
	}()
	return channel, nil
}

// Handles a request, see the docs for the forwardRequest() function, as handleRequest is pretty much a wrapper
// around forwardRequest.
func (ch *ClientHandler) handleRequest(f *frame.RawFrame) {
	err := ch.forwardRequest(f, nil)

	if err != nil {
		log.Warnf("error sending request with opcode %v and streamid %d: %s", f.Header.OpCode.String(), f.Header.StreamId, err.Error())
		return
	}
}

// Forwards the request, parsing it and enqueuing it to the appropriate cluster connector(s)' write queue(s).
func (ch *ClientHandler) forwardRequest(request *frame.RawFrame, customResponseChannel chan *customResponse) error {
	overallRequestStartTime := time.Now()

	log.Tracef("Request frame: %v", request)

	currentKeyspace := ch.LoadCurrentKeyspace()
	context := NewFrameDecodeContext(request)
	var replacedTerms []*statementReplacedTerms
	var err error
	if ch.conf.ReplaceCqlFunctions {
		context, replacedTerms, err = ch.queryModifier.replaceQueryString(currentKeyspace, context)
	}

	if err != nil {
		return err
	}
	requestInfo, err := buildRequestInfo(
		context, replacedTerms, ch.preparedStatementCache, ch.metricHandler, currentKeyspace, ch.primaryCluster,
		ch.forwardSystemQueriesToTarget, ch.topologyConfig.VirtualizationEnabled, ch.forwardAuthToTarget, ch.timeUuidGenerator)
	if err != nil {
		if errVal, ok := err.(*UnpreparedExecuteError); ok {
			unpreparedFrame, err := createUnpreparedFrame(errVal)
			if err != nil {
				return err
			}
			log.Debugf(
				"PS Cache miss, created unprepared response with version %v, streamId %v and preparedId %s",
				errVal.Header.Version, errVal.Header.StreamId, errVal.preparedId)

			// send it back to client
			ch.clientConnector.sendResponseToClient(unpreparedFrame)
			log.Debugf("Unprepared Response sent, exiting handleRequest now")
			return nil
		}
		return err
	}

	requestTimeout := time.Duration(ch.conf.ProxyRequestTimeoutMs) * time.Millisecond
	err = ch.executeRequest(context, requestInfo, currentKeyspace, overallRequestStartTime, customResponseChannel, requestTimeout)
	if err != nil {
		return err
	}
	return nil
}

// executeRequest executes the forward decision and waits for one or two responses, then returns the response
// that should be sent back to the client.
func (ch *ClientHandler) executeRequest(
	frameContext *frameDecodeContext, requestInfo RequestInfo, currentKeyspace string,
	overallRequestStartTime time.Time, customResponseChannel chan *customResponse, requestTimeout time.Duration) error {
	fwdDecision := requestInfo.GetForwardDecision()
	log.Tracef("Opcode: %v, Forward decision: %v", frameContext.GetRawFrame().Header.OpCode, fwdDecision)

	f := frameContext.GetRawFrame()
	originRequest := f
	targetRequest := f
	var clientResponse *frame.RawFrame
	var err error

	switch castedRequestInfo := requestInfo.(type) {
	case *InterceptedRequestInfo:
		clientResponse, err = ch.handleInterceptedRequest(castedRequestInfo, frameContext, currentKeyspace)
	case *PrepareRequestInfo:
		clientResponse, originRequest, targetRequest, err = ch.handlePrepareRequest(castedRequestInfo, frameContext, currentKeyspace)
	case *ExecuteRequestInfo:
		clientResponse, originRequest, targetRequest, err = ch.handleExecuteRequest(castedRequestInfo, frameContext, currentKeyspace)
	case *BatchRequestInfo:
		originRequest, targetRequest, err = ch.handleBatchRequest(castedRequestInfo, frameContext)
	}

	if err != nil {
		return err
	}

	if fwdDecision == forwardToNone {
		if clientResponse == nil {
			return fmt.Errorf("forwardDecision is NONE but client response is nil")
		}

		if customResponseChannel != nil {
			customResponseChannel <- &customResponse{aggregatedResponse: clientResponse}
		} else {
			ch.clientConnector.sendResponseToClient(clientResponse)
		}

		return nil
	}

	reqCtx := NewRequestContext(f, requestInfo, overallRequestStartTime, customResponseChannel)
	var contextHoldersMap *sync.Map
	if fwdDecision == forwardToAsyncOnly {
		contextHoldersMap = ch.asyncRequestContextHolders // different map because of stream id collision
	} else {
		contextHoldersMap = ch.requestContextHolders
	}
	holder, err := storeRequestContext(contextHoldersMap, reqCtx)
	if err != nil {
		return err
	}

	if requestInfo.ShouldBeTrackedInMetrics() {
		proxyMetrics := ch.metricHandler.GetProxyMetrics()
		switch fwdDecision {
		case forwardToBoth:
			proxyMetrics.InFlightWrites.Add(1)
		case forwardToOrigin:
			proxyMetrics.InFlightReadsOrigin.Add(1)
		case forwardToTarget:
			proxyMetrics.InFlightReadsTarget.Add(1)
		case forwardToAsyncOnly:
		default:
			log.Errorf("unexpected forwardDecision %v, unable to track proxy level metrics", fwdDecision)
		}
	}

	ch.clientHandlerRequestWaitGroup.Add(1)
	if fwdDecision != forwardToAsyncOnly {
		timer := time.AfterFunc(requestTimeout, func() {
			ch.closedRespChannelLock.RLock()
			defer ch.closedRespChannelLock.RUnlock()
			if ch.closedRespChannel {
				finished := reqCtx.SetTimeout(ch.nodeMetrics, f)
				if finished {
					ch.finishRequest(currentKeyspace, holder, reqCtx)
				}
				return
			}
			ch.respChannel <- NewTimeoutResponse(f, false)
		})
		reqCtx.SetTimer(timer)
	}

	startupFrameInterface := ch.startupRequest.Load()
	// Determine the negotiated protocol version as stored by the startup frame to be used in eventual heartbeats.
	var startupFrameVersion primitive.ProtocolVersion
	if startupFrameInterface != nil {
		startupFrameVersion = startupFrameInterface.(*frame.RawFrame).Header.Version
	}

	switch fwdDecision {
	case forwardToBoth:
		log.Tracef("Forwarding request with opcode %v for stream %v to %v and %v",
			f.Header.OpCode, f.Header.StreamId, common.ClusterTypeOrigin, common.ClusterTypeTarget)
		ch.originCassandraConnector.sendRequestToCluster(originRequest)
		ch.targetCassandraConnector.sendRequestToCluster(targetRequest)
	case forwardToOrigin:
		log.Tracef("Forwarding request with opcode %v for stream %v to %v",
			f.Header.OpCode, f.Header.StreamId, common.ClusterTypeOrigin)
		ch.originCassandraConnector.sendRequestToCluster(originRequest)
		ch.targetCassandraConnector.sendHeartbeat(startupFrameVersion, ch.conf.HeartbeatIntervalMs)
	case forwardToTarget:
		log.Tracef("Forwarding request with opcode %v for stream %v to %v",
			f.Header.OpCode, f.Header.StreamId, common.ClusterTypeTarget)
		ch.targetCassandraConnector.sendRequestToCluster(targetRequest)
		ch.originCassandraConnector.sendHeartbeat(startupFrameVersion, ch.conf.HeartbeatIntervalMs)
	case forwardToAsyncOnly:
	default:
		return fmt.Errorf("unknown forward decision %v, stream: %d", fwdDecision, f.Header.StreamId)
	}

	sendAlsoToAsync := requestInfo.ShouldAlsoBeSentAsync() && ch.asyncConnector != nil
	if !sendAlsoToAsync && fwdDecision != forwardToAsyncOnly {
		return nil
	}

	// from this point onwards, the request is meant to be sent to async connector
	// it can be a request that should ALSO be sent to async as fire and forget
	// or a request that ONLY needs to be sent to the async connector (and a response is expected, i.e. not fire and forget)
	// like async connector handshake requests

	return ch.sendToAsyncConnector(
		currentKeyspace, frameContext, originRequest, targetRequest, fwdDecision, reqCtx, holder, sendAlsoToAsync,
		overallRequestStartTime, requestTimeout)
}

func (ch *ClientHandler) handleInterceptedRequest(
	requestInfo RequestInfo, frameContext *frameDecodeContext, currentKeyspace string) (*frame.RawFrame, error) {

	prepared := false
	var interceptedRequestInfo *InterceptedRequestInfo
	prepareRequestInfo, ok := requestInfo.(*PrepareRequestInfo)
	if ok {
		interceptedRequestInfo, ok = prepareRequestInfo.GetBaseRequestInfo().(*InterceptedRequestInfo)
		if !ok {
			return nil, fmt.Errorf("expected intercepted request info on prepare request info but got %v", prepareRequestInfo.GetBaseRequestInfo())
		}
		prepared = true
	} else {
		interceptedRequestInfo, ok = requestInfo.(*InterceptedRequestInfo)
		if !ok {
			return nil, fmt.Errorf("expected intercepted request info but got %v", requestInfo)
		}
	}

	f := frameContext.GetRawFrame()
	interceptedQueryType := interceptedRequestInfo.GetQueryType()
	var interceptedQueryResponse message.Message
	var controlConn *ControlConn
	if ch.forwardSystemQueriesToTarget {
		controlConn = ch.targetControlConn
	} else {
		controlConn = ch.originControlConn
	}
	virtualHosts, err := controlConn.GetVirtualHosts()
	if err != nil {
		return nil, err
	}

	typeCodec := GetDefaultGenericTypeCodec()

	switch interceptedQueryType {
	case peersV2:
		interceptedQueryResponse = &message.Invalid{
			ErrorMessage: "unconfigured table peers_v2",
		}
	case peersV1:
		parsedSelectClause := interceptedRequestInfo.GetParsedSelectClause()
		if parsedSelectClause == nil {
			return nil, fmt.Errorf("unable to intercept system.peers query (prepared=%v) because parsed select clause is nil", prepared)
		}
		interceptedQueryResponse, err = NewSystemPeersResult(prepareRequestInfo, currentKeyspace,
			typeCodec, f.Header.Version, controlConn.GetSystemPeersColumnNames(), controlConn.GetSystemLocalColumnData(),
			parsedSelectClause, virtualHosts, controlConn.GetLocalVirtualHostIndex(), ch.conf.ProxyListenPort)
	case local:
		parsedSelectClause := interceptedRequestInfo.GetParsedSelectClause()
		if parsedSelectClause == nil {
			return nil, fmt.Errorf("unable to intercept system.local query (prepared=%v) because parsed select clause is nil", prepared)
		}
		localVirtualHost := virtualHosts[controlConn.GetLocalVirtualHostIndex()]
		interceptedQueryResponse, err = NewSystemLocalResult(prepareRequestInfo, currentKeyspace,
			typeCodec, f.Header.Version, controlConn.GetSystemLocalColumnData(), parsedSelectClause,
			localVirtualHost, ch.conf.ProxyListenPort)
	default:
		return nil, fmt.Errorf("expected intercepted query type: %v", interceptedQueryType)
	}

	if err != nil {
		if errVal, ok := err.(*ColumnNotFoundErr); ok {
			interceptedQueryResponse = &message.Invalid{
				ErrorMessage: fmt.Sprintf("Undefined column name %v", errVal.Name),
			}
		} else {
			return nil, err
		}
	}

	interceptedResponseFrame := frame.NewFrame(f.Header.Version, f.Header.StreamId, interceptedQueryResponse)
	interceptedResponseRawFrame, err := defaultCodec.ConvertToRawFrame(interceptedResponseFrame)
	if err != nil {
		return nil, fmt.Errorf("could not convert intercepted response frame %v: %w", interceptedResponseFrame, err)
	}

	if prepareRequestInfo != nil {
		ch.preparedStatementCache.StoreIntercepted(prepareRequestInfo)
	}

	return interceptedResponseRawFrame, nil
}

func (ch *ClientHandler) handlePrepareRequest(
	castedRequestInfo *PrepareRequestInfo, frameContext *frameDecodeContext, currentKeyspace string) (
	clientResponse *frame.RawFrame, originRequest *frame.RawFrame, targetRequest *frame.RawFrame, err error) {

	f := frameContext.GetRawFrame()
	switch castedRequestInfo.GetBaseRequestInfo().(type) {
	case *InterceptedRequestInfo:
		clientResponse, err = ch.handleInterceptedRequest(castedRequestInfo, frameContext, currentKeyspace)
		if err != nil {
			return nil, nil, nil, err
		}
		originRequest = nil
		targetRequest = nil
	default:
		originRequest = f
		targetRequest = f
	}
	return clientResponse, originRequest, targetRequest, nil
}

func (ch *ClientHandler) handleExecuteRequest(
	castedRequestInfo *ExecuteRequestInfo, frameContext *frameDecodeContext, currentKeyspace string) (
	clientResponse *frame.RawFrame, originRequest *frame.RawFrame, targetRequest *frame.RawFrame, err error) {

	f := frameContext.GetRawFrame()
	originRequest = f
	targetRequest = f

	preparedEntry := castedRequestInfo.GetPreparedEntry()
	prepareRequestInfo := preparedEntry.GetPrepareRequestInfo()
	fwdDecision := castedRequestInfo.GetForwardDecision()

	if fwdDecision == forwardToNone {
		interceptedRequestInfo, ok := prepareRequestInfo.GetBaseRequestInfo().(*InterceptedRequestInfo)
		if !ok {
			return nil, nil, nil, fmt.Errorf(
				"expected intercepted statement info while handling bound statement but got %v", prepareRequestInfo.GetBaseRequestInfo())
		}
		clientResponse, err = ch.handleInterceptedRequest(interceptedRequestInfo, frameContext, currentKeyspace)
		if err != nil {
			return nil, nil, nil, err
		}

		return clientResponse, nil, nil, err
	}

	sendToAsyncConnector := (castedRequestInfo.ShouldAlsoBeSentAsync() || fwdDecision == forwardToAsyncOnly) && ch.asyncConnector != nil
	asyncConnectorIsOrigin := ch.asyncConnector != nil && ch.asyncConnector.clusterType == common.ClusterTypeOrigin

	originExecuteRequestRaw, replacementTimeUuids, err := ch.rewriteOriginPrepare(
		castedRequestInfo, frameContext, sendToAsyncConnector, asyncConnectorIsOrigin)
	if err != nil {
		return nil, nil, nil, err
	}
	originRequest = originExecuteRequestRaw

	asyncConnectorIsTarget := ch.asyncConnector != nil && ch.asyncConnector.clusterType == common.ClusterTypeTarget
	targetRequest, err = ch.rewriteTargetPrepare(
		castedRequestInfo, frameContext, replacementTimeUuids, sendToAsyncConnector, asyncConnectorIsTarget)

	if err != nil {
		return nil, nil, nil, err
	}

	return nil, originRequest, targetRequest, nil
}

func (ch *ClientHandler) rewriteOriginPrepare(castedRequestInfo *ExecuteRequestInfo, frameContext *frameDecodeContext,
	sendToAsyncConnector bool, asyncConnectorIsOrigin bool) (*frame.RawFrame, []*uuid.UUID, error) {
	preparedEntry := castedRequestInfo.GetPreparedEntry()
	prepareRequestInfo := preparedEntry.GetPrepareRequestInfo()
	fwdDecision := castedRequestInfo.GetForwardDecision()
	preparedData := preparedEntry.GetPreparedData()
	replacedTerms := prepareRequestInfo.GetReplacedTerms()

	var replacementTimeUuids []*uuid.UUID
	if fwdDecision == forwardToBoth || fwdDecision == forwardToOrigin || (sendToAsyncConnector && asyncConnectorIsOrigin) {
		if fwdDecision == forwardToTarget {
			if preparedData.GetOriginPreparedId() == nil {
				log.Debugf("Discarding async read because prepared response hasn't arrived yet: %s", prepareRequestInfo.GetQuery())
				return nil, replacementTimeUuids, nil
			}
		}
		clientRequest, err := frameContext.GetOrDecodeFrame()
		if err != nil {
			return nil, replacementTimeUuids, fmt.Errorf("could not decode execute raw frame: %w", err)
		}

		newOriginRequest := clientRequest.Clone()
		var newOriginExecuteMsg *message.Execute
		if len(replacedTerms) > 0 {
			replacementTimeUuids = ch.parameterModifier.generateTimeUuids(prepareRequestInfo)
			newOriginExecuteMsg, err = ch.parameterModifier.AddValuesToExecuteFrame(
				newOriginRequest, prepareRequestInfo, preparedData.GetOriginVariablesMetadata(), replacementTimeUuids)
			if err != nil {
				return nil, replacementTimeUuids, fmt.Errorf("could not add values to origin EXECUTE: %w", err)
			}
		} else {
			var ok bool
			newOriginExecuteMsg, ok = newOriginRequest.Body.Message.(*message.Execute)
			if !ok {
				return nil, replacementTimeUuids, fmt.Errorf("expected Execute but got %v instead",
					newOriginRequest.Body.Message.GetOpCode())
			}
		}

		originalQueryId := newOriginExecuteMsg.QueryId
		newOriginExecuteMsg.QueryId = preparedData.GetOriginPreparedId()
		log.Tracef("Replacing prepared ID %s with %s for origin cluster.",
			hex.EncodeToString(originalQueryId), hex.EncodeToString(newOriginExecuteMsg.QueryId))

		originExecuteRequestRaw, err := defaultCodec.ConvertToRawFrame(newOriginRequest)
		if err != nil {
			return nil, replacementTimeUuids, fmt.Errorf("could not convert origin EXECUTE response to raw frame: %w", err)
		}

		return originExecuteRequestRaw, replacementTimeUuids, nil
	}
	return frameContext.GetRawFrame(), replacementTimeUuids, nil
}

func (ch *ClientHandler) rewriteTargetPrepare(castedRequestInfo *ExecuteRequestInfo, frameContext *frameDecodeContext,
	replacementTimeUuids []*uuid.UUID, sendToAsyncConnector bool, asyncConnectorIsTarget bool) (*frame.RawFrame, error) {
	preparedEntry := castedRequestInfo.GetPreparedEntry()
	prepareRequestInfo := preparedEntry.GetPrepareRequestInfo()
	fwdDecision := castedRequestInfo.GetForwardDecision()
	preparedData := preparedEntry.GetPreparedData()
	replacedTerms := prepareRequestInfo.GetReplacedTerms()
	if fwdDecision == forwardToBoth || fwdDecision == forwardToTarget || (sendToAsyncConnector && asyncConnectorIsTarget) {
		if fwdDecision == forwardToOrigin {
			if preparedData.GetTargetPreparedId() == nil {
				log.Debugf("Discarding async read because prepared response hasn't arrived yet: %s", prepareRequestInfo.GetQuery())
				return nil, nil
			}
		}
		clientRequest, err := frameContext.GetOrDecodeFrame()
		if err != nil {
			return nil, fmt.Errorf("could not decode execute raw frame: %w", err)
		}

		newTargetRequest := clientRequest.Clone()
		var newTargetExecuteMsg *message.Execute
		if len(replacedTerms) > 0 {
			if replacementTimeUuids == nil {
				replacementTimeUuids = ch.parameterModifier.generateTimeUuids(prepareRequestInfo)
			}
			newTargetExecuteMsg, err = ch.parameterModifier.AddValuesToExecuteFrame(
				newTargetRequest, prepareRequestInfo, preparedData.GetTargetVariablesMetadata(), replacementTimeUuids)
			if err != nil {
				return nil, fmt.Errorf("could not add values to target EXECUTE: %w", err)
			}
		} else {
			var ok bool
			newTargetExecuteMsg, ok = newTargetRequest.Body.Message.(*message.Execute)
			if !ok {
				return nil, fmt.Errorf("expected Execute but got %v instead", newTargetRequest.Body.Message.GetOpCode())
			}
		}

		originalQueryId := newTargetExecuteMsg.QueryId
		newTargetExecuteMsg.QueryId = preparedData.GetTargetPreparedId()
		log.Tracef("Replacing prepared ID %s with %s for target cluster.",
			hex.EncodeToString(originalQueryId), hex.EncodeToString(newTargetExecuteMsg.QueryId))

		newTargetRequestRaw, err := defaultCodec.ConvertToRawFrame(newTargetRequest)
		if err != nil {
			return nil, fmt.Errorf("could not convert target EXECUTE response to raw frame: %w", err)
		}

		return newTargetRequestRaw, nil
	}
	return frameContext.GetRawFrame(), nil
}

func (ch *ClientHandler) handleBatchRequest(
	castedRequestInfo *BatchRequestInfo, frameContext *frameDecodeContext) (
	originRequest *frame.RawFrame, targetRequest *frame.RawFrame, err error) {
	f := frameContext.GetRawFrame()
	originRequest = f
	targetRequest = f
	decodedFrame, err := frameContext.GetOrDecodeFrame()
	if err != nil {
		return nil, nil, fmt.Errorf("could not decode batch raw frame: %w", err)
	}

	newTargetRequest := decodedFrame.Clone()
	newTargetBatchMsg, ok := newTargetRequest.Body.Message.(*message.Batch)
	if !ok {
		return nil, nil, fmt.Errorf("expected Batch but got %v instead", newTargetRequest.Body.Message.GetOpCode())
	}
	newOriginRequest := decodedFrame.Clone()
	newOriginBatchMsg, ok := newOriginRequest.Body.Message.(*message.Batch)
	if !ok {
		return nil, nil, fmt.Errorf("expected Batch but got %v instead", newOriginRequest.Body.Message.GetOpCode())
	}

	for stmtIdx, preparedEntry := range castedRequestInfo.GetPreparedDataByStmtIdx() {
		prepareRequestInfo := preparedEntry.GetPrepareRequestInfo()
		preparedData := preparedEntry.GetPreparedData()
		if len(prepareRequestInfo.GetReplacedTerms()) > 0 {
			replacementTimeUuids := ch.parameterModifier.generateTimeUuids(prepareRequestInfo)
			err = ch.parameterModifier.addValuesToBatchChild(decodedFrame.Header.Version, newTargetBatchMsg.Children[stmtIdx],
				preparedEntry.GetPrepareRequestInfo(), preparedData.GetTargetVariablesMetadata(), replacementTimeUuids)
			if err == nil && newOriginBatchMsg != nil {
				err = ch.parameterModifier.addValuesToBatchChild(decodedFrame.Header.Version, newOriginBatchMsg.Children[stmtIdx],
					preparedEntry.GetPrepareRequestInfo(), preparedData.GetOriginVariablesMetadata(), replacementTimeUuids)
			}
			if err != nil {
				return nil, nil, fmt.Errorf("could not add values to batch child statement: %w", err)
			}
		}

		originalQueryId := newTargetBatchMsg.Children[stmtIdx].QueryOrId.([]byte)
		newTargetBatchMsg.Children[stmtIdx].QueryOrId = preparedData.GetTargetPreparedId()
		log.Tracef("Replacing prepared ID %s within a BATCH with %s for target cluster.",
			hex.EncodeToString(originalQueryId), hex.EncodeToString(preparedData.GetTargetPreparedId()))
		newOriginBatchMsg.Children[stmtIdx].QueryOrId = preparedData.GetOriginPreparedId()
		log.Tracef("Replacing prepared ID %s within a BATCH with %s for origin cluster.",
			hex.EncodeToString(originalQueryId), hex.EncodeToString(preparedData.GetOriginPreparedId()))
	}

	originBatchRequest, err := defaultCodec.ConvertToRawFrame(newOriginRequest)
	if err != nil {
		return nil, nil, fmt.Errorf("could not convert origin BATCH response to raw frame: %w", err)
	}

	originRequest = originBatchRequest

	targetBatchRequest, err := defaultCodec.ConvertToRawFrame(newTargetRequest)
	if err != nil {
		return nil, nil, fmt.Errorf("could not convert target BATCH response to raw frame: %w", err)
	}

	targetRequest = targetBatchRequest

	return originRequest, targetRequest, nil
}

func (ch *ClientHandler) sendToAsyncConnector(
	currentKeyspace string, frameContext *frameDecodeContext, originRequest *frame.RawFrame, targetRequest *frame.RawFrame,
	fwdDecision forwardDecision, reqCtx *requestContextImpl, holder *requestContextHolder, sendAlsoToAsync bool,
	overallRequestStartTime time.Time, requestTimeout time.Duration) error {
	var asyncRequest *frame.RawFrame
	if ch.primaryCluster == common.ClusterTypeTarget {
		asyncRequest = originRequest
	} else {
		asyncRequest = targetRequest
	}

	if asyncRequest == nil {
		return nil //discarded
	}

	// forwardToAsyncOnly requests are not fire and forget, i.e., client handler waits for the response
	isFireAndForget := fwdDecision != forwardToAsyncOnly

	if !ch.asyncConnector.validateAsyncStateForRequest(asyncRequest) {
		if !isFireAndForget {
			if reqCtx.Cancel(ch.nodeMetrics) {
				ch.cancelRequest(holder, reqCtx)
			}
		}
		return nil
	}

	if sendAlsoToAsync {
		asyncRequest = asyncRequest.Clone() // forwardToAsyncOnly requests don't need to be cloned because they are only sent to 1 connector
	}

	if isFireAndForget {
		ch.clientHandlerRequestWaitGroup.Add(1)
	}

	f := frameContext.GetRawFrame()

	sent := ch.asyncConnector.sendAsyncRequestToCluster(
		reqCtx.GetRequestInfo(), asyncRequest, !isFireAndForget, overallRequestStartTime, requestTimeout, func() {
			if !isFireAndForget {
				ch.closedRespChannelLock.RLock()
				defer ch.closedRespChannelLock.RUnlock()
				if ch.closedRespChannel {
					finished := reqCtx.SetTimeout(ch.nodeMetrics, f)
					if finished {
						ch.finishRequest(currentKeyspace, holder, reqCtx)
					}
				} else {
					ch.respChannel <- NewTimeoutResponse(f, true)
				}
			} else {
				ch.clientHandlerRequestWaitGroup.Done()
			}
		})

	if !sent {
		if !isFireAndForget {
			if reqCtx.Cancel(ch.nodeMetrics) {
				ch.cancelRequest(holder, reqCtx)
			}
		} else {
			ch.clientHandlerRequestWaitGroup.Done()
		}
	}

	return nil
}

// Aggregates the responses received from the two clusters as follows:
//   - if both responses are a success OR both responses are a failure: return responseFromOC
//   - if either response is a failure, the failure "wins": return the failed response
//
// Also updates metrics appropriately.
func (ch *ClientHandler) aggregateAndTrackResponses(
	requestInfo RequestInfo,
	request *frame.RawFrame,
	responseFromOriginCassandra *frame.RawFrame,
	responseFromTargetCassandra *frame.RawFrame) (*frame.RawFrame, common.ClusterType) {

	originOpCode := responseFromOriginCassandra.Header.OpCode
	log.Tracef("Aggregating responses. %v opcode %d, %v opcode %d",
		common.ClusterTypeOrigin, originOpCode, common.ClusterTypeTarget, responseFromTargetCassandra.Header.OpCode)

	// aggregate responses and update relevant aggregate metrics for general failed or successful responses
	if isResponseSuccessful(responseFromOriginCassandra) && isResponseSuccessful(responseFromTargetCassandra) {
		if originOpCode == primitive.OpCodeSupported {
			log.Tracef("Aggregated response: both successes, sending back %v response with opcode %d",
				common.ClusterTypeTarget, originOpCode)
			return responseFromTargetCassandra, common.ClusterTypeTarget
		} else {
			if ch.primaryCluster == common.ClusterTypeTarget {
				log.Tracef("Aggregated response: both successes, sending back %v response with opcode %d",
					common.ClusterTypeTarget, responseFromTargetCassandra.Header.OpCode)
				return responseFromTargetCassandra, common.ClusterTypeTarget
			} else {
				log.Tracef("Aggregated response: both successes, sending back %v response with opcode %d",
					common.ClusterTypeOrigin, originOpCode)
				return responseFromOriginCassandra, common.ClusterTypeOrigin
			}
		}
	}

	proxyMetrics := ch.metricHandler.GetProxyMetrics()
	if !isResponseSuccessful(responseFromOriginCassandra) && !isResponseSuccessful(responseFromTargetCassandra) {
		log.Debugf("Aggregated response: both failures, sending back %v response with opcode %d",
			common.ClusterTypeOrigin, originOpCode)
		if requestInfo.ShouldBeTrackedInMetrics() {
			proxyMetrics.FailedWritesOnBoth.Add(1)
		}
		return responseFromOriginCassandra, common.ClusterTypeOrigin
	}

	// if either response is a failure, the failure "wins" --> return the failed response
	if !isResponseSuccessful(responseFromOriginCassandra) {
		log.Debugf("Aggregated response: failure only on %v, sending back %v response with opcode %d",
			common.ClusterTypeOrigin, common.ClusterTypeOrigin, originOpCode)
		if requestInfo.ShouldBeTrackedInMetrics() {
			proxyMetrics.FailedWritesOnOrigin.Add(1)
		}
		return responseFromOriginCassandra, common.ClusterTypeOrigin
	} else {
		log.Debugf("Aggregated response: failure only on %v, sending back %v response with opcode %d",
			common.ClusterTypeTarget, common.ClusterTypeTarget, originOpCode)
		if requestInfo.ShouldBeTrackedInMetrics() {
			proxyMetrics.FailedWritesOnTarget.Add(1)
		}
		return responseFromTargetCassandra, common.ClusterTypeTarget
	}
}

// Replaces the credentials in the provided auth frame (which are the Target credentials) with
// the Origin credentials that are provided to the proxy in the configuration.
func (ch *ClientHandler) handleClientCredentials(f *frame.RawFrame) (*frame.RawFrame, error) {
	parsedAuthFrame, err := defaultCodec.ConvertFromRawFrame(f)
	if err != nil {
		return nil, fmt.Errorf("could not extract auth credentials from frame to start the secondary handshake: %w", err)
	}

	authResponse, ok := parsedAuthFrame.Body.Message.(*message.AuthResponse)
	if !ok {
		return nil, fmt.Errorf(
			"expected AuthResponse but got %v, can not proceed with secondary handshake",
			parsedAuthFrame.Body.Message)
	}

	clientCreds, err := ParseCredentialsFromRequest(authResponse.Token)
	if err != nil {
		return nil, err
	}

	if clientCreds == nil {
		log.Debugf("Found auth response frame without creds: %v", authResponse)
		return f, nil
	}

	log.Debugf("Successfully extracted credentials from client auth frame: %v", clientCreds)

	var primaryHandshakeCreds *AuthCredentials
	if ch.forwardAuthToTarget {
		// primary handshake is TARGET, secondary is ORIGIN

		if ch.targetCredsOnClientRequest {
			ch.secondaryHandshakeCreds = &AuthCredentials{
				Username: ch.originUsername,
				Password: ch.originPassword,
			}
		} else {
			// unreachable code atm, if forwardAuthToTarget is true then targetCredsOnClientRequest is true
			ch.secondaryHandshakeCreds = clientCreds
			primaryHandshakeCreds = &AuthCredentials{
				Username: ch.targetUsername,
				Password: ch.targetPassword,
			}
		}
	} else {
		// primary handshake is ORIGIN, secondary is TARGET

		if ch.targetCredsOnClientRequest {
			ch.secondaryHandshakeCreds = clientCreds
			primaryHandshakeCreds = &AuthCredentials{
				Username: ch.originUsername,
				Password: ch.originPassword,
			}
		} else {
			ch.secondaryHandshakeCreds = &AuthCredentials{
				Username: ch.targetUsername,
				Password: ch.targetPassword,
			}
		}
	}

	ch.asyncHandshakeCreds = clientCreds
	if ch.asyncConnector != nil {
		if ch.targetCredsOnClientRequest && ch.asyncConnector.clusterType == common.ClusterTypeOrigin {
			ch.asyncHandshakeCreds = &AuthCredentials{
				Username: ch.originUsername,
				Password: ch.originPassword,
			}
		}
		if !ch.targetCredsOnClientRequest && ch.asyncConnector.clusterType == common.ClusterTypeTarget {
			ch.asyncHandshakeCreds = &AuthCredentials{
				Username: ch.targetUsername,
				Password: ch.targetPassword,
			}
		}
	}

	if primaryHandshakeCreds == nil {
		// client credentials don't need to be replaced
		return f, nil
	}

	authResponse.Token = primaryHandshakeCreds.Marshal()

	f, err = defaultCodec.ConvertToRawFrame(parsedAuthFrame)
	if err != nil {
		return nil, fmt.Errorf("could not convert new auth response to a raw frame, can not proceed with secondary handshake: %w", err)
	}

	return f, nil
}

func (ch *ClientHandler) LoadCurrentKeyspace() string {
	ks := ch.currentKeyspaceName.Load()
	if ks != nil {
		return ks.(string)
	} else {
		return ""
	}
}

func (ch *ClientHandler) StoreCurrentKeyspace(keyspace string) {
	ch.currentKeyspaceName.Store(keyspace)
}

func decodeErrorResult(frame *frame.RawFrame) (message.Error, error) {
	body, err := defaultCodec.DecodeBody(frame.Header, bytes.NewReader(frame.Body))
	if err != nil {
		return nil, fmt.Errorf("could not decode error body: %w", err)
	}

	errorResult, ok := body.Message.(message.Error)
	if !ok {
		return nil, fmt.Errorf("expected error body but got %T", body.Message)
	}

	return errorResult, nil
}

func isResponseSuccessful(response *frame.RawFrame) bool {
	return response.Header.OpCode != primitive.OpCodeError
}

func createUnpreparedFrame(errVal *UnpreparedExecuteError) (*frame.RawFrame, error) {
	unpreparedMsg := &message.Unprepared{
		ErrorMessage: fmt.Sprintf("Prepared query with ID %s not found (either the query was not prepared "+
			"on this host (maybe the host has been restarted?) or you have prepared too many queries and it has "+
			"been evicted from the internal cache)", hex.EncodeToString(errVal.preparedId)),
		Id: errVal.preparedId,
	}
	f := frame.NewFrame(errVal.Header.Version, errVal.Header.StreamId, unpreparedMsg)
	f.Body.TracingId = errVal.Body.TracingId

	rawFrame, err := defaultCodec.ConvertToRawFrame(f)
	if err != nil {
		return nil, fmt.Errorf("could not convert unprepared response frame to rawframe: %w", err)
	}

	return rawFrame, nil
}

// Creates request context holder for the provided request context and adds it to the map.
// If a holder already exists, return it instead.
func getOrCreateRequestContextHolder(contextHoldersMap *sync.Map, streamId int16) *requestContextHolder {
	reqCtxHolder, ok := contextHoldersMap.Load(streamId)
	if ok {
		return reqCtxHolder.(*requestContextHolder)
	} else {
		reqCtxHolder, _ := contextHoldersMap.LoadOrStore(streamId, NewRequestContextHolder())
		return reqCtxHolder.(*requestContextHolder)
	}
}

// Stores the provided request context in a RequestContextHolder. The holder is retrieved using getOrCreateRequestContextHolder,
// see the documentation on that function for more details.
func storeRequestContext(contextHoldersMap *sync.Map, reqCtx *requestContextImpl) (*requestContextHolder, error) {
	requestContextHolder := getOrCreateRequestContextHolder(contextHoldersMap, reqCtx.request.Header.StreamId)
	if requestContextHolder == nil {
		return nil, fmt.Errorf("could not find request context holder with stream id %d", reqCtx.request.Header.StreamId)
	}

	err := requestContextHolder.SetIfEmpty(reqCtx)
	if err != nil {
		return nil, fmt.Errorf("stream id collision (%d)", reqCtx.request.Header.StreamId)
	}

	return requestContextHolder, nil
}

// Updates cluster level error metrics based on the outcome in the response
func trackClusterErrorMetrics(
	response *frame.RawFrame,
	connectorType ClusterConnectorType,
	nodeMetrics *metrics.NodeMetrics) {
	if !isResponseSuccessful(response) {
		errorMsg, err := decodeErrorResult(response)
		if err != nil {
			log.Errorf("could not track read response: %v", err)
			return
		}

		trackClusterErrorMetricsFromErrorMessage(errorMsg, connectorType, nodeMetrics)
	}
}

func trackClusterErrorMetricsFromErrorMessage(
	errorMsg message.Error,
	connectorType ClusterConnectorType,
	nodeMetrics *metrics.NodeMetrics) {

	nodeMetricsInstance, err := GetNodeMetricsByClusterConnector(nodeMetrics, connectorType)
	if err != nil {
		log.Errorf("Failed to track cluster error metrics: %v.", err)
		return
	}

	switch errorMsg.GetErrorCode() {
	case primitive.ErrorCodeUnprepared:
		nodeMetricsInstance.UnpreparedErrors.Add(1)
	case primitive.ErrorCodeReadTimeout:
		nodeMetricsInstance.ReadTimeouts.Add(1)
	case primitive.ErrorCodeWriteTimeout:
		nodeMetricsInstance.WriteTimeouts.Add(1)
	case primitive.ErrorCodeOverloaded:
		nodeMetricsInstance.OverloadedErrors.Add(1)
	case primitive.ErrorCodeReadFailure:
		nodeMetricsInstance.ReadFailures.Add(1)
	case primitive.ErrorCodeWriteFailure:
		nodeMetricsInstance.WriteFailures.Add(1)
	case primitive.ErrorCodeUnavailable:
		nodeMetricsInstance.UnavailableErrors.Add(1)
	default:
		log.Debugf("Recording %v other error: %v", connectorType, errorMsg)
		nodeMetricsInstance.OtherErrors.Add(1)
	}
}

func forwardAuthToTarget(
	originControlConn *ControlConn,
	targetControlConn *ControlConn,
	forwardClientCredsToOrigin bool) (forwardAuthToTarget bool, targetCredsOnClientRequest bool) {
	authEnabledOnOrigin, err := originControlConn.IsAuthEnabled()
	clusterType := common.ClusterTypeOrigin
	var authEnabledOnTarget bool
	if err == nil {
		authEnabledOnTarget, err = targetControlConn.IsAuthEnabled()
		clusterType = common.ClusterTypeTarget
	}

	if err != nil {
		log.Errorf("Error detected while checking if auth is enabled on %v to figure out which cluster should "+
			"receive the auth credentials from the client. Falling back to sending auth to %v and assuming "+
			"that client credentials are meant for %v. "+
			"This is a bug, please report: %v", clusterType, common.ClusterTypeOrigin, common.ClusterTypeTarget, err)
		return false, true
	}

	// only use forwardClientCredsToOrigin setting if we need creds for both,
	// otherwise just forward the client creds to the only cluster that asked for them
	if !authEnabledOnOrigin && authEnabledOnTarget {
		return true, true
	} else if authEnabledOnOrigin && !authEnabledOnTarget {
		return false, false
	} else {
		return false, !forwardClientCredsToOrigin
	}
}

// checkUnsupportedProtocolError handles the case where the protocol library throws an error while decoding the version (maybe the client tries to use v1 or v6)
func checkUnsupportedProtocolError(err error) *message.ProtocolError {
	protocolVersionErr := &frame.ProtocolVersionErr{}
	if errors.As(err, &protocolVersionErr) {
		var protocolErrMsg *message.ProtocolError
		if protocolVersionErr.Version.IsBeta() && !protocolVersionErr.UseBeta {
			protocolErrMsg = &message.ProtocolError{
				ErrorMessage: fmt.Sprintf("Beta version of the protocol used (%d/v%d-beta), but USE_BETA flag is unset",
					protocolVersionErr.Version, protocolVersionErr.Version)}
		} else {
			protocolErrMsg = &message.ProtocolError{
				ErrorMessage: fmt.Sprintf("Invalid or unsupported protocol version (%d)", protocolVersionErr.Version)}
		}

		return protocolErrMsg
	}

	return nil
}

// checkProtocolVersion handles the case where the protocol library does not return an error but the proxy does not support a specific version
func checkProtocolVersion(version primitive.ProtocolVersion) *message.ProtocolError {
	if version < primitive.ProtocolVersion5 || version.IsDse() {
		return nil
	}

	protocolErrMsg := &message.ProtocolError{
		ErrorMessage: fmt.Sprintf("Invalid or unsupported protocol version (%d)", version)}

	return protocolErrMsg
}

type customResponse struct {
	originResponse     *frame.RawFrame
	targetResponse     *frame.RawFrame
	aggregatedResponse *frame.RawFrame
}

type protocolEventObserverImpl struct {
	cancelFn       context.CancelFunc
	connectionHost *Host
}

func NewProtocolEventObserver(cancelFunc context.CancelFunc, host *Host) *protocolEventObserverImpl {
	return &protocolEventObserverImpl{
		cancelFn:       cancelFunc,
		connectionHost: host,
	}
}

func (recv *protocolEventObserverImpl) OnHostRemoved(host *Host) {
	if recv.connectionHost.HostId == host.HostId {
		log.Infof("Host used in connection was removed, closing connection: %v", host)
		recv.cancelFn()
	}
}

func (recv *protocolEventObserverImpl) GetHost() *Host {
	return recv.connectionHost
}

func GetNodeMetricsByClusterConnector(nodeMetrics *metrics.NodeMetrics, connectorType ClusterConnectorType) (*metrics.NodeMetricsInstance, error) {
	switch connectorType {
	case ClusterConnectorTypeOrigin:
		return nodeMetrics.OriginMetrics, nil
	case ClusterConnectorTypeTarget:
		return nodeMetrics.TargetMetrics, nil
	case ClusterConnectorTypeAsync:
		return nodeMetrics.AsyncMetrics, nil
	default:
		return nil, fmt.Errorf("unexpected connectorType %v, unable to retrieve node metrics", connectorType)
	}
}

func newFrameProcessor(conf *config.Config, nodeMetrics *metrics.NodeMetrics, connectorType ClusterConnectorType) FrameProcessor {
	var streamIdsMetric metrics.Gauge
	connectorMetrics, err := GetNodeMetricsByClusterConnector(nodeMetrics, connectorType)
	if err != nil {
		log.Error(err)
	}
	if connectorMetrics != nil {
		streamIdsMetric = connectorMetrics.UsedStreamIds
	}
	var mapper StreamIdMapper
	if connectorType == ClusterConnectorTypeAsync {
		mapper = NewInternalStreamIdMapper(conf.ProxyMaxStreamIds, streamIdsMetric)
	} else {
		mapper = NewStreamIdMapper(conf.ProxyMaxStreamIds, streamIdsMetric)
	}
	return NewStreamIdProcessor(mapper)
}
