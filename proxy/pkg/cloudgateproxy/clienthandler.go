package cloudgateproxy

import (
	"bytes"
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
	"net"
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

	originControlConn *ControlConn
	targetControlConn *ControlConn

	typeCodecManager TypeCodecManager

	preparedStatementCache *PreparedStatementCache

	metricHandler          *metrics.MetricHandler
	nodeMetrics            *metrics.NodeMetrics

	clientHandlerContext    context.Context
	clientHandlerCancelFunc context.CancelFunc

	currentKeyspaceName *atomic.Value

	authErrorMessage *message.AuthenticationError

	startupFrame          *frame.RawFrame
	targetStartupResponse *frame.RawFrame
	targetCreds           *AuthCredentials

	originUsername string
	originPassword string

	// map of request context holders that store the contexts for the active requests, keyed on streamID
	requestContextHolders *sync.Map

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
	topologyConfig *config.TopologyConfig

	localClientHandlerWg *sync.WaitGroup

	originHost *Host
	targetHost *Host

	originObserver *protocolEventObserverImpl
	targetObserver *protocolEventObserverImpl
}

func NewClientHandler(
	clientTcpConn net.Conn,
	originCassandraConnInfo *ClusterConnectionInfo,
	targetCassandraConnInfo *ClusterConnectionInfo,
	typeCodecManager TypeCodecManager,
	originControlConn *ControlConn,
	targetControlConn *ControlConn,
	conf *config.Config,
	topologyConfig *config.TopologyConfig,
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
	targetHost *Host) (*ClientHandler, error) {

	nodeMetrics, err := metricHandler.GetNodeMetrics(
		originCassandraConnInfo.endpoint.GetEndpointIdentifier(),
		targetCassandraConnInfo.endpoint.GetEndpointIdentifier())
	if err != nil {
		return nil, fmt.Errorf("failed to create node metrics: %w", err)
	}

	clientHandlerContext, clientHandlerCancelFunc := context.WithCancel(context.Background())
	clientHandlerShutdownRequestContext, clientHandlerShutdownRequestCancelFn := context.WithCancel(globalShutdownRequestCtx)
	requestsDoneCtx, requestsDoneCancelFn := context.WithCancel(context.Background())

	localClientHandlerWg := &sync.WaitGroup{}
	globalClientHandlersWg.Add(1)
	go func() {
		defer globalClientHandlersWg.Done()
		<-clientHandlerContext.Done()
		clientHandlerShutdownRequestCancelFn()
		requestsDoneCancelFn()
		localClientHandlerWg.Wait()
		log.Debugf("Client Handler is shutdown.")
	}()

	respChannel := make(chan *Response, numWorkers)

	originConnector, err := NewClusterConnector(
		originCassandraConnInfo, conf, nodeMetrics, localClientHandlerWg,
		clientHandlerContext, clientHandlerCancelFunc, respChannel, readScheduler, writeScheduler, requestsDoneCtx)
	if err != nil {
		clientHandlerCancelFunc()
		return nil, err
	}

	targetConnector, err := NewClusterConnector(
		targetCassandraConnInfo, conf, nodeMetrics, localClientHandlerWg,
		clientHandlerContext, clientHandlerCancelFunc, respChannel, readScheduler, writeScheduler, requestsDoneCtx)
	if err != nil {
		clientHandlerCancelFunc()
		return nil, err
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
			clientHandlerShutdownRequestContext),

		originCassandraConnector:      originConnector,
		targetCassandraConnector:      targetConnector,
		originControlConn:             originControlConn,
		targetControlConn:             targetControlConn,
		typeCodecManager:              typeCodecManager,
		preparedStatementCache:        psCache,
		metricHandler:                 metricHandler,
		nodeMetrics:                   nodeMetrics,
		clientHandlerContext:          clientHandlerContext,
		clientHandlerCancelFunc:       clientHandlerCancelFunc,
		currentKeyspaceName:           &atomic.Value{},
		authErrorMessage:              nil,
		startupFrame:                  nil,
		originUsername:                originUsername,
		originPassword:                originPassword,
		requestContextHolders:         &sync.Map{},
		reqChannel:                    requestsChannel,
		respChannel:                   respChannel,
		clientHandlerRequestWaitGroup: &sync.WaitGroup{},
		closedRespChannel:             false,
		closedRespChannelLock:         &sync.RWMutex{},
		responsesDoneChan:             responsesDoneChan,
		eventsDoneChan:                eventsDoneChan,
		requestsDoneCancelFn:          requestsDoneCancelFn,
		requestResponseScheduler:      requestResponseScheduler,
		conf:                          conf,
		localClientHandlerWg:          localClientHandlerWg,
		topologyConfig:                topologyConfig,
		originHost:                    originHost,
		targetHost:                    targetHost,
		originObserver:                originObserver,
		targetObserver:                targetObserver,
	}, nil
}

/**
 *	Initialises all components and launches all listening loops that they have.
 */
func (ch *ClientHandler) run(activeClients *int32) {
	ch.clientConnector.run(activeClients)
	ch.originCassandraConnector.run()
	ch.targetCassandraConnector.run()
	ch.requestLoop()
	ch.listenForEventMessages()
	ch.responseLoop()

	addObserver(ch.originObserver, ch.originControlConn)
	addObserver(ch.targetObserver, ch.targetControlConn)

	go func() {
		<- ch.originCassandraConnector.doneChan
		<- ch.targetCassandraConnector.doneChan
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

		wg := &sync.WaitGroup{}
		for {
			f, ok := <-ch.reqChannel
			if !ok {
				break
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
			ch.requestContextHolders.Range(func(key, value interface{}) bool {
				reqCtxHolder := value.(*requestContextHolder)
				reqCtx := reqCtxHolder.Get()
				if reqCtx == nil {
					return true
				}
				canceled := reqCtx.Cancel()
				if canceled {
					ch.cancelRequest(reqCtxHolder, reqCtx)
				}
				return true
			})
		}()

		log.Debugf("Waiting for all in flight requests from %v to finish.", connectionAddr)
		ch.clientHandlerRequestWaitGroup.Wait()
	}()
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

		for {
			response, ok := <- ch.respChannel
			if !ok {
				break
			}

			wg.Add(1)
			ch.requestResponseScheduler.Schedule(func() {
				defer wg.Done()

				if ch.tryProcessProtocolError(response) {
					return
				}

				streamId := response.GetStreamId()
				holder := ch.getOrCreateRequestContextHolder(streamId)
				reqCtx := holder.Get()
				if reqCtx == nil {
					log.Warnf("Could not find request context for stream id %d on %v. " +
						"It either timed out or a protocol error occurred.", streamId, response.cluster)
					return
				}

				finished := false
				if response.responseFrame == nil {
					finished = reqCtx.SetTimeout(ch.nodeMetrics, response.requestFrame)
				} else {
					finished = reqCtx.SetResponse(ch.nodeMetrics, response.responseFrame, response.cluster)
					ch.trackClusterErrorMetrics(response.responseFrame, response.cluster)
				}

				if finished {
					ch.finishRequest(holder, reqCtx)
				}
			})
		}

		log.Debugf("Shutting down responseLoop.")
	}()
}

// Checks if response is a protocol error. Returns true if it processes this response. If it returns false,
// then the response wasn't processed and it should be processed by another function.
func (ch *ClientHandler) tryProcessProtocolError(response *Response) bool {
	if response.responseFrame != nil &&
		response.responseFrame.Header.OpCode == primitive.OpCodeError {
		body, err := defaultCodec.DecodeBody(
			response.responseFrame.Header, bytes.NewReader(response.responseFrame.Body))

		if err != nil {
			log.Errorf("Could not parse error with stream id 0 on %v: %v, skipping it.",
				response.cluster, response.responseFrame.Header)
			return true
		} else {
			protocolError, ok := body.Message.(*message.ProtocolError)
			if ok {
				log.Infof("Protocol error detected (%v) on %v, forwarding it to the client.",
					protocolError, response.cluster)
				ch.clientConnector.sendResponseToClient(response.responseFrame)
				return true
			}
		}
	}

	return false
}

// should only be called after SetTimeout or SetResponse returns true
func (ch *ClientHandler) finishRequest(holder *requestContextHolder, reqCtx *RequestContext) {
	defer ch.clientHandlerRequestWaitGroup.Done()
	err := holder.Clear(reqCtx)
	if err != nil {
		log.Debugf("Could not free stream id: %v", err)
	}

	proxyMetrics := ch.metricHandler.GetProxyMetrics()
	switch reqCtx.stmtInfo.GetForwardDecision() {
	case forwardToBoth:
		proxyMetrics.ProxyRequestDurationBoth.Track(reqCtx.startTime)
		proxyMetrics.InFlightRequestsBoth.Subtract(1)
	case forwardToOrigin:
		proxyMetrics.ProxyRequestDurationOrigin.Track(reqCtx.startTime)
		proxyMetrics.InFlightRequestsOrigin.Subtract(1)
	case forwardToTarget:
		proxyMetrics.ProxyRequestDurationTarget.Track(reqCtx.startTime)
		proxyMetrics.InFlightRequestsTarget.Subtract(1)
	default:
		log.Errorf("unexpected forwardDecision %v, unable to track proxy level metrics", reqCtx.stmtInfo.GetForwardDecision())
	}

	aggregatedResponse, err := ch.computeAggregatedResponse(reqCtx)
	finalResponse := aggregatedResponse
	if err == nil {
		finalResponse, err = ch.processAggregatedResponse(aggregatedResponse, reqCtx)
	}

	if err != nil {
		if reqCtx.customResponseChannel != nil {
			close(reqCtx.customResponseChannel)
		}
		log.Debugf("Error handling request (%v): %v", reqCtx.request.Header, err)
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
func (ch *ClientHandler) cancelRequest(holder *requestContextHolder, reqCtx *RequestContext) {
	defer ch.clientHandlerRequestWaitGroup.Done()
	err := holder.Clear(reqCtx)
	if err != nil {
		log.Debugf("Could not free stream id: %v", err)
	}

	proxyMetrics := ch.metricHandler.GetProxyMetrics()
	switch reqCtx.stmtInfo.GetForwardDecision() {
	case forwardToBoth:
		proxyMetrics.InFlightRequestsBoth.Subtract(1)
	case forwardToOrigin:
		proxyMetrics.InFlightRequestsOrigin.Subtract(1)
	case forwardToTarget:
		proxyMetrics.InFlightRequestsTarget.Subtract(1)
	default:
		log.Errorf("unexpected forwardDecision %v, unable to track proxy level metrics", reqCtx.stmtInfo.GetForwardDecision())
	}

	if reqCtx.customResponseChannel != nil {
		close(reqCtx.customResponseChannel)
	}

	log.Tracef("Canceled request %v.", reqCtx.request.Header)
}

// Computes the response to be sent to the client based on the forward decision of the request.
func (ch *ClientHandler) computeAggregatedResponse(requestContext *RequestContext) (*frame.RawFrame, error) {
	forwardDecision := requestContext.stmtInfo.GetForwardDecision()
	if forwardDecision == forwardToOrigin {
		if requestContext.originResponse == nil {
			return nil, fmt.Errorf("did not receive response from origin cassandra channel, stream: %d", requestContext.request.Header.StreamId)
		}
		log.Tracef("Forward to origin: just returning the response received from OC: %d", requestContext.originResponse.Header.OpCode)
		if !isResponseSuccessful(requestContext.originResponse) {
			ch.metricHandler.GetProxyMetrics().FailedRequestsOrigin.Add(1)
		}
		return requestContext.originResponse, nil

	} else if forwardDecision == forwardToTarget {
		if requestContext.targetResponse == nil {
			return nil, fmt.Errorf("did not receive response from target cassandra channel, stream: %d", requestContext.request.Header.StreamId)
		}
		log.Tracef("Forward to target: just returning the response received from TC: %d", requestContext.targetResponse.Header.OpCode)
		if !isResponseSuccessful(requestContext.targetResponse) {
			ch.metricHandler.GetProxyMetrics().FailedRequestsTarget.Add(1)
		}
		return requestContext.targetResponse, nil

	} else if forwardDecision == forwardToBoth {
		if requestContext.originResponse == nil {
			return nil, fmt.Errorf("did not receive response from original cassandra channel, stream: %d", requestContext.request.Header.StreamId)
		}
		if requestContext.targetResponse == nil {
			return nil, fmt.Errorf("did not receive response from target cassandra channel, stream: %d", requestContext.request.Header.StreamId)
		}
		return ch.aggregateAndTrackResponses(requestContext.request, requestContext.originResponse, requestContext.targetResponse), nil

	} else {
		return nil, fmt.Errorf("unknown forward decision %v, request context: %v", forwardDecision, requestContext)
	}
}

// Modifies internal state based on the provided aggregated response (e.g. storing prepared IDs)
func (ch *ClientHandler) processAggregatedResponse(response *frame.RawFrame, reqCtx *RequestContext) (*frame.RawFrame, error) {
	switch response.Header.OpCode {
	case primitive.OpCodeResult, primitive.OpCodeError:
		decodedFrame, err := defaultCodec.ConvertFromRawFrame(response)
		if err != nil {
			return nil, fmt.Errorf("error decoding response: %w", err)
		}

		switch bodyMsg := decodedFrame.Body.Message.(type) {
		case *message.PreparedResult:
			err = ch.processPreparedResponse(bodyMsg, reqCtx)
			if err != nil {
				return nil, fmt.Errorf("failed to handle prepared result: %w", err)
			}
		case *message.SetKeyspaceResult:
			if bodyMsg.Keyspace == "" {
				log.Warnf("unexpected set keyspace empty")
			} else {
				ch.currentKeyspaceName.Store(bodyMsg.Keyspace)
			}
		case *message.Unprepared:
			executeBody, err := defaultCodec.DecodeBody(reqCtx.request.Header, bytes.NewReader(reqCtx.request.Body))
			if err != nil {
				return nil, fmt.Errorf("could not decode request body after receiving UNPREPARED: %w", err)
			}
			bs, ok := executeBody.Message.(*message.Execute)
			if !ok {
				return nil, fmt.Errorf("received UNPREPARED but request message is %v instead of EXECUTE", executeBody.Message)
			}
			newFrame := decodedFrame.Clone()
			newUnprepared := &message.Unprepared{
				ErrorMessage: fmt.Sprintf("Prepared query with ID %s not found (either the query was not prepared "+
					"on this host (maybe the host has been restarted?) or you have prepared too many queries and it has "+
					"been evicted from the internal cache)", hex.EncodeToString(bs.QueryId)),
				Id: bs.QueryId,
			}
			newFrame.Body.Message = newUnprepared
			newRawFrame, err := defaultCodec.ConvertToRawFrame(newFrame)
			if err != nil {
				return nil, fmt.Errorf("could not convert response after receiving UNPREPARED: %w", err)
			}
			log.Infof("Replacing prepared ID in UNPREPARED %s with %s. Original error: %v", hex.EncodeToString(bodyMsg.Id), hex.EncodeToString(bs.QueryId), bodyMsg.ErrorMessage)
			return newRawFrame, nil
		}
	}
	return response, nil
}

func (ch *ClientHandler) processPreparedResponse(bodyMsg *message.PreparedResult, reqCtx *RequestContext) error {
	if bodyMsg.PreparedQueryId == nil {
		return errors.New("unexpected prepared query id nil")
	} else if reqCtx.stmtInfo == nil {
		return errors.New("unexpected statement info nil on request context")
	} else if preparedStmtInfo, ok := reqCtx.stmtInfo.(*PreparedStatementInfo); !ok {
		return errors.New("unexpected request context statement info is not prepared statement info")
	} else if reqCtx.targetResponse == nil {
		return errors.New("unexpected target response nil")
	} else {
		targetBody, err := defaultCodec.DecodeBody(reqCtx.targetResponse.Header, bytes.NewReader(reqCtx.targetResponse.Body))
		if err != nil {
			return fmt.Errorf("error decoding target result response: %w", err)
		}

		targetPreparedResult, ok := targetBody.Message.(*message.PreparedResult)
		if !ok {
			return fmt.Errorf("expected PREPARED RESULT targetBody in target result response but got %T", targetBody.Message)
		}
		ch.preparedStatementCache.Store(bodyMsg.PreparedQueryId, targetPreparedResult.PreparedQueryId, preparedStmtInfo)
	}
	return nil
}

type handshakeRequestResult struct {
	authSuccess        bool
	err                error
	customResponseChan chan *customResponse
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
			scheduledTaskChannel <- &handshakeRequestResult{
				authSuccess: false,
				err:         ch.sendAuthErrorToClient(request),
			}
			return
		}

		if request.Header.OpCode == primitive.OpCodeAuthResponse {
			newAuthFrame, err := ch.replaceAuthFrame(request)
			if err != nil {
				scheduledTaskChannel <- &handshakeRequestResult{
					authSuccess: false,
					err:         err,
				}
				return
			}

			request = newAuthFrame
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
		if response.targetResponse == nil {
			return false, fmt.Errorf("no response received from Target for startup %v", request)
		}
		ch.targetStartupResponse = response.targetResponse
		ch.startupFrame = request

		_, _, _, err := handleTargetHandshakeResponse(
			1, response.targetResponse, ch.clientConnector.connection.RemoteAddr(), ch.targetCassandraConnector.connection.RemoteAddr())
		if err != nil {
			return false, fmt.Errorf("unsuccessful startup on Target: %w", err)
		}
		aggregatedResponse = response.originResponse
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
		if aggregatedResponse.Header.OpCode == primitive.OpCodeReady || aggregatedResponse.Header.OpCode == primitive.OpCodeAuthSuccess {
			// target handshake must happen within a single client request lifetime
			// to guarantee that no other request with the same
			// stream id goes to target in the meantime

			// if we add stream id mapping logic in the future, then
			// we can start the target handshake earlier and wait for it to end here

			targetAuthChannel, err := ch.startTargetHandshake()
			if err != nil {
				tempResult.err = err
				scheduledTaskChannel <- tempResult
				return
			}

			err, ok := <-targetAuthChannel
			if !ok {
				tempResult.err = errors.New("target handshake failed (scheduledTaskChannel closed)")
				scheduledTaskChannel <- tempResult
				return
			}

			if err != nil {
				var authError *AuthError
				if errors.As(err, &authError) {
					ch.authErrorMessage = authError.errMsg
					tempResult.err = ch.sendAuthErrorToClient(request)
					scheduledTaskChannel <- tempResult
					return
				}

				log.Errorf("handshake failed, shutting down the client handler and connectors: %s", err.Error())
				ch.clientHandlerCancelFunc()
				tempResult.err = fmt.Errorf("handshake failed: %w", ShutdownErr)
				scheduledTaskChannel <- tempResult
				return
			}

			tempResult.authSuccess = true
			scheduledTaskChannel <- tempResult
		}

		// send overall response back to client
		ch.clientConnector.sendResponseToClient(aggregatedResponse)
		scheduledTaskChannel <- tempResult
	})

	result, ok = <- scheduledTaskChannel
	if !ok {
		return false, errors.New("unexpected scheduledTaskChannel closure in handle handshake request")
	}
	return result.authSuccess, result.err
}

// Builds auth error response and sends it to the client.
func (ch *ClientHandler) sendAuthErrorToClient(requestFrame *frame.RawFrame) error {
	authErrorResponse, err := ch.buildAuthErrorResponse(requestFrame, ch.authErrorMessage)
	if err == nil {
		log.Warnf("Target handshake failed with an auth error, returning %v to client.", ch.authErrorMessage)
		ch.clientConnector.sendResponseToClient(authErrorResponse)
		return nil
	} else {
		return fmt.Errorf("target handshake failed with an auth error but could not create response frame: %w", err)
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

// Starts the handshake against the Target cluster in the background (goroutine).
//
// Returns error if the handshake could not be started.
//
// If the handshake is started but fails, the returned channel will contain the error.
//
// If the returned channel is closed before a value could be read, then the handshake has failed as well.
//
// The handshake was successful if the returned channel contains a "nil" value.
func (ch *ClientHandler) startTargetHandshake() (chan error, error) {
	startupFrame := ch.startupFrame
	if startupFrame == nil {
		return nil, errors.New("can not start target handshake before a Startup body was received")
	}
	targetStartupResponse := ch.targetStartupResponse
	if targetStartupResponse == nil {
		return nil, errors.New("can not start target handshake before a Startup response was received from target")
	}

	channel := make(chan error)
	ch.clientHandlerRequestWaitGroup.Add(1)
	go func() {
		defer ch.clientHandlerRequestWaitGroup.Done()
		defer close(channel)
		err := ch.handleTargetCassandraStartup(startupFrame, targetStartupResponse)
		channel <- err
	}()
	return channel, nil
}

// Handles a request, see the docs for the forwardRequest() function, as handleRequest is pretty much a wrapper
// around forwardRequest.
func (ch *ClientHandler) handleRequest(f *frame.RawFrame) {
	err := ch.forwardRequest(f, nil)

	if err != nil {
		log.Warnf("error sending request with opcode %02x and streamid %d: %s", f.Header.OpCode, f.Header.StreamId, err.Error())
		return
	}
}

// Forwards the request, parsing it and enqueuing it to the appropriate cluster connector(s)' write queue(s).
func (ch *ClientHandler) forwardRequest(request *frame.RawFrame, customResponseChannel chan *customResponse) error {
	overallRequestStartTime := time.Now()
	context := &frameDecodeContext{
		frame: request,
	}
	var err error
	context, err = modifyFrame(context)
	stmtInfo, err := parseStatement(
		context, ch.preparedStatementCache, ch.metricHandler, ch.currentKeyspaceName, ch.conf.ForwardReadsToTarget,
		ch.conf.ForwardSystemQueriesToTarget, ch.topologyConfig.VirtualizationEnabled)
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

	err = ch.executeStatement(context, stmtInfo, overallRequestStartTime, customResponseChannel)
	if err != nil {
		return err
	}
	return nil
}

// executeStatement executes the forward decision and waits for one or two responses, then returns the response
// that should be sent back to the client.
func (ch *ClientHandler) executeStatement(
	frameContext *frameDecodeContext, stmtInfo StatementInfo, overallRequestStartTime time.Time, customResponseChannel chan *customResponse) error {
	forwardDecision := stmtInfo.GetForwardDecision()
	log.Tracef("Opcode: %v, Forward decision: %v", frameContext.frame.Header.OpCode, forwardDecision)

	f := frameContext.frame
	originRequest := f
	targetRequest := f
	var clientResponse *frame.RawFrame

	switch castedStmtInfo := stmtInfo.(type) {
	case *InterceptedStatementInfo:
		interceptedQueryType := castedStmtInfo.GetQueryType()
		var interceptedQueryResponse message.Message
		var err error
		var controlConn *ControlConn
		if ch.conf.ForwardSystemQueriesToTarget {
			controlConn = ch.targetControlConn
		} else {
			controlConn = ch.originControlConn
		}
		virtualHosts, err := controlConn.GetVirtualHosts()
		if err != nil {
			return err
		}

		typeCodec := ch.typeCodecManager.GetOrCreate(f.Header.Version)

		switch interceptedQueryType {
		case peersV2:
			interceptedQueryResponse = &message.Invalid {
				ErrorMessage: "unconfigured table peers_v2",
			}
		case peersV1:
			interceptedQueryResponse, err = NewSystemPeersRowsResult(
				typeCodec, virtualHosts, controlConn.GetLocalVirtualHostIndex(),
				ch.conf.ProxyQueryPort, controlConn.PreferredIpColumnExists())
			if err != nil {
				return err
			}
		case local:
			localVirtualHost := virtualHosts[controlConn.GetLocalVirtualHostIndex()]
			interceptedQueryResponse, err = NewSystemLocalRowsResult(
				typeCodec, controlConn.GetSystemLocalInfo(), localVirtualHost, ch.conf.ProxyQueryPort)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("expected intercepted query type: %v", interceptedQueryType)
		}

		interceptedResponseFrame := frame.NewFrame(f.Header.Version, f.Header.StreamId, interceptedQueryResponse)
		interceptedResponseRawFrame, err := defaultCodec.ConvertToRawFrame(interceptedResponseFrame)
		if err != nil {
			return fmt.Errorf("could not convert intercepted response frame %v: %w", interceptedResponseFrame, err)
		}

		clientResponse = interceptedResponseRawFrame
	case *BoundStatementInfo:
		if forwardDecision == forwardToBoth || forwardDecision == forwardToTarget {
			decodedFrame, err := frameContext.GetOrDecodeFrame()
			if err != nil {
				return fmt.Errorf("could not decode execute raw frame: %w", err)
			}
			decodedFrame = decodedFrame.Clone()
			executeMsg, ok := decodedFrame.Body.Message.(*message.Execute)
			if !ok {
				return fmt.Errorf("expected Execute but got %v instead", decodedFrame.Body.Message.GetOpCode())
			}
			originalQueryId := executeMsg.QueryId
			executeMsg.QueryId = castedStmtInfo.GetPreparedData().GetTargetPreparedId()
			log.Tracef("Replacing prepared ID %s with %s for target cluster.", hex.EncodeToString(originalQueryId), hex.EncodeToString(executeMsg.QueryId))
			targetExecuteRequest, err := defaultCodec.ConvertToRawFrame(decodedFrame)
			if err != nil {
				return fmt.Errorf("could not convert target EXECUTE response to raw frame: %w", err)
			}
			targetRequest = targetExecuteRequest
		}
	}

	if forwardDecision == forwardToNone {
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

	reqCtx := NewRequestContext(f, stmtInfo, overallRequestStartTime, customResponseChannel)
	holder, err := ch.storeRequestContext(reqCtx)
	if err != nil {
		return err
	}

	proxyMetrics := ch.metricHandler.GetProxyMetrics()
	switch forwardDecision {
	case forwardToBoth:
		proxyMetrics.InFlightRequestsBoth.Add(1)
	case forwardToOrigin:
		proxyMetrics.InFlightRequestsOrigin.Add(1)
	case forwardToTarget:
		proxyMetrics.InFlightRequestsTarget.Add(1)
	default:
		log.Errorf("unexpected forwardDecision %v, unable to track proxy level metrics", forwardDecision)
	}

	ch.clientHandlerRequestWaitGroup.Add(1)
	timer := time.AfterFunc(time.Duration(ch.conf.RequestTimeoutMs) * time.Millisecond, func() {
		ch.closedRespChannelLock.RLock()
		defer ch.closedRespChannelLock.RUnlock()
		if ch.closedRespChannel {
			finished := reqCtx.SetTimeout(ch.nodeMetrics, f)
			if finished {
				ch.finishRequest(holder, reqCtx)
			}
			return
		}
		ch.respChannel <- NewTimeoutResponse(f)
	})
	reqCtx.SetTimer(timer)

	switch forwardDecision {
	case forwardToBoth:
		log.Tracef("Forwarding request with opcode %v for stream %v to OC and TC", f.Header.OpCode, f.Header.StreamId)
		ch.originCassandraConnector.sendRequestToCluster(originRequest)
		ch.targetCassandraConnector.sendRequestToCluster(targetRequest)
	case forwardToOrigin:
		log.Tracef("Forwarding request with opcode %v for stream %v to OC", f.Header.OpCode, f.Header.StreamId)
		ch.originCassandraConnector.sendRequestToCluster(originRequest)
	case forwardToTarget:
		log.Tracef("Forwarding request with opcode %v for stream %v to TC", f.Header.OpCode, f.Header.StreamId)
		ch.targetCassandraConnector.sendRequestToCluster(targetRequest)
	default:
		return fmt.Errorf("unknown forward decision %v, stream: %d", forwardDecision, f.Header.StreamId)
	}

	return nil
}

// Aggregates the responses received from the two clusters as follows:
//   - if both responses are a success OR both responses are a failure: return responseFromOC
//   - if either response is a failure, the failure "wins": return the failed response
//
// Also updates metrics appropriately.
func (ch *ClientHandler) aggregateAndTrackResponses(
	request *frame.RawFrame, responseFromOriginCassandra *frame.RawFrame, responseFromTargetCassandra *frame.RawFrame) *frame.RawFrame {

	originOpCode := responseFromOriginCassandra.Header.OpCode
	log.Tracef("Aggregating responses. OC opcode %d, TargetCassandra opcode %d", originOpCode, responseFromTargetCassandra.Header.OpCode)

	// aggregate responses and update relevant aggregate metrics for general failed or successful responses
	if isResponseSuccessful(responseFromOriginCassandra) && isResponseSuccessful(responseFromTargetCassandra) {
		if originOpCode == primitive.OpCodeSupported {
			log.Tracef("Aggregated response: both successes, sending back TC's response with opcode %d", originOpCode)
			return responseFromTargetCassandra
		} else if request.Header.OpCode == primitive.OpCodePrepare {
			// special case for PREPARE requests to always return ORIGIN, even though the default handling for "BOTH" requests would be enough
			return responseFromOriginCassandra
		} else {
			if ch.conf.ForwardReadsToTarget {
				log.Tracef("Aggregated response: both successes, sending back TC's response with opcode %d", responseFromTargetCassandra.Header.OpCode)
				return responseFromTargetCassandra
			} else {
				log.Tracef("Aggregated response: both successes, sending back OC's response with opcode %d", originOpCode)
				return responseFromOriginCassandra
			}
		}
	}

	proxyMetrics := ch.metricHandler.GetProxyMetrics()
	if !isResponseSuccessful(responseFromOriginCassandra) && !isResponseSuccessful(responseFromTargetCassandra) {
		log.Debugf("Aggregated response: both failures, sending back OC's response with opcode %d", originOpCode)
		proxyMetrics.FailedRequestsBoth.Add(1)
		return responseFromOriginCassandra
	}

	// if either response is a failure, the failure "wins" --> return the failed response
	if !isResponseSuccessful(responseFromOriginCassandra) {
		log.Debugf("Aggregated response: failure only on OC, sending back OC's response with opcode %d", originOpCode)
		proxyMetrics.FailedRequestsBothFailedOnOriginOnly.Add(1)
		return responseFromOriginCassandra
	} else {
		log.Debugf("Aggregated response: failure only on TargetCassandra, sending back TargetCassandra's response with opcode %d", originOpCode)
		proxyMetrics.FailedRequestsBothFailedOnTargetOnly.Add(1)

		return responseFromTargetCassandra
	}

}

// Replaces the credentials in the provided auth frame (which are the Target credentials) with
// the Origin credentials that are provided to the proxy in the configuration.
func (ch *ClientHandler) replaceAuthFrame(f *frame.RawFrame) (*frame.RawFrame, error) {
	parsedAuthFrame, err := defaultCodec.ConvertFromRawFrame(f)
	if err != nil {
		return nil, fmt.Errorf("could not extract auth credentials from frame to start the target handshake: %w", err)
	}

	authResponse, ok := parsedAuthFrame.Body.Message.(*message.AuthResponse)
	if !ok {
		return nil, fmt.Errorf("expected AuthResponse but got %v, can not proceed with target handshake", parsedAuthFrame.Body.Message)
	}

	authCreds, err := ParseCredentialsFromRequest(authResponse.Token)
	if err != nil {
		return nil, err
	}

	if authCreds == nil {
		log.Debugf("Found auth response frame without creds: %v", authResponse)
		return f, nil
	}

	log.Debugf("Successfuly extracted target credentials from auth frame: %v", authCreds)

	ch.targetCreds = authCreds

	originCreds := &AuthCredentials{
		Username: ch.originUsername,
		Password: ch.originPassword,
	}
	authResponse.Token = originCreds.Marshal()

	f, err = defaultCodec.ConvertToRawFrame(parsedAuthFrame)
	if err != nil {
		return nil, fmt.Errorf("could not convert new auth response to a raw frame, can not proceed with target handshake: %w", err)
	}

	return f, nil
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
func (ch *ClientHandler) getOrCreateRequestContextHolder(streamId int16) *requestContextHolder {
	reqCtxHolder, ok := ch.requestContextHolders.Load(streamId)
	if ok {
		return reqCtxHolder.(*requestContextHolder)
	} else {
		reqCtxHolder, _ := ch.requestContextHolders.LoadOrStore(streamId, NewRequestContextHolder())
		return reqCtxHolder.(*requestContextHolder)
	}
}

// Stores the provided request context in a RequestContextHolder. The holder is retrieved using getOrCreateRequestContextHolder,
// see the documentation on that function for more details.
func (ch *ClientHandler) storeRequestContext(reqCtx *RequestContext) (*requestContextHolder, error) {
	requestContextHolder := ch.getOrCreateRequestContextHolder(reqCtx.request.Header.StreamId)
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
func (ch *ClientHandler) trackClusterErrorMetrics(response *frame.RawFrame, cluster ClusterType) {
	if !isResponseSuccessful(response) {
		errorMsg, err := decodeErrorResult(response)
		if err != nil {
			log.Errorf("could not track read response: %v", err)
			return
		}

		switch cluster {
		case OriginCassandra:
			switch errorMsg.GetErrorCode() {
			case primitive.ErrorCodeUnprepared:
				ch.nodeMetrics.OriginMetrics.OriginUnpreparedErrors.Add(1)
			case primitive.ErrorCodeReadTimeout:
				ch.nodeMetrics.OriginMetrics.OriginReadTimeouts.Add(1)
			case primitive.ErrorCodeWriteTimeout:
				ch.nodeMetrics.OriginMetrics.OriginWriteTimeouts.Add(1)
			default:
				log.Debugf("Recording origin other error: %v", errorMsg)
				ch.nodeMetrics.OriginMetrics.OriginOtherErrors.Add(1)
			}
		case TargetCassandra:
			switch errorMsg.GetErrorCode() {
			case primitive.ErrorCodeUnprepared:
				ch.nodeMetrics.TargetMetrics.TargetUnpreparedErrors.Add(1)
				case primitive.ErrorCodeReadTimeout:
				ch.nodeMetrics.TargetMetrics.TargetReadTimeouts.Add(1)
				case primitive.ErrorCodeWriteTimeout:
				ch.nodeMetrics.TargetMetrics.TargetWriteTimeouts.Add(1)
				default:
					log.Debugf("Recording target other error: %v", errorMsg)
				ch.nodeMetrics.TargetMetrics.TargetOtherErrors.Add(1)
				}
		default:
			log.Errorf("unexpected clusterType %v, unable to track node metrics", cluster)
		}
	}
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