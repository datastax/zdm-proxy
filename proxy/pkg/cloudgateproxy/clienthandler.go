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
    - a global metricsHandler object (must be a reference to the one created in the proxy)
	- the prepared statement cache
    - the connection's keyspace, if a USE statement has been issued

*/

type ClientHandler struct {
	clientConnector *ClientConnector

	originCassandraConnector *ClusterConnector
	targetCassandraConnector *ClusterConnector

	preparedStatementCache *PreparedStatementCache

	metricsHandler  metrics.IMetricsHandler
	globalWaitGroup *sync.WaitGroup

	clientHandlerContext    context.Context
	clientHandlerCancelFunc context.CancelFunc

	currentKeyspaceName *atomic.Value

	authErrorMessage *message.AuthenticationError

	startupFrame *frame.RawFrame
	targetCreds  *AuthCredentials

	originUsername string
	originPassword string

	// map of request context holders that store the contexts for the active requests, keyed on streamID
	requestContextHolders *sync.Map

	reqChannel  <-chan *frame.RawFrame
	respChannel chan *Response

	requestWaitGroup *sync.WaitGroup

	closedRespChannel     bool
	closedRespChannelLock *sync.RWMutex

	responsesDoneChan chan<- bool
	requestsDoneChan  chan<- bool
	eventsDoneChan    chan<- bool

	requestResponseScheduler  *Scheduler
	clientConnectorScheduler  *Scheduler
	clusterConnectorScheduler *Scheduler

	conf *config.Config
}

func NewClientHandler(
	clientTcpConn net.Conn,
	originCassandraConnInfo *ClusterConnectionInfo,
	targetCassandraConnInfo *ClusterConnectionInfo,
	conf *config.Config,
	originUsername string,
	originPassword string,
	psCache *PreparedStatementCache,
	metricsHandler metrics.IMetricsHandler,
	waitGroup *sync.WaitGroup,
	globalContext context.Context,
	requestResponseScheduler *Scheduler,
	readScheduler *Scheduler,
	writeScheduler *Scheduler,
	numWorkers int) (*ClientHandler, error) {

	clientHandlerContext, clientHandlerCancelFunc := context.WithCancel(context.Background())

	go func() {
		select {
		case <-clientHandlerContext.Done():
			return
		case <-globalContext.Done():
			clientHandlerCancelFunc()
			return
		}
	}()

	respChannel := make(chan *Response, numWorkers)

	originConnector, err := NewClusterConnector(
		originCassandraConnInfo, conf, metricsHandler, waitGroup,
		clientHandlerContext, clientHandlerCancelFunc, respChannel, readScheduler, writeScheduler)
	if err != nil {
		clientHandlerCancelFunc()
		return nil, err
	}

	targetConnector, err := NewClusterConnector(
		targetCassandraConnInfo, conf, metricsHandler, waitGroup,
		clientHandlerContext, clientHandlerCancelFunc, respChannel, readScheduler, writeScheduler)
	if err != nil {
		clientHandlerCancelFunc()
		return nil, err
	}

	responsesDoneChan := make(chan bool, 1)
	requestsDoneChan := make(chan bool, 1)
	eventsDoneChan := make(chan bool, 1)
	requestsChannel := make(chan *frame.RawFrame, numWorkers)

	return &ClientHandler{
		clientConnector: NewClientConnector(
			clientTcpConn,
			conf,
			metricsHandler,
			waitGroup,
			requestsChannel,
			clientHandlerContext,
			clientHandlerCancelFunc,
			responsesDoneChan,
			requestsDoneChan,
			eventsDoneChan,
			readScheduler,
			writeScheduler),

		originCassandraConnector: originConnector,
		targetCassandraConnector: targetConnector,
		preparedStatementCache:   psCache,
		metricsHandler:           metricsHandler,
		globalWaitGroup:          waitGroup,
		clientHandlerContext:     clientHandlerContext,
		clientHandlerCancelFunc:  clientHandlerCancelFunc,
		currentKeyspaceName:      &atomic.Value{},
		authErrorMessage:         nil,
		startupFrame:             nil,
		originUsername:           originUsername,
		originPassword:           originPassword,
		requestContextHolders:    &sync.Map{},
		reqChannel:               requestsChannel,
		respChannel:              respChannel,
		requestWaitGroup:         &sync.WaitGroup{},
		closedRespChannel:        false,
		closedRespChannelLock:    &sync.RWMutex{},
		requestsDoneChan:         requestsDoneChan,
		responsesDoneChan:        responsesDoneChan,
		eventsDoneChan:           eventsDoneChan,
		requestResponseScheduler: requestResponseScheduler,
		conf:                     conf,
	}, nil
}

/**
 *	Initialises all components and launches all listening loops that they have.
 */
func (ch *ClientHandler) run(activeClients *int32) {
	ch.clientConnector.run(activeClients)
	ch.originCassandraConnector.run()
	ch.targetCassandraConnector.run()
	go func() {
		<- ch.originCassandraConnector.doneChan
		<- ch.targetCassandraConnector.doneChan
		ch.closedRespChannelLock.Lock()
		defer ch.closedRespChannelLock.Unlock()
		close(ch.respChannel)
		ch.closedRespChannel = true
	}()
	ch.requestLoop()
	ch.listenForEventMessages()
	ch.responseLoop()
}

// Infinite loop that blocks on receiving from the requests channel.
func (ch *ClientHandler) requestLoop() {
	ready := false
	var err error
	ch.globalWaitGroup.Add(1)
	log.Debugf("requestLoop starting now")
	go func() {
		defer ch.globalWaitGroup.Done()
		defer close(ch.requestsDoneChan)
		defer ch.originCassandraConnector.writeCoalescer.Close()
		defer ch.targetCassandraConnector.writeCoalescer.Close()

		connectionAddr := ch.clientConnector.connection.RemoteAddr().String()

		wg := &sync.WaitGroup{}
		defer wg.Wait()

		for {
			f, ok := <-ch.reqChannel
			if !ok {
				break
			}

			log.Debugf("Request received on client handler: %v", f.Header)
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

		ch.requestWaitGroup.Wait()
	}()
}

// Infinite loop that blocks on receiving from both cluster connector event channels.
//
// Event messages that come through will only be routed if
//   - it's a schema change from origin
func (ch *ClientHandler) listenForEventMessages() {
	ch.globalWaitGroup.Add(1)
	log.Debugf("listenForEventMessages loop starting now")
	go func() {
		defer ch.globalWaitGroup.Done()
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
				log.Debug("Received protocol error on event message listener, forwarding to client: ", body.Message)
			case *message.SchemaChangeEvent:
				if fromTarget {
					log.Infof("Received schema change event from target, skipping: %v", msgType)
					continue
				}
			case *message.StatusChangeEvent:
				if !fromTarget {
					log.Infof("Received status change event from origin, skipping: %v", msgType)
					continue
				}
			case *message.TopologyChangeEvent:
				if !fromTarget {
					log.Infof("Received topology change event from origin, skipping: %v", msgType)
					continue
				}
			default:
				log.Infof("Expected event message (fromTarget: %v) but got: %v", fromTarget, msgType)
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
	ch.globalWaitGroup.Add(1)
	log.Debugf("responseLoop starting now")
	go func() {
		defer ch.globalWaitGroup.Done()
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
					finished = reqCtx.SetTimeout(ch.metricsHandler, response.requestFrame)
				} else {
					finished = reqCtx.SetResponse(ch.metricsHandler, response.responseFrame, response.cluster)
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
	err := holder.Clear(reqCtx)
	if err != nil {
		log.Debugf("Could not free stream id: %v", err)
	}
	ch.requestWaitGroup.Done()

	switch reqCtx.decision {
	case forwardToBoth:
		ch.metricsHandler.TrackInHistogram(metrics.ProxyRequestDurationBoth, reqCtx.startTime)
		ch.metricsHandler.DecrementCountByOne(metrics.InFlightRequestsBoth)
	case forwardToOrigin:
		ch.metricsHandler.TrackInHistogram(metrics.ProxyRequestDurationOrigin, reqCtx.startTime)
		ch.metricsHandler.DecrementCountByOne(metrics.InFlightRequestsOrigin)
	case forwardToTarget:
		ch.metricsHandler.TrackInHistogram(metrics.ProxyRequestDurationTarget, reqCtx.startTime)
		ch.metricsHandler.DecrementCountByOne(metrics.InFlightRequestsTarget)
	default:
		log.Errorf("unexpected forwardDecision %v, unable to track proxy level metrics", reqCtx.decision)
	}

	aggregatedResponse, err := ch.computeAggregatedResponse(reqCtx)
	if err == nil {
		err = ch.processAggregatedResponse(aggregatedResponse)
	}

	if err != nil {
		if reqCtx.customResponseChannel != nil {
			close(reqCtx.customResponseChannel)
		}
		log.Debugf("Error handling request (%v): %v", reqCtx.request.Header, err)
		return
	}

	reqCtx.request = nil
	reqCtx.originResponse = nil
	reqCtx.targetResponse = nil

	if reqCtx.customResponseChannel != nil {
		reqCtx.customResponseChannel <- aggregatedResponse
	} else {
		ch.clientConnector.sendResponseToClient(aggregatedResponse)
	}
}

// Computes the response to be sent to the client based on the forward decision of the request.
func (ch *ClientHandler) computeAggregatedResponse(requestContext *RequestContext) (*frame.RawFrame, error) {
	forwardDecision := requestContext.decision
	if forwardDecision == forwardToOrigin {
		if requestContext.originResponse == nil {
			return nil, fmt.Errorf("did not receive response from origin cassandra channel, stream: %d", requestContext.request.Header.StreamId)
		}
		log.Debugf("Forward to origin: just returning the response received from OC: %d", requestContext.originResponse.Header.OpCode)
		if !isResponseSuccessful(requestContext.originResponse) {
			ch.metricsHandler.IncrementCountByOne(metrics.FailedRequestsOrigin)
		}
		return requestContext.originResponse, nil

	} else if forwardDecision == forwardToTarget {
		if requestContext.targetResponse == nil {
			return nil, fmt.Errorf("did not receive response from target cassandra channel, stream: %d", requestContext.request.Header.StreamId)
		}
		log.Debugf("Forward to target: just returning the response received from TC: %d", requestContext.targetResponse.Header.OpCode)
		if !isResponseSuccessful(requestContext.targetResponse) {
			ch.metricsHandler.IncrementCountByOne(metrics.FailedRequestsTarget)
		}
		return requestContext.targetResponse, nil

	} else if forwardDecision == forwardToBoth {
		if requestContext.originResponse == nil {
			return nil, fmt.Errorf("did not receive response from original cassandra channel, stream: %d", requestContext.request.Header.StreamId)
		}
		if requestContext.targetResponse == nil {
			return nil, fmt.Errorf("did not receive response from target cassandra channel, stream: %d", requestContext.request.Header.StreamId)
		}
		return ch.aggregateAndTrackResponses(requestContext.originResponse, requestContext.targetResponse), nil

	} else {
		return nil, fmt.Errorf("unknown forward decision %v, stream: %d", forwardDecision, requestContext.targetResponse.Header.StreamId)
	}
}

// Modifies internal state based on the provided aggregated response (e.g. storing prepared IDs)
func (ch *ClientHandler) processAggregatedResponse(response *frame.RawFrame) error {
	switch response.Header.OpCode {
	case primitive.OpCodeResult:
		body, err := defaultCodec.DecodeBody(response.Header, bytes.NewReader(response.Body))
		if err != nil {
			return fmt.Errorf("error decoding result response: %w", err)
		}

		resultMsg, ok := body.Message.(message.Result)
		if !ok {
			return fmt.Errorf("expected RESULT message but got %T", body.Message)
		}

		resultType := resultMsg.GetResultType()
		if resultType == primitive.ResultTypePrepared || resultType == primitive.ResultTypeSetKeyspace {
			switch bodyMsg := body.Message.(type) {
			case *message.PreparedResult:
				if bodyMsg.PreparedQueryId == nil {
					log.Warnf("unexpected prepared query id nil")
				} else {
					ch.preparedStatementCache.cachePreparedId(response.Header.StreamId, bodyMsg.PreparedQueryId)
				}
			case *message.SetKeyspaceResult:
				if bodyMsg.Keyspace == "" {
					log.Warnf("unexpected set keyspace empty")
				} else {
					ch.currentKeyspaceName.Store(bodyMsg.Keyspace)
				}
			default:
				return fmt.Errorf("expected resulttype %v but got %T", resultType, bodyMsg)
			}
		}
	}
	return nil
}

type handshakeRequestResult struct {
	authSuccess  bool
	err          error
	responseChan chan *frame.RawFrame
}

// Handles requests while the handshake has not been finalized.
//
// Forwards certain requests that are part of the handshake to Origin only.
//
// When the Origin handshake ends, this function blocks, waiting until Target handshake is done.
// This ensures that the client connection is Ready only when both Cluster Connector connections are ready.
func (ch *ClientHandler) handleHandshakeRequest(f *frame.RawFrame, wg *sync.WaitGroup) (bool, error) {
	scheduledTaskChannel := make(chan *handshakeRequestResult, 1)
	wg.Add(1)
	ch.requestResponseScheduler.Schedule(func() {
		defer wg.Done()
		defer close(scheduledTaskChannel)
		if ch.authErrorMessage != nil {
			scheduledTaskChannel <- &handshakeRequestResult{
				authSuccess: false,
				err:         ch.sendAuthErrorToClient(f),
			}
			return
		}

		if f.Header.OpCode == primitive.OpCodeStartup {
			ch.startupFrame = f
		} else if f.Header.OpCode == primitive.OpCodeAuthResponse {
			newAuthFrame, err := ch.replaceAuthFrame(f)
			if err != nil {
				scheduledTaskChannel <- &handshakeRequestResult{
					authSuccess: false,
					err:         err,
				}
				return
			}

			f = newAuthFrame
		}

		responseChan := make(chan *frame.RawFrame, 1)
		err := ch.forwardRequest(f, responseChan)
		if err != nil {
			scheduledTaskChannel <- &handshakeRequestResult{
				authSuccess: false,
				err:         err,
			}
			return
		}

		scheduledTaskChannel <- &handshakeRequestResult{
			authSuccess:  false,
			err:          err,
			responseChan: responseChan,
		}
	})

	result, ok := <-scheduledTaskChannel
	if !ok {
		return false, errors.New("unexpected scheduledTaskChannel closure in handle handshake request")
	}

	if result.responseChan == nil {
		return result.authSuccess, result.err
	}

	responseChan := result.responseChan

	select {
	case response, ok := <-responseChan:
		if response == nil || !ok {
			return false, nil
		}

		scheduledTaskChannel = make(chan *handshakeRequestResult, 1)
		wg.Add(1)
		ch.requestResponseScheduler.Schedule(func() {
			defer wg.Done()
			defer close(scheduledTaskChannel)
			tempResult := &handshakeRequestResult{
				authSuccess:  false,
				err:          nil,
				responseChan: nil,
			}
			if response.Header.OpCode == primitive.OpCodeReady || response.Header.OpCode == primitive.OpCodeAuthSuccess {
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
						tempResult.err = ch.sendAuthErrorToClient(f)
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
			ch.clientConnector.sendResponseToClient(response)
		})

		result, ok = <- scheduledTaskChannel
		if !ok {
			return false, errors.New("unexpected scheduledTaskChannel closure in handle handshake request")
		}
		return result.authSuccess, result.err
	case <-ch.clientHandlerContext.Done():
		return false, ShutdownErr
	}
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
		return nil, errors.New("can not start target handshake before a Startup message was received")
	}

	channel := make(chan error)
	ch.requestWaitGroup.Add(1)
	go func() {
		defer ch.requestWaitGroup.Done()
		defer close(channel)
		err := ch.handleTargetCassandraStartup(startupFrame)
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
func (ch *ClientHandler) forwardRequest(request *frame.RawFrame, customResponseChannel chan *frame.RawFrame) error {
	overallRequestStartTime := time.Now()

	forwardDecision, err := inspectFrame(
		request, ch.preparedStatementCache, ch.metricsHandler, ch.currentKeyspaceName, ch.conf)
	if err != nil {
		if errVal, ok := err.(*UnpreparedExecuteError); ok {
			unpreparedFrame, err := createUnpreparedFrame(errVal)
			if err != nil {
				return err
			}
			log.Debugf(
				"PS Cache miss, created unprepared response with version %v, streamId %v and preparedId %v",
				errVal.Header.Version, errVal.Header.StreamId, errVal.preparedId)

			// send it back to client
			ch.clientConnector.sendResponseToClient(unpreparedFrame)
			log.Debugf("Unprepared Response sent, exiting handleRequest now")
			return nil
		}
		return err
	}

	err = ch.executeForwardDecision(request, forwardDecision, overallRequestStartTime, customResponseChannel)
	if err != nil {
		return err
	}
	return nil
}

// executeForwardDecision executes the forward decision and waits for one or two responses, then returns the response
// that should be sent back to the client.
func (ch *ClientHandler) executeForwardDecision(f *frame.RawFrame, forwardDecision forwardDecision, overallRequestStartTime time.Time, customResponseChannel chan *frame.RawFrame) error {
	log.Tracef("Opcode: %v, Forward decision: %v", f.Header.OpCode, forwardDecision)

	reqCtx := NewRequestContext(f, forwardDecision, overallRequestStartTime, customResponseChannel)
	holder, err := ch.storeRequestContext(reqCtx)
	if err != nil {
		return err
	}

	switch forwardDecision {
	case forwardToBoth:
		ch.metricsHandler.IncrementCountByOne(metrics.InFlightRequestsBoth)
	case forwardToOrigin:
		ch.metricsHandler.IncrementCountByOne(metrics.InFlightRequestsOrigin)
	case forwardToTarget:
		ch.metricsHandler.IncrementCountByOne(metrics.InFlightRequestsTarget)
	default:
		log.Errorf("unexpected forwardDecision %v, unable to track proxy level metrics", forwardDecision)
	}

	ch.requestWaitGroup.Add(1)
	timer := time.AfterFunc(queryTimeout, func() {
		ch.closedRespChannelLock.RLock()
		defer ch.closedRespChannelLock.RUnlock()
		if ch.closedRespChannel {
			finished := reqCtx.SetTimeout(ch.metricsHandler, f)
			if finished {
				ch.finishRequest(holder, reqCtx)
			}
			return
		}
		ch.respChannel <- NewTimeoutResponse(f)
	})
	reqCtx.SetTimer(timer)

	if forwardDecision == forwardToOrigin {
		log.Debugf("Forwarding request with opcode %v for stream %v to OC", f.Header.OpCode, f.Header.StreamId)
		ch.originCassandraConnector.sendRequestToCluster(f)
		return nil

	} else if forwardDecision == forwardToTarget {
		log.Debugf("Forwarding request with opcode %v for stream %v to TC", f.Header.OpCode, f.Header.StreamId)
		ch.targetCassandraConnector.sendRequestToCluster(f)
		return nil

	} else if forwardDecision == forwardToBoth {
		log.Debugf("Forwarding request with opcode %v for stream %v to OC and TC", f.Header.OpCode, f.Header.StreamId)
		ch.originCassandraConnector.sendRequestToCluster(f)
		ch.targetCassandraConnector.sendRequestToCluster(f)
		return nil
	} else {
		return fmt.Errorf("unknown forward decision %v, stream: %d", forwardDecision, f.Header.StreamId)
	}
}

// Aggregates the responses received from the two clusters as follows:
//   - if both responses are a success OR both responses are a failure: return responseFromOC
//   - if either response is a failure, the failure "wins": return the failed response
//
// Also updates metrics appropriately.
func (ch *ClientHandler) aggregateAndTrackResponses(responseFromOriginCassandra *frame.RawFrame, responseFromTargetCassandra *frame.RawFrame) *frame.RawFrame {

	originOpCode := responseFromOriginCassandra.Header.OpCode
	log.Debugf("Aggregating responses. OC opcode %d, TargetCassandra opcode %d", originOpCode, responseFromTargetCassandra.Header.OpCode)

	// aggregate responses and update relevant aggregate metrics for general failed or successful responses
	if isResponseSuccessful(responseFromOriginCassandra) && isResponseSuccessful(responseFromTargetCassandra) {
		if originOpCode == primitive.OpCodeSupported {
			log.Debugf("Aggregated response: both successes, sending back TC's response with opcode %d", originOpCode)
			return responseFromTargetCassandra
		} else {
			log.Debugf("Aggregated response: both successes, sending back OC's response with opcode %d", originOpCode)
			return responseFromOriginCassandra
		}
	}

	if !isResponseSuccessful(responseFromOriginCassandra) && !isResponseSuccessful(responseFromTargetCassandra) {
		log.Debugf("Aggregated response: both failures, sending back OC's response with opcode %d", originOpCode)
		ch.metricsHandler.IncrementCountByOne(metrics.FailedRequestsBoth)
		return responseFromOriginCassandra
	}

	// if either response is a failure, the failure "wins" --> return the failed response
	if !isResponseSuccessful(responseFromOriginCassandra) {
		log.Debugf("Aggregated response: failure only on OC, sending back OC's response with opcode %d", originOpCode)
		ch.metricsHandler.IncrementCountByOne(metrics.FailedRequestsBothFailedOnOriginOnly)
		return responseFromOriginCassandra
	} else {
		log.Debugf("Aggregated response: failure only on TargetCassandra, sending back TargetCassandra's response with opcode %d", originOpCode)
		ch.metricsHandler.IncrementCountByOne(metrics.FailedRequestsBothFailedOnTargetOnly)
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
		return nil, fmt.Errorf("expected error message but got %T", body.Message)
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
				ch.metricsHandler.IncrementCountByOne(metrics.OriginUnpreparedErrors)
			case primitive.ErrorCodeReadTimeout:
				ch.metricsHandler.IncrementCountByOne(metrics.OriginReadTimeouts)
			case primitive.ErrorCodeWriteTimeout:
				ch.metricsHandler.IncrementCountByOne(metrics.OriginWriteTimeouts)
			default:
				ch.metricsHandler.IncrementCountByOne(metrics.OriginOtherErrors)
			}
		case TargetCassandra:
			switch errorMsg.GetErrorCode() {
			case primitive.ErrorCodeUnprepared:
				ch.metricsHandler.IncrementCountByOne(metrics.TargetUnpreparedErrors)
			case primitive.ErrorCodeReadTimeout:
				ch.metricsHandler.IncrementCountByOne(metrics.TargetReadTimeouts)
			case primitive.ErrorCodeWriteTimeout:
				ch.metricsHandler.IncrementCountByOne(metrics.TargetWriteTimeouts)
			default:
				ch.metricsHandler.IncrementCountByOne(metrics.TargetOtherErrors)
			}
		default:
			log.Errorf("unexpected clusterType %v, unable to track cluster metrics", cluster)
		}
	}
}