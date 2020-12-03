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

	startupFrame *frame.RawFrame

	eventsChannel chan *frame.RawFrame

	targetUsername string
	targetPassword string
}

func NewClientHandler(
	clientTcpConn net.Conn,
	originCassandraConnInfo *ClusterConnectionInfo,
	targetCassandraConnInfo *ClusterConnectionInfo,
	conf *config.Config,
	targetUsername string,
	targetPassword string,
	psCache *PreparedStatementCache,
	metricsHandler metrics.IMetricsHandler,
	waitGroup *sync.WaitGroup,
	globalContext context.Context) (*ClientHandler, error) {

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

	originConnector, err := NewClusterConnector(
		originCassandraConnInfo, conf, metricsHandler, waitGroup, clientHandlerContext, clientHandlerCancelFunc)
	if err != nil {
		clientHandlerCancelFunc()
		return nil, err
	}

	targetConnector, err := NewClusterConnector(
		targetCassandraConnInfo, conf, metricsHandler, waitGroup, clientHandlerContext, clientHandlerCancelFunc)
	if err != nil {
		clientHandlerCancelFunc()
		return nil, err
	}

	eventsChannel := make(chan *frame.RawFrame, conf.EventQueueSizeFrames)

	return &ClientHandler{
		clientConnector: NewClientConnector(
			clientTcpConn,
			eventsChannel,
			conf,
			metricsHandler,
			waitGroup,
			clientHandlerContext,
			clientHandlerCancelFunc),

		originCassandraConnector: originConnector,
		targetCassandraConnector: targetConnector,
		preparedStatementCache:   psCache,
		metricsHandler:           metricsHandler,
		globalWaitGroup:          waitGroup,
		clientHandlerContext:     clientHandlerContext,
		clientHandlerCancelFunc:  clientHandlerCancelFunc,
		currentKeyspaceName:      &atomic.Value{},
		startupFrame:             nil,
		eventsChannel:            eventsChannel,
		targetUsername:           targetUsername,
		targetPassword:           targetPassword,
	}, nil
}

/**
 *	Initialises all components and launches all listening loops that they have.
 */
func (ch *ClientHandler) run() {
	ch.clientConnector.run()
	ch.originCassandraConnector.run()
	ch.targetCassandraConnector.run()
	ch.listenForClientRequests()
	ch.listenForEventMessages()
}

/**
 *	Infinite loop that blocks on receiving from clientConnectorRequestChan
 *	Every request that comes through will spawn a handleRequest() goroutine
 */
func (ch *ClientHandler) listenForClientRequests() {
	ready := false
	var err error
	ch.globalWaitGroup.Add(1)
	log.Debugf("listenForClientRequests loop starting now")
	go func() {
		defer ch.globalWaitGroup.Done()
		defer close(ch.clientConnector.responseChannel)
		defer ch.originCassandraConnector.writeCoalescer.Close()
		defer ch.targetCassandraConnector.writeCoalescer.Close()

		connectionAddr := ch.clientConnector.connection.RemoteAddr().String()
		handleWaitGroup := &sync.WaitGroup{}
		for {
			f, ok := <-ch.clientConnector.requestChannel
			if !ok {
				break
			}

			log.Debugf("Request received on client handler: %v", f.Header)
			if !ready {
				log.Tracef("not ready")
				// Handle client authentication
				ready, err = ch.handleHandshakeRequest(f, handleWaitGroup)
				if err != nil && !errors.Is(err, ShutdownErr) {
					log.Error(err)
				}
				if ready {
					log.Infof(
						"Handshake successful with client %s", connectionAddr)
				}
				log.Tracef("ready? %t", ready)
				continue
			}

			ch.handleRequest(f, handleWaitGroup)
		}

		log.Infof("Shutting down client handler request listener %v.", connectionAddr)

		handleWaitGroup.Wait()
	}()
}

/**
 *	Infinite loop that blocks on receiving from both cluster connector event channels
 *	Event messages that come through will only be routed if:
 *  - it's a schema change from origin
 */
func (ch *ClientHandler) listenForEventMessages() {
	ch.globalWaitGroup.Add(1)
	log.Debugf("listenForEventMessages loop starting now")
	go func() {
		defer ch.globalWaitGroup.Done()
		defer close(ch.eventsChannel)
		shutDownChannels := 0
		targetChannel := ch.targetCassandraConnector.clusterConnEventsChan
		originChannel := ch.originCassandraConnector.clusterConnEventsChan
		for {
			if shutDownChannels >= 2 {
				break
			}

			var frame *frame.RawFrame
			var ok bool
			var fromTarget bool

			//goland:noinspection ALL
			select {
			case frame, ok = <-targetChannel:
				if !ok {
					log.Info("Target event channel closed")
					shutDownChannels++
					targetChannel = nil
					continue
				}
				fromTarget = true
			case frame, ok = <-originChannel:
				if !ok {
					log.Info("Origin event channel closed")
					shutDownChannels++
					originChannel = nil
					continue
				}
				fromTarget = false
			}

			log.Debugf("Event received (fromTarget: %v) on client handler: %v", fromTarget, frame.Header)

			body, err := defaultCodec.DecodeBody(frame.Header, bytes.NewReader(frame.Body))
			if err != nil {
				log.Warnf("Error decoding event response: %v", err)
				continue
			}

			switch eventMsg := body.Message.(type) {
			case *message.SchemaChangeEvent:
				if fromTarget {
					log.Infof("Received schema change event from target, skipping: %v", eventMsg)
					continue
				}
			case *message.StatusChangeEvent:
				if !fromTarget {
					log.Infof("Received status change event from origin, skipping: %v", eventMsg)
					continue
				}
			case *message.TopologyChangeEvent:
				if !fromTarget {
					log.Infof("Received topology change event from origin, skipping: %v", eventMsg)
					continue
				}
			default:
				log.Infof("Expected event message (fromTarget: %v) but got: %v", fromTarget, eventMsg)
				continue
			}

			ch.eventsChannel <- frame
		}

		log.Infof("Shutting down client event messages listener.")
	}()
}

/**
 *	Handles a request. Called as a goroutine every time a valid requestFrame is received,
 *	so each request is executed concurrently to other requests.
 *
 *	Calls one or two goroutines forwardToCluster(), so the request is executed on each cluster concurrently
 */
func (ch *ClientHandler) handleHandshakeRequest(f *frame.RawFrame, waitGroup *sync.WaitGroup) (bool, error) {
	if f.Header.OpCode == primitive.OpCodeStartup {
		ch.startupFrame = f
	}

	response, err := ch.forwardRequest(f)

	if err != nil {
		return false, err
	}

	if response == nil {
		return false, nil
	}

	authSuccess := false
	if response.Header.OpCode == primitive.OpCodeReady || response.Header.OpCode == primitive.OpCodeAuthSuccess {
		// target handshake must happen within a single client request lifetime
		// to guarantee that no other request with the same
		// stream id goes to target in the meantime

		// if we add stream id mapping logic in the future, then
		// we can start the target handshake earlier and wait for it to end here

		targetAuthChannel, err := ch.startTargetHandshake(waitGroup)
		if err != nil {
			return false, err
		}

		err, ok := <-targetAuthChannel
		if !ok {
			return false, errors.New("target handshake failed (channel closed)")
		}

		if err != nil {
			log.Errorf("handshake failed, shutting down the client handler and connectors: %s", err.Error())
			ch.clientHandlerCancelFunc()
			return false, fmt.Errorf("handshake failed: %w", ShutdownErr)
		}

		authSuccess = true
	}

	// send overall response back to client
	ch.clientConnector.responseChannel <- response

	return authSuccess, nil
}

func (ch *ClientHandler) startTargetHandshake(waitGroup *sync.WaitGroup) (chan error, error) {
	startupFrame := ch.startupFrame
	if startupFrame == nil {
		return nil, errors.New("can not start target handshake before a Startup message was received")
	}

	channel := make(chan error)
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		defer close(channel)
		err := ch.handleTargetCassandraStartup(startupFrame)
		channel <- err
	}()
	return channel, nil
}

/**
 *	Handles a request. Called as a goroutine every time a valid requestFrame is received,
 *	so each request is executed concurrently to other requests.
 *
 *	Calls one or two goroutines forwardToCluster(), so the request is executed on each cluster concurrently
 */
func (ch *ClientHandler) handleRequest(f *frame.RawFrame, waitGroup *sync.WaitGroup) {
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		response, err := ch.forwardRequest(f)

		if err != nil {
			log.Warnf("error handling request with opcode %02x and streamid %d: %s", f.Header.OpCode, f.Header.StreamId, err.Error())
			return
		}

		if response != nil {
			// send overall response back to client
			ch.clientConnector.responseChannel <- response
		}
	}()
}

func (ch *ClientHandler) forwardRequest(request *frame.RawFrame) (*frame.RawFrame, error) {
	overallRequestStartTime := time.Now()

	forwardDecision, err := inspectFrame(request, ch.preparedStatementCache, ch.metricsHandler, ch.currentKeyspaceName)
	if err != nil {
		if errVal, ok := err.(*UnpreparedExecuteError); ok {
			unpreparedFrame, err := createUnpreparedFrame(errVal)
			if err != nil {
				return nil, err
			}
			log.Debugf(
				"PS Cache miss, created unprepared response with version %v, streamId %v and preparedId %v",
				errVal.Header.Version, errVal.Header.StreamId, errVal.preparedId)

			// send it back to client
			ch.clientConnector.responseChannel <- unpreparedFrame
			log.Debugf("Unprepared Response sent, exiting handleRequest now")
			return nil, nil
		}
		return nil, err
	}
	log.Tracef("Opcode: %v, Forward decision: %v", request.Header.OpCode, forwardDecision)

	switch forwardDecision {
	case forwardToBoth:
		ch.metricsHandler.IncrementCountByOne(metrics.InFlightRequestsBoth)
		defer ch.metricsHandler.TrackInHistogram(metrics.ProxyRequestDurationBoth, overallRequestStartTime)
		defer ch.metricsHandler.DecrementCountByOne(metrics.InFlightRequestsBoth)
	case forwardToOrigin:
		ch.metricsHandler.IncrementCountByOne(metrics.InFlightRequestsOrigin)
		defer ch.metricsHandler.TrackInHistogram(metrics.ProxyRequestDurationOrigin, overallRequestStartTime)
		defer ch.metricsHandler.DecrementCountByOne(metrics.InFlightRequestsOrigin)
	case forwardToTarget:
		ch.metricsHandler.IncrementCountByOne(metrics.InFlightRequestsTarget)
		defer ch.metricsHandler.TrackInHistogram(metrics.ProxyRequestDurationTarget, overallRequestStartTime)
		defer ch.metricsHandler.DecrementCountByOne(metrics.InFlightRequestsTarget)
	default:
		log.Errorf("unexpected forwardDecision %v, unable to track proxy level metrics", forwardDecision)
	}

	response, err := ch.executeForwardDecision(request, forwardDecision)
	if err != nil {
		return nil, err
	}

	switch response.Header.OpCode {
	case primitive.OpCodeResult:
		body, err := defaultCodec.DecodeBody(response.Header, bytes.NewReader(response.Body))
		if err != nil {
			return nil, fmt.Errorf("error decoding result response: %w", err)
		}

		resultMsg, ok := body.Message.(message.Result)
		if !ok {
			return nil, fmt.Errorf("expected RESULT message but got %T", body.Message)
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
				return nil, fmt.Errorf("expected resulttype %v but got %T", resultType, bodyMsg)
			}
		}
	}
	return response, nil
}

// executeForwardDecision executes the forward decision and waits for one or two responses, then returns the response
// that should be sent back to the client.
func (ch *ClientHandler) executeForwardDecision(f *frame.RawFrame, forwardDecision forwardDecision) (*frame.RawFrame, error) {

	if forwardDecision == forwardToOrigin {
		log.Debugf("Forwarding request with opcode %v for stream %v to OC", f.Header.OpCode, f.Header.StreamId)
		originChan := ch.originCassandraConnector.forwardToCluster(f)
		response, ok := <-originChan
		if !ok {
			return nil, fmt.Errorf("did not receive response from original cassandra channel, stream: %d", f.Header.StreamId)
		}
		log.Debugf("Forward to origin: just returning the response received from OC: %d", response.Header.OpCode)
		if !isResponseSuccessful(response) {
			ch.metricsHandler.IncrementCountByOne(metrics.FailedRequestsOrigin)
		}
		return response, nil

	} else if forwardDecision == forwardToTarget {
		log.Debugf("Forwarding request with opcode %v for stream %v to TC", f.Header.OpCode, f.Header.StreamId)
		targetChan := ch.targetCassandraConnector.forwardToCluster(f)
		response, ok := <-targetChan
		if !ok {
			return nil, fmt.Errorf("did not receive response from target cassandra channel, stream: %d", f.Header.StreamId)
		}
		log.Debugf("Forward to target: just returning the response received from TC: %d", response.Header.OpCode)
		if !isResponseSuccessful(response) {
			ch.metricsHandler.IncrementCountByOne(metrics.FailedRequestsTarget)
		}
		return response, nil

	} else if forwardDecision == forwardToBoth {
		log.Debugf("Forwarding request with opcode %v for stream %v to OC and TC", f.Header.OpCode, f.Header.StreamId)
		originChan := ch.originCassandraConnector.forwardToCluster(f)
		targetChan := ch.targetCassandraConnector.forwardToCluster(f)
		var originResponse, targetResponse *frame.RawFrame
		var ok bool
		for originResponse == nil || targetResponse == nil {
			//goland:noinspection GoNilness
			select {
			case originResponse, ok = <-originChan:
				if !ok {
					return nil, fmt.Errorf("did not receive response from original cassandra channel, stream: %d", f.Header.StreamId)
				}
				originChan = nil // ignore further channel operations
			case targetResponse, ok = <-targetChan:
				if !ok {
					return nil, fmt.Errorf("did not receive response from target cassandra channel, stream: %d", f.Header.StreamId)
				}
				targetChan = nil // ignore further channel operations
			}
		}
		return ch.aggregateAndTrackResponses(originResponse, targetResponse), nil

	} else {
		return nil, fmt.Errorf("unknown forward decision %v, stream: %d", forwardDecision, f.Header.StreamId)
	}
}

/**
 *	Aggregates the responses received from the two clusters as follows:
 *		- if both responses are a success OR both responses are a failure: return responseFromOC
 *		- if either response is a failure, the failure "wins": return the failed response
 *	Also updates metrics appropriately
 */
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
