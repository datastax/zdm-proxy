package cloudgateproxy

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
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
	clientConnector            *ClientConnector
	clientConnectorRequestChan chan *Frame // channel on which the client connector passes requests coming from the client

	originCassandraConnector *ClusterConnector
	targetCassandraConnector *ClusterConnector

	preparedStatementCache *PreparedStatementCache

	metricsHandler  metrics.IMetricsHandler
	globalWaitGroup *sync.WaitGroup

	clientHandlerContext    context.Context
	clientHandlerCancelFunc context.CancelFunc

	currentKeyspaceName *atomic.Value

	startupFrame *Frame
}

func NewClientHandler(clientTcpConn net.Conn,
	originCassandraConnInfo *ClusterConnectionInfo,
	targetCassandraConnInfo *ClusterConnectionInfo,
	psCache *PreparedStatementCache,
	metricsHandler metrics.IMetricsHandler,
	waitGroup *sync.WaitGroup,
	globalContext context.Context) (*ClientHandler, error) {
	clientReqChan := make(chan *Frame)

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
		originCassandraConnInfo, metricsHandler, waitGroup, clientHandlerContext, clientHandlerCancelFunc)
	if err != nil {
		clientHandlerCancelFunc()
		return nil, err
	}

	targetConnector, err := NewClusterConnector(
		targetCassandraConnInfo, metricsHandler, waitGroup, clientHandlerContext, clientHandlerCancelFunc)
	if err != nil {
		clientHandlerCancelFunc()
		return nil, err
	}

	return &ClientHandler{
		clientConnector:            NewClientConnector(clientTcpConn, clientReqChan, metricsHandler, waitGroup, clientHandlerContext, clientHandlerCancelFunc),
		clientConnectorRequestChan: clientReqChan,
		originCassandraConnector:   originConnector,
		targetCassandraConnector:   targetConnector,
		preparedStatementCache:     psCache,
		metricsHandler:             metricsHandler,
		globalWaitGroup:            waitGroup,
		clientHandlerContext:       clientHandlerContext,
		clientHandlerCancelFunc:    clientHandlerCancelFunc,
		currentKeyspaceName:        &atomic.Value{},
		startupFrame:               nil,
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

		handleWaitGroup := &sync.WaitGroup{}
		for {
			frame, ok := <-ch.clientConnectorRequestChan

			if !ok {
				log.Debug("Shutting down client requests listener.")
				break
			}

			log.Tracef("frame received")
			if !ready {
				log.Tracef("not ready")
				// Handle client authentication
				ready, err = ch.handleHandshakeRequest(frame, handleWaitGroup)
				if err != nil && err != ShutdownErr {
					log.Error(err)
				}
				if ready {
					log.Infof(
						"Handshake successful with client %s",
						ch.clientConnector.connection.RemoteAddr().String())
				}
				log.Tracef("ready? %t", ready)
				continue
			}

			ch.handleRequest(frame, handleWaitGroup)
		}

		handleWaitGroup.Wait()
	}()
}

/**
 *	Handles a request. Called as a goroutine every time a valid requestFrame is received,
 *	so each request is executed concurrently to other requests.
 *
 *	Calls one or two goroutines forwardToCluster(), so the request is executed on each cluster concurrently
 */
func (ch *ClientHandler) handleHandshakeRequest(f *Frame, waitGroup *sync.WaitGroup) (bool, error) {
	if f.Opcode == OpCodeStartup {
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
	if response.Opcode == OpCodeReady || response.Opcode == OpCodeAuthSuccess {
		// target handshake must happen within a single client request lifetime
		// to guarantee that no other request with the same
		// stream id goes to target in the meantime

		// if we add stream id mapping logic in the future, then
		// we can start the target handshake earlier and wait for it to end here

		targetAuthChannel, err := ch.startTargetHandshake(waitGroup)
		if err != nil {
			return false, err
		}

		select {
		case err, ok := <-targetAuthChannel:
			if !ok {
				err = errors.New("target handshake failed (channel closed)")
			}

			if err != nil {
				log.Errorf("handshake failed, shutting down the client handler and connectors: %s", err.Error())
				ch.clientHandlerCancelFunc()
				return false, ShutdownErr
			}

			authSuccess = true

		case <-ch.clientHandlerContext.Done():
			return false, ShutdownErr
		}
	}

	// send overall response back to client
	ch.clientConnector.responseChannel <- response.RawBytes

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
func (ch *ClientHandler) handleRequest(f *Frame, waitGroup *sync.WaitGroup) {
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		response, err := ch.forwardRequest(f)

		if err != nil {
			log.Warnf("error handling request with opcode %02x and streamid %d: %s", f.Opcode, f.StreamId, err.Error())
			return
		}

		if response != nil {
			// send overall response back to client
			ch.clientConnector.responseChannel <- response.RawBytes
		}
	}()
}

func (ch *ClientHandler) forwardRequest(f *Frame) (*Frame, error) {
	overallRequestStartTime := time.Now()

	forwardDecision, err := inspectFrame(f, ch.preparedStatementCache, ch.metricsHandler, ch.currentKeyspaceName)
	if err != nil {
		return nil, err
	}
	log.Tracef("Opcode: %v, Forward decision: %v", f.Opcode, forwardDecision)

	if forwardDecision == forwardToTarget || forwardDecision == forwardToBoth {
		ch.metricsHandler.IncrementCountByOne(metrics.InFlightWriteRequests)
		defer ch.metricsHandler.TrackInHistogram(metrics.ProxyWriteLatencyHist, overallRequestStartTime)
		defer ch.metricsHandler.DecrementCountByOne(metrics.InFlightWriteRequests)
	}
	if forwardDecision == forwardToOrigin || forwardDecision == forwardToBoth {
		ch.metricsHandler.IncrementCountByOne(metrics.InFlightReadRequests)
		defer ch.metricsHandler.TrackInHistogram(metrics.ProxyReadLatencyHist, overallRequestStartTime)
		defer ch.metricsHandler.DecrementCountByOne(metrics.InFlightReadRequests)
	}

	response, err := ch.executeForwardDecision(f, forwardDecision)
	if err != nil {
		return nil, err
	}

	// Status and topology events should not be forwarded back to the client
	if response.Opcode == OpCodeEvent {
		eventType, _ := readString(response.Body)
		if eventType != "SCHEMA_CHANGE" {
			return nil, nil
		}
	}

	// if it was a prepare request, cache the ID and statement info
	if isResponsePrepared(response) {
		preparedId, _ := readShortBytes(response.Body[4:])
		if preparedId != nil {
			ch.preparedStatementCache.cachePreparedId(response.StreamId, preparedId)
		}
	}

	if isResponseSetKeyspace(response) {
		keyspaceName, _ := readString(response.Body[4:])
		if keyspaceName != "" {
			ch.currentKeyspaceName.Store(keyspaceName)
		}
	}

	return response, nil
}

// executeForwardDecision executes the forward decision and waits for one or two responses, then returns the response
// that should be sent back to the client.
func (ch *ClientHandler) executeForwardDecision(f *Frame, forwardDecision forwardDecision) (*Frame, error) {

	if forwardDecision == forwardToOrigin {
		log.Debugf("Forwarding request with opcode %v for stream %v to OC", f.Opcode, f.StreamId)
		startTime := time.Now()
		originChan := ch.originCassandraConnector.forwardToCluster(f.RawBytes, f.StreamId)
		response, ok := <-originChan
		if !ok {
			return nil, fmt.Errorf("did not receive response from original cassandra channel, stream: %d", f.StreamId)
		}
		ch.metricsHandler.TrackInHistogram(metrics.OriginReadLatencyHist, startTime)
		log.Debugf("Forward to origin: just returning the response received from OC: %d", response.Opcode)
		trackReadResponse(response, ch.metricsHandler)
		return response, nil

	} else if forwardDecision == forwardToTarget {
		log.Debugf("Forwarding request with opcode %v for stream %v to TC", f.Opcode, f.StreamId)
		startTime := time.Now()
		targetChan := ch.targetCassandraConnector.forwardToCluster(f.RawBytes, f.StreamId)
		response, ok := <-targetChan
		if !ok {
			return nil, fmt.Errorf("did not receive response from target cassandra channel, stream: %d", f.StreamId)
		}
		ch.metricsHandler.TrackInHistogram(metrics.TargetWriteLatencyHist, startTime)
		log.Debugf("Forward to target: just returning the response received from TC: %d", response.Opcode)
		trackReadResponse(response, ch.metricsHandler)
		return response, nil

	} else if forwardDecision == forwardToBoth {
		log.Debugf("Forwarding request with opcode %v for stream %v to OC and TC", f.Opcode, f.StreamId)
		startTime := time.Now()
		originChan := ch.originCassandraConnector.forwardToCluster(f.RawBytes, f.StreamId)
		targetChan := ch.targetCassandraConnector.forwardToCluster(f.RawBytes, f.StreamId)
		var originResponse, targetResponse *Frame
		var ok bool
		for originResponse == nil || targetResponse == nil {
			//goland:noinspection GoNilness
			select {
			case originResponse, ok = <-originChan:
				if !ok {
					return nil, fmt.Errorf("did not receive response from original cassandra channel, stream: %d", f.StreamId)
				}
				originChan = nil // ignore further channel operations
				ch.metricsHandler.TrackInHistogram(metrics.OriginWriteLatencyHist, startTime)
			case targetResponse, ok = <-targetChan:
				if !ok {
					return nil, fmt.Errorf("did not receive response from target cassandra channel, stream: %d", f.StreamId)
				}
				targetChan = nil // ignore further channel operations
				ch.metricsHandler.TrackInHistogram(metrics.TargetWriteLatencyHist, startTime)
			}
		}
		return ch.aggregateAndTrackResponses(originResponse, targetResponse), nil

	} else {
		return nil, fmt.Errorf("unknown forward decision %v, stream: %d", forwardDecision, f.StreamId)
	}
}

/**
 *	Aggregates the responses received from the two clusters as follows:
 *		- if both responses are a success OR both responses are a failure: return responseFromOC
 *		- if either response is a failure, the failure "wins": return the failed response
 *	Also updates metrics appropriately
 */
func (ch *ClientHandler) aggregateAndTrackResponses(responseFromOriginCassandra *Frame, responseFromTargetCassandra *Frame) *Frame {

	log.Debugf("Aggregating responses. OC opcode %d, TargetCassandra opcode %d", responseFromOriginCassandra.Opcode, responseFromTargetCassandra.Opcode)

	// track specific write failures in relevant metrics
	if !isResponseSuccessful(responseFromOriginCassandra) {
		ch.trackFailedIndividualWriteResponse(responseFromOriginCassandra, true)
	}

	if !isResponseSuccessful(responseFromTargetCassandra) {
		ch.trackFailedIndividualWriteResponse(responseFromTargetCassandra, false)
	}

	// aggregate responses and update relevant aggregate metrics for general failed or successful responses
	if isResponseSuccessful(responseFromOriginCassandra) && isResponseSuccessful(responseFromTargetCassandra) {
		log.Debugf("Aggregated response: both successes, sending back OC's response with opcode %d", responseFromOriginCassandra.Opcode)
		ch.metricsHandler.IncrementCountByOne(metrics.SuccessBothWrites)
		return responseFromOriginCassandra
	}

	if !isResponseSuccessful(responseFromOriginCassandra) && !isResponseSuccessful(responseFromTargetCassandra) {
		log.Debugf("Aggregated response: both failures, sending back OC's response with opcode %d", responseFromOriginCassandra.Opcode)
		ch.metricsHandler.IncrementCountByOne(metrics.FailedBothWrites)
		return responseFromOriginCassandra
	}

	// if either response is a failure, the failure "wins" --> return the failed response
	if !isResponseSuccessful(responseFromOriginCassandra) {
		log.Debugf("Aggregated response: failure only on OC, sending back OC's response with opcode %d", responseFromOriginCassandra.Opcode)
		ch.metricsHandler.IncrementCountByOne(metrics.FailedOriginOnlyWrites)
		return responseFromOriginCassandra
	} else {
		log.Debugf("Aggregated response: failure only on TargetCassandra, sending back TargetCassandra's response with opcode %d", responseFromOriginCassandra.Opcode)
		ch.metricsHandler.IncrementCountByOne(metrics.FailedTargetOnlyWrites)
		return responseFromTargetCassandra
	}

}

/**
Updates read-related metrics based on the outcome in the response
*/

func trackReadResponse(response *Frame, mh metrics.IMetricsHandler) {
	if isResponseSuccessful(response) {
		mh.IncrementCountByOne(metrics.SuccessReads)
	} else {
		errCode := binary.BigEndian.Uint16(response.Body[0:2])
		switch errCode {
		case 0x2500:
			mh.IncrementCountByOne(metrics.UnpreparedReads)
		case 0x1200:
			mh.IncrementCountByOne(metrics.ReadTimeOutsOriginCluster)
		default:
			mh.IncrementCountByOne(metrics.FailedReads)
		}
	}
}

/**
Updates metrics related to individual write responses for failed writes.
Only deals with Unprepared and Timed Out failures, as general write failures are tracked as aggregates
*/
func (ch *ClientHandler) trackFailedIndividualWriteResponse(response *Frame, fromOrigin bool) {
	errCode := binary.BigEndian.Uint16(response.Body[0:2])
	switch errCode {
	case 0x2500:
		if fromOrigin {
			ch.metricsHandler.IncrementCountByOne(metrics.UnpreparedOriginWrites)
		} else {
			ch.metricsHandler.IncrementCountByOne(metrics.UnpreparedTargetWrites)
		}
	case 0x1100:
		if fromOrigin {
			ch.metricsHandler.IncrementCountByOne(metrics.WriteTimeOutsOriginCluster)
		} else {
			ch.metricsHandler.IncrementCountByOne(metrics.WriteTimeOutsTargetCluster)
		}
	}
}

func isUnpreparedError(f *Frame) bool {
	errCode, err := readShort(f.Body)
	if err != nil {
		return false
	}
	return ErrorCode(errCode) == ErrorCodeUnprepared
}

func isResponseSuccessful(response *Frame) bool {
	return !(response.Opcode == OpCodeError)
}

func isResponseSetKeyspace(response *Frame) bool {
	kind := getResultKind(response)
	return kind == ResultKindSetKeyspace
}

func isResponsePrepared(response *Frame) bool {
	kind := getResultKind(response)
	return kind == ResultKindPrepared
}

func getResultKind(response *Frame) ResultKind {
	if response.Opcode == OpCodeResult {
		kind, err := readInt(response.Body)
		if err != nil {
			return 0
		}
		return ResultKind(kind)
	}
	return 0
}
