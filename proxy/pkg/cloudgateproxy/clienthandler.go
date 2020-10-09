package cloudgateproxy

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"net"
	"sync"
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
	authenticated := false
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
			if !authenticated {
				log.Tracef("not authenticated")
				// Handle client authentication
				authenticated, err = ch.handleStartupFrame(frame)
				if err != nil && err != ShutdownErr {
					log.Error(err)
				}
				if authenticated {
					log.Infof(
						"Authentication successful with client %s",
						ch.clientConnector.connection.RemoteAddr().String())
				}
				log.Tracef("authenticated? %t", authenticated)
				continue
			}

			handleWaitGroup.Add(1)
			go ch.handleRequest(frame, handleWaitGroup)
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
func (ch *ClientHandler) handleRequest(f *Frame, waitGroup *sync.WaitGroup) error {

	overallRequestStartTime := time.Now()

	defer waitGroup.Done()

	forwardDecision, err := inspectFrame(f, ch.preparedStatementCache, ch.metricsHandler)
	if err != nil {
		return err
	}
	log.Tracef("Opcode: %v, Forward decision: %v", f.Opcode, forwardDecision)

	if forwardDecision == forwardToBoth {
		ch.metricsHandler.IncrementCountByOne(metrics.InFlightWriteRequests)
		defer ch.metricsHandler.TrackInHistogram(metrics.ProxyWriteLatencyHist, overallRequestStartTime)
		defer ch.metricsHandler.DecrementCountByOne(metrics.InFlightWriteRequests)
	} else {
		ch.metricsHandler.IncrementCountByOne(metrics.InFlightReadRequests)
		defer ch.metricsHandler.TrackInHistogram(metrics.ProxyReadLatencyHist, overallRequestStartTime)
		defer ch.metricsHandler.DecrementCountByOne(metrics.InFlightReadRequests)
	}

	var responseFromTargetCassandra *Frame
	var responseFromOriginCassandra *Frame
	var ok bool

	log.Debugf("Forwarding request with opcode %v for stream %v to OC", f.Opcode, f.StreamId)
	originCassandraRequestStartTime := time.Now()
	responseFromOriginCassandraChan := ch.originCassandraConnector.forwardToCluster(f.RawBytes, f.StreamId)

	// if it is a write request then also parse it for the TargetCassandra cluster
	if forwardDecision == forwardToBoth {
		log.Debugf("Forwarding request with opcode %v for stream %v to TC", f.Opcode, f.StreamId)
		targetCassandraRequestStartTime := time.Now()
		responseFromTargetCassandraChan := ch.targetCassandraConnector.forwardToCluster(f.RawBytes, f.StreamId)
		responseFromTargetCassandra, ok = <-responseFromTargetCassandraChan
		ch.metricsHandler.TrackInHistogram(metrics.TargetWriteLatencyHist, targetCassandraRequestStartTime)
		if !ok {
			return fmt.Errorf("did not receive response from TargetCassandra channel, stream: %d", f.StreamId)
		}
	}

	// wait for OC response in any case
	responseFromOriginCassandra, ok = <-responseFromOriginCassandraChan
	if forwardDecision == forwardToBoth {
		ch.metricsHandler.TrackInHistogram(metrics.OriginWriteLatencyHist, originCassandraRequestStartTime)
	} else {
		ch.metricsHandler.TrackInHistogram(metrics.OriginReadLatencyHist, originCassandraRequestStartTime)
	}
	if !ok {
		return fmt.Errorf("did not receive response from original cassandra channel, stream: %d", f.StreamId)
	}

	var response *Frame
	if forwardDecision == forwardToBoth {
		log.Debugf("Write request: aggregating the responses received - OC: %d && TC: %d", responseFromOriginCassandra.Opcode, responseFromTargetCassandra.Opcode)
		response = aggregateAndTrackResponses(responseFromOriginCassandra, responseFromTargetCassandra, ch.metricsHandler)
	} else {
		log.Debugf("Non-write request: just returning the response received from OC: %d", responseFromOriginCassandra.Opcode)
		response = responseFromOriginCassandra
		trackReadResponse(response, ch.metricsHandler)
	}

	// send overall response back to client
	ch.clientConnector.responseChannel <- response.RawBytes

	// if it was a prepare request, cache the ID and statement info
	if f.Opcode == OpCodePrepare && isResponseSuccessful(response) {
		ch.preparedStatementCache.cachePreparedID(response)
	}

	return nil
}

/**
 *	Aggregates the responses received from the two clusters as follows:
 *		- if both responses are a success OR both responses are a failure: return responseFromOC
 *		- if either response is a failure, the failure "wins": return the failed response
 *	Also updates metrics appropriately
 */
func aggregateAndTrackResponses(responseFromOriginCassandra *Frame, responseFromTargetCassandra *Frame, mh metrics.IMetricsHandler) *Frame {

	log.Debugf("Aggregating responses. OC opcode %d, TargetCassandra opcode %d", responseFromOriginCassandra.Opcode, responseFromTargetCassandra.Opcode)

	// track specific write failures in relevant metrics
	if !isResponseSuccessful(responseFromOriginCassandra) {
		trackFailedIndividualWriteResponse(responseFromOriginCassandra, true, mh)
	}

	if !isResponseSuccessful(responseFromTargetCassandra) {
		trackFailedIndividualWriteResponse(responseFromTargetCassandra, false, mh)
	}

	// aggregate responses and update relevant aggregate metrics for general failed or successful responses
	if isResponseSuccessful(responseFromOriginCassandra) && isResponseSuccessful(responseFromTargetCassandra) {
		log.Debugf("Aggregated response: both successes, sending back OC's response with opcode %d", responseFromOriginCassandra.Opcode)
		mh.IncrementCountByOne(metrics.SuccessBothWrites)
		return responseFromOriginCassandra
	}

	if !isResponseSuccessful(responseFromOriginCassandra) && !isResponseSuccessful(responseFromTargetCassandra) {
		log.Debugf("Aggregated response: both failures, sending back OC's response with opcode %d", responseFromOriginCassandra.Opcode)
		mh.IncrementCountByOne(metrics.FailedBothWrites)
		return responseFromOriginCassandra
	}

	// if either response is a failure, the failure "wins" --> return the failed response
	if !isResponseSuccessful(responseFromOriginCassandra) {
		log.Debugf("Aggregated response: failure only on OC, sending back OC's response with opcode %d", responseFromOriginCassandra.Opcode)
		mh.IncrementCountByOne(metrics.FailedOriginOnlyWrites)
		return responseFromOriginCassandra
	} else {
		log.Debugf("Aggregated response: failure only on TargetCassandra, sending back TargetCassandra's response with opcode %d", responseFromOriginCassandra.Opcode)
		mh.IncrementCountByOne(metrics.FailedTargetOnlyWrites)
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
func trackFailedIndividualWriteResponse(response *Frame, fromOrigin bool, mh metrics.IMetricsHandler) {
	errCode := binary.BigEndian.Uint16(response.Body[0:2])
	switch errCode {
	case 0x2500:
		if fromOrigin {
			mh.IncrementCountByOne(metrics.UnpreparedOriginWrites)
		} else {
			mh.IncrementCountByOne(metrics.UnpreparedTargetWrites)
		}
	case 0x1100:
		if fromOrigin {
			mh.IncrementCountByOne(metrics.WriteTimeOutsOriginCluster)
		} else {
			mh.IncrementCountByOne(metrics.WriteTimeOutsTargetCluster)
		}
	}
}

func isUnpreparedError(f *Frame) bool {
	errCode := binary.BigEndian.Uint16(f.Body[0:2])
	return errCode == 0x2500
}

func isResponseSuccessful(response *Frame) bool {
	return !(response.Opcode == 0x00)
}
