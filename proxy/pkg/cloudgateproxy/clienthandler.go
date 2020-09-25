package cloudgateproxy

import (
	"encoding/binary"
	"fmt"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"net"
)

/*
	ClientHandler holds the 1:1:1 pairing:
    	- a client connector (+ a channel on which the connector sends the requests coming from the client)
    	- a connector to OC
    	- a connector to TC

	Additionally, it has:
    - a global metrics object (must be a reference to the one created in the proxy)
	- the prepared statement cache

 */

type ClientHandler struct {

	clientConnector				*ClientConnector
	clientConnectorRequestChan	chan *Frame	// channel on which the client connector passes requests coming from the client

	originCassandraConnector	*ClusterConnector
	targetCassandraConnector	*ClusterConnector

	preparedStatementCache		*PreparedStatementCache

	metrics						*metrics.Metrics
}

func NewClientHandler(	clientTcpConn net.Conn,
						originCassandraConnInfo *ClusterConnectionInfo,
						targetCassandraConnInfo *ClusterConnectionInfo,
						psCache *PreparedStatementCache,
						metrics *metrics.Metrics) *ClientHandler{
	clientReqChan := make(chan *Frame)

	return &ClientHandler{
		clientConnector:			NewClientConnector(clientTcpConn, clientReqChan, metrics),
		clientConnectorRequestChan: clientReqChan,
		originCassandraConnector: 	NewClusterConnector(originCassandraConnInfo, metrics),
		targetCassandraConnector: 	NewClusterConnector(targetCassandraConnInfo, metrics),
		preparedStatementCache: 	psCache,
		metrics:					metrics,
	}
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
	log.Debugf("listenForClientRequests loop starting now")
	go func() {
		for {
			// TODO: handle channel closed
			frame := <-ch.clientConnectorRequestChan
			log.Debugf("frame received")
			if !authenticated {
				log.Debugf("not authenticated")
				// Handle client authentication
				authenticated, err = ch.handleStartupFrame(frame)
				if err != nil {
					log.Error(err)
				}
				log.Debugf("authenticated? %t", authenticated)
				continue
			}

			go ch.handleRequest(frame)
		}
	}()
}

/**
 *	Handles a request. Called as a goroutine every time a valid requestFrame is received,
 *	so each request is executed concurrently to other requests.
 *
 *	Calls one or two goroutines forwardToCluster(), so the request is executed on each cluster concurrently
 */
func (ch *ClientHandler) handleRequest(f *Frame) error {

	paths, isWriteRequest, err := CassandraParseRequest(ch.preparedStatementCache, f.RawBytes)
	if err != nil {
		return err
	}

	log.Debugf("parsed request, writeRequest? %t, resulting path(s) %v", isWriteRequest, paths)

	query, err := createQuery(f, paths, ch.preparedStatementCache, isWriteRequest)
	if err != nil {
		log.Errorf("Error creating query %v", err)
		return err
	}
	log.Debugf("Statement created of type %s", query.Type)

	var responseFromTargetCassandra *Frame
	var responseFromOriginCassandra *Frame
	var ok bool

	log.Debugf("Forwarding query of type %v with opcode %v and path %v for stream %v to OC", query.Type, query.Opcode, paths, query.Stream)
	responseFromOriginCassandraChan := ch.originCassandraConnector.forwardToCluster(query.Query, query.Stream)

	// if it is a write request (also a batch involving at least one write) then also parse it for the TargetCassandra cluster
	if isWriteRequest {
		log.Debugf("Forwarding query of type %v with opcode %v and path %v for stream %v to TC", query.Type, query.Opcode, paths, query.Stream)
		responseFromTargetCassandraChan := ch.targetCassandraConnector.forwardToCluster(query.Query, query.Stream)
		responseFromTargetCassandra, ok = <- responseFromTargetCassandraChan
		if !ok {
			return fmt.Errorf("did not receive response from TargetCassandra channel, stream: %d", f.Stream)
		}
	}

	// wait for OC response in any case
	responseFromOriginCassandra, ok = <- responseFromOriginCassandraChan
	if !ok {
		return fmt.Errorf("did not receive response from original cassandra channel, stream: %d", f.Stream)
	}

	var response *Frame
	if isWriteRequest {
		log.Debugf("Write request: aggregating the responses received - OC: %d && TC: %d", responseFromOriginCassandra.Opcode, responseFromTargetCassandra.Opcode)
		response = aggregateResponses(responseFromOriginCassandra, responseFromTargetCassandra)
	} else {
		log.Debugf("Non-write request: just returning the response received from OC: %d", responseFromOriginCassandra.Opcode)
		response = responseFromOriginCassandra
	}

	// send overall response back to client
	ch.clientConnector.responseChannel <- response.RawBytes

	// if it was a prepare request, cache the ID and statement info
	if query.Type == PREPARE && isResponseSuccessful(response){
		ch.preparedStatementCache.cachePreparedID(response)
	}

	return nil
}

/**
 *	Aggregates the responses received from the two clusters as follows:
 *		- if both responses are a success OR both responses are a failure: return responseFromOC
 *		- if either response is a failure, the failure "wins": return the failed response
 */
func aggregateResponses(responseFromOriginalCassandra *Frame, responseFromTargetCassandra *Frame) *Frame {

	log.Debugf("Aggregating responses. OC opcode %d, TargetCassandra opcode %d", responseFromOriginalCassandra.Opcode, responseFromTargetCassandra.Opcode)

	if (isResponseSuccessful(responseFromOriginalCassandra) && isResponseSuccessful(responseFromTargetCassandra)) ||
		(!isResponseSuccessful(responseFromOriginalCassandra) && !isResponseSuccessful(responseFromTargetCassandra)) {
		log.Debugf("Aggregated response: both successes or both failures, sending back OC's response with opcode %d", responseFromOriginalCassandra.Opcode)
		return responseFromOriginalCassandra
	}

	// if either response is a failure, the failure "wins" --> return the failed response
	if !isResponseSuccessful(responseFromOriginalCassandra) {
		log.Debugf("Aggregated response: failure only on OC, sending back OC's response with opcode %d", responseFromOriginalCassandra.Opcode)
		return responseFromOriginalCassandra
	} else {
		log.Debugf("Aggregated response: failure only on TargetCassandra, sending back TargetCassandra's response with opcode %d", responseFromOriginalCassandra.Opcode)
		return responseFromTargetCassandra
	}

}

func isResponseSuccessful(f *Frame) bool {
	return f.Opcode == 0x08 || f.Opcode == 0x06
}

func checkError(body []byte) {
	errCode := binary.BigEndian.Uint16(body[0:2])
	switch errCode {
	case 0x0000:
		// Server Error
		//TODO p.Metrics.IncrementServerErrors()
	case 0x1100:
		// Write Timeout
		//TODO p.Metrics.IncrementWriteFails()
	case 0x1200:
		// Read Timeout
		//TODO p.Metrics.IncrementReadFails()
	}

}