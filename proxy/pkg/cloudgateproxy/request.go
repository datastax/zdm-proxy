package cloudgateproxy

import (
	"encoding/binary"
	log "github.com/sirupsen/logrus"
)

// Method that handles a request.
// This is called as a goroutine every time a valid frame is received and it does not contain an authentication request
// One goroutine for each request, so each request is executed concurrently
func (p* CloudgateProxy) handleRequest(f *Frame, clientApplicationIP string) error {

	// CassandraParseRequest returns an array of paths (just strings) with the format "/opcode/action/table"
	// one path if a simple request, multiple paths if a batch
	// parsing requests is not cluster-specific, even considering prepared statements (as preparedIDs are computed based on the statement only)

	paths, isWriteRequest, isServiceRequest, err := CassandraParseRequest(p.preparedStatementCache, f.RawBytes)
	if err != nil {
		return err
	}

	log.Debugf("parsed request, writeRequest? %t, serviceRequest? %t, resulting path(s) %v", isWriteRequest, isServiceRequest, paths)

	originCassandraQuery, err := p.createQuery(f, clientApplicationIP, paths, false)
	if err != nil {
		log.Errorf("Error creating query %v", err)
		return err
	}
	log.Debugf("Statement for originCassandra created. Query of type %s", originCassandraQuery.Type)

	// This has to happen here and not in the createQuery call, because we want to do it only once
	if originCassandraQuery.Type == PREPARE {
		log.Debugf("tracking statement to be prepared")
		p.trackStatementToBePrepared(originCassandraQuery, isWriteRequest)
		log.Debugf("statement to be prepared tracked in transient map")
	}

	var responseFromOriginalCassandra *Frame
	var responseFromAstra *Frame

	// request is forwarded to each cluster in a separate goroutine so this happens concurrently
	responseFromOriginalCassandraChan := make(chan *Frame)
	go p.forwardToCluster(originCassandraQuery, isServiceRequest, responseFromOriginalCassandraChan)

	log.Debugf("Launched forwardToCluster (OriginCassandra) goroutine")
	// if it is a write request (also a batch involving at least one write) then also parse it for the Astra cluster
	if isWriteRequest {
		log.Debugf("Write request, now creating statement for Astra")
		astraQuery, err := p.createQuery(f, clientApplicationIP, paths, true)
		if err != nil {
			return err
		}
		responseFromAstraChan := make(chan *Frame)
		go p.forwardToCluster(astraQuery, isServiceRequest, responseFromAstraChan)
		log.Debugf("Launched forwardToCluster (Astra) goroutine")
		// we only wait for the astra response if the request was sent to astra. this is why the receive from this channel is in the if block
		responseFromAstra = <- responseFromAstraChan
	}

	// wait for OC response in any case
	responseFromOriginalCassandra = <- responseFromOriginalCassandraChan

	var response *Frame
	if isWriteRequest {
		log.Debugf("Write request: aggregating the responses received - OC: %d && Astra: %d", responseFromOriginalCassandra.Opcode, responseFromAstra.Opcode)
		response = aggregateResponses(responseFromOriginalCassandra, responseFromAstra)
	} else {
		log.Debugf("Non-write request: just returning the response received from OC: %d", responseFromOriginalCassandra.Opcode)
		response = responseFromOriginalCassandra
	}

	// send overall response back to client
	p.responseForClientChannels[clientApplicationIP] <- response.RawBytes
	// if it was a prepare request, cache the ID and statement info
	if originCassandraQuery.Type == PREPARE && isResponseSuccessful(response){
		p.cachePreparedID(response)
	}

	return nil
}

func aggregateResponses(responseFromOriginalCassandra *Frame, responseFromAstra *Frame) *Frame {

	log.Debugf("Aggregating responses. OC opcode %d, Astra opcode %d", responseFromOriginalCassandra.Opcode, responseFromAstra.Opcode)

	//	if both responses are a success OR both responses are a failure --> return responseFromOC
	if (isResponseSuccessful(responseFromOriginalCassandra) && isResponseSuccessful(responseFromAstra)) ||
		(!isResponseSuccessful(responseFromOriginalCassandra) && !isResponseSuccessful(responseFromAstra)) {
		log.Debugf("Aggregated response: both successes or both failures, sending back OC's response with opcode %d", responseFromOriginalCassandra.Opcode)
		return responseFromOriginalCassandra
	}

	// if either response is a failure, the failure "wins" --> return the failed response
	if !isResponseSuccessful(responseFromOriginalCassandra) {
		log.Debugf("Aggregated response: failure only on OC, sending back OC's response with opcode %d", responseFromOriginalCassandra.Opcode)
		return responseFromOriginalCassandra
	} else {
		log.Debugf("Aggregated response: failure only on Astra, sending back Astra's response with opcode %d", responseFromOriginalCassandra.Opcode)
		return responseFromAstra
	}

}

func isResponseSuccessful(f *Frame) bool {
	return f.Opcode == 0x08 || f.Opcode == 0x06
}

func (p* CloudgateProxy) trackStatementToBePrepared(q*Query, isWriteRequest bool) {
	// add the statement info for this query to the transient map of statements to be prepared
	stmtInfo := PreparedStatementInfo{Statement: q.Query, Keyspace: q.Keyspace, IsWriteStatement: isWriteRequest}
	// TODO is it necessary to lock in this case?
	p.lock.Lock()
	p.statementsBeingPrepared[q.Stream] = stmtInfo
	p.lock.Unlock()
}

func (p* CloudgateProxy) cachePreparedID(f *Frame) {
	log.Debugf("In cachePreparedID")

	data := f.RawBytes

	kind := int(binary.BigEndian.Uint32(data[9:13]))
	log.Debugf("Kind: %d", kind)
	if kind != 4 {
		// TODO error: this result is not a reply to a PREPARE request
	}

	//idLength := int(binary.BigEndian.Uint16(data[13:15]))
	//log.Debugf("idLength %d", idLength)
	idLength := int(binary.BigEndian.Uint16(data[13 : 15]))
	preparedID := string(data[15 : 15+idLength])

	log.Debugf("PreparedID: %s for stream %d", preparedID, f.Stream)

	p.lock.Lock()
	log.Debugf("cachePreparedID: lock acquired")
	// move the information about this statement into the cache
	p.preparedStatementCache[preparedID] = p.statementsBeingPrepared[f.Stream]
	log.Debugf("PSInfo set in map for PreparedID: %s", preparedID, f.Stream)
	// remove it from the temporary map
	delete(p.statementsBeingPrepared, f.Stream)
	log.Debugf("cachePreparedID: removing statement info from transient map")
	p.lock.Unlock()
	log.Debugf("cachePreparedID: lock released")

}



