package cloudgateproxy

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/riptano/cloud-gate/proxy/pkg/cqlparser"
	"github.com/riptano/cloud-gate/proxy/pkg/frame"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	"github.com/riptano/cloud-gate/proxy/pkg/query"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"strings"
)

// Method that handles a request.
// This is called as a goroutine every time a valid frame is received and it does not contain an authentication request
// One goroutine for each request, so each request is executed concurrently
func (p* CloudgateProxy) handleRequest(f *frame.Frame, clientApplicationIP string) error {

	// CassandraParseRequest returns an array of paths (just strings) with the format "/opcode/action/table"
	// one path if a simple request, multiple paths if a batch
	// parsing requests is not cluster-specific, even considering prepared statements (as preparedIDs are computed based on the statement only)

	paths, isWriteRequest, isServiceRequest, err := cqlparser.CassandraParseRequest(p.preparedStatementCache, f.RawBytes)
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
	if originCassandraQuery.Type == query.PREPARE {
		log.Debugf("tracking statement to be prepared")
		p.trackStatementToBePrepared(originCassandraQuery, isWriteRequest)
		log.Debugf("statement to be prepared tracked in transient map")
	}

	var responseFromOriginalCassandra *frame.Frame
	var responseFromAstra *frame.Frame

	// request is forwarded to each cluster in a separate goroutine so this happens concurrently
	responseFromOriginalCassandraChan := make(chan *frame.Frame)
	go p.forwardToCluster(originCassandraQuery, isServiceRequest, responseFromOriginalCassandraChan)

	log.Debugf("Launched forwardToCluster (OriginCassandra) goroutine")
	// if it is a write request (also a batch involving at least one write) then also parse it for the Astra cluster
	if isWriteRequest {
		log.Debugf("Write request, now creating statement for Astra")
		astraQuery, err := p.createQuery(f, clientApplicationIP, paths, true)
		if err != nil {
			return err
		}
		responseFromAstraChan := make(chan *frame.Frame)
		go p.forwardToCluster(astraQuery, isServiceRequest, responseFromAstraChan)
		log.Debugf("Launched forwardToCluster (Astra) goroutine")
		// we only wait for the astra response if the request was sent to astra. this is why the receive from this channel is in the if block
		responseFromAstra = <- responseFromAstraChan
	}

	// wait for OC response in any case
	responseFromOriginalCassandra = <- responseFromOriginalCassandraChan

	var response *frame.Frame
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
	if originCassandraQuery.Type == query.PREPARE && isResponseSuccessful(response){
		p.cachePreparedID(response)
	}

	return nil
}

func aggregateResponses(responseFromOriginalCassandra *frame.Frame, responseFromAstra *frame.Frame) *frame.Frame{

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

func isResponseSuccessful(f *frame.Frame) bool {
	return f.Opcode == 0x08 || f.Opcode == 0x06
}

// Simple function that reads data from a connection and builds a frame
func parseFrame(src net.Conn, frameHeader []byte, metrics *metrics.Metrics) (*frame.Frame, error) {
	sourceAddress := src.RemoteAddr().String()

	// [Alice] read the frameHeader, whose length is constant (9 bytes), and put it into this slice
	log.Debugf("reading frame header from src %s", sourceAddress)
	bytesRead, err := io.ReadFull(src, frameHeader)
	if err != nil {
		if err != io.EOF {
			log.Debugf("%s disconnected", sourceAddress)
		} else {
			log.Debugf("error reading frame header. bytesRead %d", bytesRead)
			log.Error(err)
		}
		return nil, err
	}
	log.Debugf("frameheader number of bytes read by ReadFull %d", bytesRead) // [Alice]
	log.Debugf("frameheader content read by ReadFull %v", frameHeader) // [Alice]
	bodyLen := binary.BigEndian.Uint32(frameHeader[5:9])
	log.Debugf("bodyLen %d", bodyLen) // [Alice]
	data := frameHeader
	bytesSoFar := 0

	log.Debugf("data: %v", data)

	if bodyLen != 0 {
		for bytesSoFar < int(bodyLen) {
			rest := make([]byte, int(bodyLen)-bytesSoFar)
			bytesRead, err := io.ReadFull(src, rest)
			if err != nil {
				log.Error(err)
				continue	// [Alice] next iteration of this small for loop, not the outer infinite one
			}
			data = append(data, rest[:bytesRead]...)
			bytesSoFar += bytesRead
		}
	}
	//log.Debugf("(from %s): %v", src.RemoteAddr(), string(data))
	f := frame.New(data)
	metrics.IncrementFrames()

	if f.Flags&0x01 == 1 {
		log.Errorf("compression flag for stream %d set, unable to parse query beyond header", f.Stream)
	}

	return f, err
}

func (p *CloudgateProxy) createQuery(f *frame.Frame, clientIPAddress string, paths []string, toAstra bool) (*query.Query, error){
	var q *query.Query

	if paths[0] == cqlparser.UnknownPreparedQueryPath {
		// TODO is this handling correct?
		return q, fmt.Errorf("encountered unknown prepared query for stream %d, ignoring", f.Stream)
	}

	if len(paths) > 1 {
		log.Debugf("batch query")
		return p.createBatchQuery(f, clientIPAddress, paths, toAstra)
	}

	// currently handles query and prepare statements that involve 'use, insert, update, delete, and truncate'
	fields := strings.Split(paths[0], "/")
	log.Debugf("$$$$$$$$ path %s split into following fields: %s", paths[0], fields)
	// NOTE: fields[0] is an empty element. the first meaningful element in the path is fields[1]
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
		log.Debugf("fields[%d]: %s", i, fields[i])
	}
	log.Debugf("fields: %s", fields)

	if len(fields) > 2 {
		log.Debugf("at least two fields found")
		switch fields[1] {
		case "prepare":
			log.Debugf("prepare statement query")
			return p.createPrepareQuery(fields[3], f, clientIPAddress, paths, toAstra)
		case "query":
			log.Debugf("statement query")
			queryType := query.Type(fields[2])
			log.Debugf("fields: %s", queryType)
			switch queryType {
			case query.USE:
				log.Debugf("query type use")
				return p.createUseQuery(fields[3], f, clientIPAddress, paths, toAstra)
			case query.INSERT, query.UPDATE, query.DELETE, query.TRUNCATE:
				log.Debugf("query type insert update delete or truncate")
				return p.createWriteQuery(fields[3], queryType, f, clientIPAddress, paths, toAstra)
			case query.SELECT:
				log.Debugf("query type select")
				p.Metrics.IncrementReads()
				return p.createReadQuery(fields[3], f, clientIPAddress, paths)
			}
		case "batch":
			return p.createBatchQuery(f, clientIPAddress, paths, toAstra)
		}
	} else {
		// path is '/opcode' case
		// FIXME: decide if there are any cases we need to handle here
		var destination query.DestinationCluster
		if toAstra {
			destination = query.ORIGIN_CASSANDRA
		} else {
			destination = query.ASTRA
		}

		log.Debugf("len(fields) < 2: %s, %v", fields, len(fields))
		switch fields[1] {
		case "execute":
			log.Debugf("Execute")
			// create and return execute query. this will just be of type EXECUTE, containing the raw request bytes. nothing else is needed.
			q = query.New(query.EXECUTE, f, clientIPAddress, destination, paths, "")
		case "register":
			log.Debugf("Register!!")
			q = query.New(query.REGISTER, f, clientIPAddress, destination, paths, "")
		case "options":
			log.Debugf("Options!!")
			q = query.New(query.OPTIONS, f, clientIPAddress, destination, paths, "")
		default:
			log.Debugf("Misc query")
			q = query.New(query.MISC, f, clientIPAddress, destination, paths, "")
		}

		//return p.executeOnAstra(q)
	}
	return q, nil
}

func (p* CloudgateProxy) trackStatementToBePrepared(q* query.Query, isWriteRequest bool) {
	// add the statement info for this query to the transient map of statements to be prepared
	stmtInfo := cqlparser.PreparedStatementInfo{Statement: q.Query, Keyspace: q.Keyspace, IsWriteStatement: isWriteRequest}
	// TODO is it necessary to lock in this case?
	p.lock.Lock()
	p.statementsBeingPrepared[q.Stream] = stmtInfo
	p.lock.Unlock()
}

func (p* CloudgateProxy) cachePreparedID(f *frame.Frame) {
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


// TODO is the fromClause really necessary here? can it be replaced or extracted otherwise?
func (p *CloudgateProxy) createPrepareQuery(fromClause string, f *frame.Frame, clientIPAddress string, paths []string, toAstra bool) (*query.Query, error) {
	var q *query.Query
	keyspace := extractKeyspace(fromClause)

	// Is the keyspace already in the table clause of the query, or do we need to add it
	if keyspace == "" {
		if toAstra {
			keyspace = p.currentAstraKeyspacePerClient[clientIPAddress]
		} else {
			keyspace = p.currentOriginCassandraKeyspacePerClient[clientIPAddress]
		}

		if keyspace == "" {
			return q, errors.New("invalid keyspace")
		}
	}

	q = query.New(query.PREPARE, f, clientIPAddress, query.GetDestinationCluster(toAstra), paths, keyspace)
	return q, nil
	//p.executeQueryAstra(q, []*migration.Table{table})
}

// TODO is the keyspace parameter really necessary here? can it be replaced or extracted otherwise?
func (p *CloudgateProxy) createUseQuery(keyspace string, f *frame.Frame, clientIPAddress string,
	paths []string, toAstra bool) (*query.Query, error) {
	var q *query.Query

	if keyspace == "" {
		log.Errorf("Missing or invalid keyspace!")
		return q, errors.New("missing or invalid keyspace")
	}

	// TODO perhaps replace this logic with call to function that parses keyspace name (sthg like extractKeyspace)
	// Cassandra assumes case-insensitive unless keyspace is encased in quotation marks
	if strings.HasPrefix(keyspace, "\"") && strings.HasSuffix(keyspace, "\"") {
		keyspace = keyspace[1 : len(keyspace)-1]
	} else {
		keyspace = strings.ToLower(keyspace)
	}

	p.lock.Lock()
	if toAstra {
		p.currentAstraKeyspacePerClient[clientIPAddress] = keyspace
	} else {
		p.currentOriginCassandraKeyspacePerClient[clientIPAddress] = keyspace
	}
	p.lock.Unlock()

	q = query.New(query.USE, f, clientIPAddress, query.GetDestinationCluster(toAstra), paths, keyspace)
	//p.executeQueryAstra(q, []*migration.Table{})

	return q, nil
}

// handleWriteQuery can handle QUERY and EXECUTE opcodes of type INSERT, UPDATE, DELETE, TRUNCATE
// TODO tidy up signature. it should be possible to pass fewer parameters with some refactoring
func (p *CloudgateProxy) createWriteQuery(fromClause string, queryType query.Type, f *frame.Frame,
	clientIPAddress string, paths []string, toAstra bool) (*query.Query, error) {
	var q *query.Query

	keyspace := extractKeyspace(fromClause)

	// Is the keyspace already in the table clause of the query, or do we need to add it
	if keyspace == "" {
		if f.Opcode == 0x0a { //if execute
			data := f.RawBytes
			idLen := binary.BigEndian.Uint16(data[9:11])
			preparedID := string(data[11:(11 + idLen)])
			if toAstra {
				keyspace = p.preparedStatementCache[preparedID].Keyspace
			} else {
				keyspace = p.preparedStatementCache[preparedID].Keyspace
			}
		} else {
			if toAstra {
				keyspace = p.currentAstraKeyspacePerClient[clientIPAddress]
			} else {
				keyspace = p.currentOriginCassandraKeyspacePerClient[clientIPAddress]
			}
		}

		if keyspace == "" {
			return q, errors.New("invalid keyspace")
		}
	}

	q = query.New(queryType, f, clientIPAddress, query.GetDestinationCluster(toAstra), paths, keyspace).UsingTimestamp()
	//p.executeQueryAstra(q, []*migration.Table{table})

	return q, nil
}

// Create select query. Selects only go to the origin cassandra cluster, so no toAstra flag is needed here.
func (p *CloudgateProxy) createReadQuery(fromClause string, f *frame.Frame, clientIPAddress string,
	paths []string) (*query.Query, error) {
	var q *query.Query

	keyspace := extractKeyspace(fromClause)

	// Is the keyspace already in the table clause of the query, or do we need to add it
	if keyspace == "" {
		if f.Opcode == 0x0a { //if execute
			data := f.RawBytes
			idLen := binary.BigEndian.Uint16(data[9:11])
			preparedID := string(data[11:(11 + idLen)])
			keyspace = p.preparedStatementCache[preparedID].Keyspace
		} else {
			keyspace = p.currentOriginCassandraKeyspacePerClient[clientIPAddress]
		}

		if keyspace == "" {
			return q, errors.New("invalid keyspace")
		}
	}

	q = query.New(query.SELECT, f, clientIPAddress, query.ORIGIN_CASSANDRA, paths, keyspace).UsingTimestamp()
	//p.executeQueryAstra(q, []*migration.Table{table})

	return q, nil
}

func (p *CloudgateProxy) createBatchQuery(f *frame.Frame, clientIPAddress string, paths []string, toAstra bool) (*query.Query, error) {
	var q *query.Query
	var keyspace string

	for _, path := range paths {
		fields := strings.Split(path, "/")
		keyspace = extractKeyspace(fields[3])
		if keyspace == "" {
			if toAstra {
				keyspace = p.currentAstraKeyspacePerClient[clientIPAddress]
			} else {
				keyspace = p.currentOriginCassandraKeyspacePerClient[clientIPAddress]
			}
			if keyspace == "" {
				return q, fmt.Errorf("invalid keyspace for batch query (%s, %d)", clientIPAddress, f.Stream)
			}
		}
	}

	q = query.New(query.BATCH, f, clientIPAddress, query.GetDestinationCluster(toAstra), paths, keyspace).UsingTimestamp()
	//p.executeQueryAstra(q, tableList)
	return q, nil
}

func (p *CloudgateProxy) createExecuteQuery(f *frame.Frame, clientIPAddress string, paths []string, toAstra bool) (*query.Query, error) {
	//TODO implement
	var q *query.Query

	return q, nil
}


// Given a FROM argument, extract the table name
// ex: table, keyspace.table, keyspace.table;, keyspace.table(, etc..
// TODO currently parsing keyspace and table, but table is probably not necessary so it should be taken out
// TODO note that table name is NOT returned
func extractKeyspace(fromClause string) string {
	fromClause = strings.TrimSuffix(fromClause, ";")

	keyspace := ""
	tableName := fromClause

	// Separate keyspace & tableName if together
	if i := strings.IndexRune(fromClause, '.'); i != -1 {
		keyspace = fromClause[:i]
		tableName = fromClause[i+1:]
	}

	// Remove column names if part of an INSERT query: ex: TABLE(col, col)
	if i := strings.IndexRune(tableName, '('); i != -1 {
		tableName = tableName[:i]
	}

	// Make keyspace and tablename lowercase if necessary.
	// Otherwise, leave case as-is but trim quotations marks
	if strings.HasPrefix(keyspace, "\"") && strings.HasSuffix(keyspace, "\"") {
		keyspace = strings.TrimPrefix(keyspace, "\"")
		keyspace = strings.TrimSuffix(keyspace, "\"")
	} else {
		keyspace = strings.ToLower(keyspace)
	}

	if strings.HasPrefix(tableName, "\"") && strings.HasSuffix(tableName, "\"") {
		tableName = strings.TrimPrefix(tableName, "\"")
		tableName = strings.TrimSuffix(tableName, "\"")
	} else {
		tableName = strings.ToLower(tableName)
	}

	return keyspace
}
