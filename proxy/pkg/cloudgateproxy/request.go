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
	"time"
)

// Method that handles a request.
// This is called as a goroutine every time a valid frame is received and it does not contain an authentication request
// One goroutine for each request, so each request is executed concurrently
func (p* CloudgateProxy) handleRequest(f *frame.Frame, clientApplicationIP string) error {

	// CassandraParseRequest returns an array of paths (just strings) with the format "/opcode/action/table"
	// one path if a simple request, multiple paths if a batch
	// parsing requests is cluster-specific because it handles prepared statements from the target cluster's cache
	// first of all parse the request for the OriginCassandra cluster, as all requests have to be forwarded to it anyway
	// TODO review the whole flow for handling of prepared statements!

	originCassandraPaths, isWriteRequest, isServiceRequest, err := cqlparser.CassandraParseRequest(p.preparedQueryInfoOriginCassandra, f.RawBytes)
	if err != nil {
		return err
	}

	log.Debugf("parsed request for OriginCassandra, writeRequest? %t, serviceRequest? %t, resulting path(s) %v", isWriteRequest, isServiceRequest, originCassandraPaths)
	originCassandraQuery, err := p.createQuery(f, clientApplicationIP, originCassandraPaths, false)
	if err != nil {
		return err
	}
	log.Debugf("Query created")
	// TODO handle prepared statement flow here

	var responseFromOriginalCassandra *frame.Frame
	var responseFromAstra *frame.Frame

	// request is forwarded to each cluster in a separate goroutine so this happens concurrently
	responseFromOriginalCassandraChan := make(chan *frame.Frame)
	go p.forwardToCluster(originCassandraQuery, isServiceRequest, responseFromOriginalCassandraChan)

	log.Debugf("Launched forwardToCluster (OriginCassandra) goroutine")
	// if it is a write request (also a batch involving at least one write) then also parse it for the Astra cluster
	if isWriteRequest {
		log.Debugf("Write request, now parsing it for Astra")
		astraPaths, _, isServiceRequest, err := cqlparser.CassandraParseRequest(p.preparedQueryInfoAstra, f.RawBytes)
		if err != nil {
			return err
		}
		log.Debugf("parsed write request for Astra, serviceRequest? %t, resulting path(s) %v", isServiceRequest, astraPaths)
		astraQuery, err := p.createQuery(f, clientApplicationIP, astraPaths, true)
		if err != nil {
			return err
		}
		responseFromAstraChan := make(chan *frame.Frame)
		go p.forwardToCluster(astraQuery, isServiceRequest, responseFromAstraChan)
		// we only wait for the astra response if the request was sent to astra. this is why the receive from this channel is within the if block
		responseFromAstra = <- responseFromAstraChan
	}

	// wait for OC response in any case
	responseFromOriginalCassandra = <- responseFromOriginalCassandraChan

	if isWriteRequest {
		// TODO this is just to anchor a breakpoint, remove
		log.Debugf("Both responses to query %s received", originCassandraQuery.Type)
	}


	if isWriteRequest {
		log.Debugf("Write request: aggregating the responses received - OC: %d && Astra: %d", responseFromOriginalCassandra.Opcode, responseFromAstra.Opcode)
		p.responseForClientChannels[clientApplicationIP] <- aggregateResponses(responseFromOriginalCassandra, responseFromOriginalCassandra)
	} else {
		log.Debugf("Non-write request: just returning the response received from OC: %d", responseFromOriginalCassandra.Opcode)
		p.responseForClientChannels[clientApplicationIP] <- responseFromOriginalCassandra.RawBytes
	}

	return nil
}

func aggregateResponses(responseFromOriginalCassandra *frame.Frame, responseFromAstra *frame.Frame) []byte{
	var aggregatedResponse []byte

	log.Debugf("Aggregating responses. OC opcode %d, Astra opcode %d", responseFromOriginalCassandra.Opcode, responseFromAstra.Opcode)

	//	if both responses are a success OR both responses are a failure --> return responseFromOC
	if (isResponseSuccessful(responseFromOriginalCassandra) && isResponseSuccessful(responseFromAstra)) ||
		(!isResponseSuccessful(responseFromOriginalCassandra) && !isResponseSuccessful(responseFromAstra)) {
		log.Debugf("Aggregated response: both successes or both failures, sending back OC's response with opcode %d", responseFromOriginalCassandra.Opcode)
		return responseFromOriginalCassandra.RawBytes
	}

	// if either response is a failure, the failure "wins" --> return the failed response
	if !isResponseSuccessful(responseFromOriginalCassandra) {
		log.Debugf("Aggregated response: failure only on OC, sending back OC's response with opcode %d", responseFromOriginalCassandra.Opcode)
		return responseFromOriginalCassandra.RawBytes
	} else {
		log.Debugf("Aggregated response: failure only on Astra, sending back Astra's response with opcode %d", responseFromOriginalCassandra.Opcode)
		return responseFromAstra.RawBytes
	}

	return aggregatedResponse
}

func isResponseSuccessful(f *frame.Frame) bool {
	return f.Opcode == 0x08
}

// Simple function that reads data from a connection and builds a frame
func parseFrame(src net.Conn, frameHeader []byte, metrics *metrics.Metrics) (*frame.Frame, error) {
	sourceAddress := src.RemoteAddr().String()

	// [Alice] read the frameHeader, whose length is constant (9 bytes), and put it into this slice
	log.Debugf("read frame header from src %s", sourceAddress)
	bytesRead, err := io.ReadFull(src, frameHeader)
	if err != nil {
		if err != io.EOF {
			log.Debugf("%s disconnected", sourceAddress)
		} else {
			log.Debugf("error reading frame header")
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
	log.Debugf("(from %s): %v", src.RemoteAddr(), string(data))
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
		return q, fmt.Errorf("encountered unknown prepared query for stream %d, ignoring", f.Stream)
	}

	if len(paths) > 1 {
		log.Debugf("batch query")
		return p.createBatchQuery(f, clientIPAddress, paths, toAstra)
	}

	// currently handles query and prepare statements that involve 'use, insert, update, delete, and truncate'
	fields := strings.Split(paths[0], "/")
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
	}
	log.Debugf("fields: %s", fields)
	if len(fields) > 2 {
		switch fields[1] {
		//case "statement prepare":
		case "prepare":
			log.Debugf("prepare statement query")
			return p.createPrepareQuery(fields[3], f, clientIPAddress, paths, toAstra)
		case "query", "execute":
			log.Debugf("statement query or execute")
			if fields[1] == "execute" {
				log.Debugf("execute")
				// TODO ***** THIS STILL NEEDS TO BE REVIEWED ****
				// TODO the EXECUTE case still has to be handled - see what an EXECUTE query looks like
				//err = p.updatePrepareID(f)
				//if err != nil {
				//	log.Error(err)
				//	return err
				//}
			}

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
				// TODO select must go to Origin only
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
		if fields[1] == "register" {
			log.Debugf("Register!!")
			q = query.New(query.REGISTER, f, clientIPAddress, destination, paths)
		} else {
			log.Debugf("Query to be ignored??")
			q = query.New(query.MISC, f, clientIPAddress, destination, paths)
		}
		//return p.executeOnAstra(q)
	}
	return q, nil
}

// updatePrepareID converts between source prepareID to astraPrepareID to ensure consistency
// between source and astra databases
// TODO sort this out
// this should be called when the responses to a PREPARE request have been received from originCassandra and Astra, so both preparedIDs are available
func (p *CloudgateProxy) updatePrepareID(f *frame.Frame) error {
	log.Debugf("updatePrepareID")
	data := f.RawBytes
	idLength := binary.BigEndian.Uint16(data[9:11])
	originCassandraPreparedID := data[11 : 11+idLength]

	// Ensures that the mapping of source preparedID to astraPreparedID has finished executing
	// TODO: do this in a better way
	for {
		_, ok := p.astraPreparedIdsByOriginPreparedIds[string(originCassandraPreparedID)]
		if ok {
			break
		}
		log.Debugf("sleeping, map %v, prepareID %v", p.astraPreparedIdsByOriginPreparedIds, string(originCassandraPreparedID))
		time.Sleep(100 * time.Millisecond)
	}

	if newPreparedID, ok := p.astraPreparedIdsByOriginPreparedIds[string(originCassandraPreparedID)]; ok {
		before := make([]byte, cassHdrLen)
		copy(before, data[:cassHdrLen])
		after := data[11+idLength:]

		newLen := make([]byte, 2)
		binary.BigEndian.PutUint16(newLen, uint16(len(newPreparedID)))

		newBytes := before
		newBytes = append(before, newLen...)
		newBytes = append(newBytes, []byte(newPreparedID)...)
		newBytes = append(newBytes, after...)

		f.RawBytes = newBytes
		binary.BigEndian.PutUint32(f.RawBytes[5:9], uint32(len(newBytes)-cassHdrLen))
		f.Length = uint32(len(newBytes) - cassHdrLen)

		return nil
	}

	return fmt.Errorf("no mapping for source prepared id %s to astra prepared id found", originCassandraPreparedID)
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

	q = query.New(query.PREPARE, f, clientIPAddress, query.GetDestinationCluster(toAstra), paths)
	return q, nil
	//p.executeQueryAstra(q, []*migration.Table{table})
}

// TODO is the keyspace parameter really necessary here? can it be replaced or extracted otherwise?
func (p *CloudgateProxy) createUseQuery(keyspace string, f *frame.Frame, clientIPAddress string,
	paths []string, toAstra bool) (*query.Query, error) {
	var q *query.Query

	if keyspace == "" {
		return q, errors.New("invalid keyspace")
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

	q = query.New(query.USE, f, clientIPAddress, query.GetDestinationCluster(toAstra), paths)
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
				keyspace = p.preparedQueryInfoAstra[preparedID].Keyspace
			} else {
				keyspace = p.preparedQueryInfoOriginCassandra[preparedID].Keyspace
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

	q = query.New(queryType, f, clientIPAddress, query.GetDestinationCluster(toAstra), paths,).UsingTimestamp()
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
			keyspace = p.preparedQueryInfoOriginCassandra[preparedID].Keyspace
		} else {
			keyspace = p.currentOriginCassandraKeyspacePerClient[clientIPAddress]
		}

		if keyspace == "" {
			return q, errors.New("invalid keyspace")
		}
	}

	q = query.New(query.SELECT, f, clientIPAddress, query.ORIGIN_CASSANDRA, paths).UsingTimestamp()
	//p.executeQueryAstra(q, []*migration.Table{table})

	return q, nil
}

func (p *CloudgateProxy) createBatchQuery(f *frame.Frame, clientIPAddress string, paths []string, toAstra bool) (*query.Query, error) {
	var q *query.Query

	for _, path := range paths {
		fields := strings.Split(path, "/")
		keyspace := extractKeyspace(fields[3])
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

	q = query.New(query.BATCH, f, clientIPAddress, query.GetDestinationCluster(toAstra), paths).UsingTimestamp()
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
