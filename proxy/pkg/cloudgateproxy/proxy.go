package cloudgateproxy

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/jpillora/backoff"
	"github.com/riptano/cloud-gate/proxy/pkg/auth"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"github.com/riptano/cloud-gate/proxy/pkg/cqlparser"
	"github.com/riptano/cloud-gate/proxy/pkg/frame"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	"github.com/riptano/cloud-gate/proxy/pkg/query"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// TODO: Make these configurable
	maxQueryRetries = 5
	queryTimeout    = 2 * time.Second

	cassHdrLen = 9
	cassMaxLen = 256 * 1024 * 1024 // 268435456 // 256 MB, per spec
)

// [Alice] for opcode values and meaning see cqlparser.go

type CloudgateProxy struct {

	Conf *config.Config

	originCassandraIP string
	astraIP string

	originCassandraConnections map[string]net.Conn
	astraConnections map[string]net.Conn
	connectionLocks map[string]*sync.RWMutex
	lock *sync.RWMutex

	// Channel signalling that the proxy is now ready to process queries
	ReadyChan chan struct{}

	// Channel to signal to coordinator that there are no more open connections to the Client's Database
	// and that the coordinator can redirect Envoy to point directly to Astra without any negative side effects
	//TODO this will probably go in the end but it is here to make the main method work for the moment
	ReadyForRedirect chan struct{}

	listeners []net.Listener

	// Maps containing the prepared queries (raw bytes) keyed on prepareId
	// One per cluster as the statements are independently prepared on each cluster
	preparedQueryInfoOriginCassandra  map[string]cqlparser.PreparedQueryInfo
	preparedQueryInfoAstra  map[string]cqlparser.PreparedQueryInfo
	// mapping between originCassandraPreparedIds and astraPreparedIds.
	// This is necessary because the client driver will only know the originCassandraPreparedId and will use this id
	// to refer to already-prepared statements: the proxy will need to figure out whether that statement was
	// already prepared on Astra too and:
	//  - if so, use the corresponding id
	//  - if not, fetch the raw bytes and issue a prepare request to Astra for it.
	astraPreparedIdsByOriginPreparedIds map[string]string

	// TODO not sure the current keyspace thing is necessary but leaving it in for now
	currentOriginCassandraKeyspacePerClient map[string]string			// Keeps track of the current keyspace that each CLIENT is in
	currentAstraKeyspacePerClient map[string]string		// Keeps track of the current keyspace that the PROXY is in while connected to Astra

	shutdown bool

	Metrics *metrics.Metrics
}

// Start starts up the proxy. The proxy creates a connection with the Astra Database,
// creates a communication channel with the migration service and then begins listening
// on $PROXY_QUERY_PORT for queries to the database.
func (p *CloudgateProxy) Start() error {
	p.reset()
	p.checkDatabaseConnections()

	//log.Debugf("wait for ReadyChan")
	//<-p.ReadyChan 								// [Alice] this signals that the proxy is ready to receive
	//log.Debugf("received ReadyChan")


	err := p.listen(p.Conf.ProxyQueryPort, p.listenOnClientConnection)
	if err != nil {
		return err
	}

	log.Infof("Proxy connected and ready to accept queries on port %d", p.Conf.ProxyQueryPort)
	return nil
}

// TODO: Is there a better way to check that we can connect to both databases?
func (p *CloudgateProxy) checkDatabaseConnections() {
	// Wait until the source database is up and ready to accept TCP connections.
	originCassandra := establishConnection(p.originCassandraIP)
	originCassandra.Close()

	// Wait until the Astra database is up and ready to accept TCP connections.
	astra := establishConnection(p.astraIP)
	astra.Close()
}

// listen creates a listener on the passed in port argument, and every connection
// that is received over that port is handled by the passed in handler function.
func (p *CloudgateProxy) listen(port int, handler func(net.Conn) error) error {
	log.Debugf("Proxy connected and ready to accept queries on port %d", port)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	p.lock.Lock()
	p.listeners = append(p.listeners, l)
	p.lock.Unlock()

	go func() {
		defer l.Close()
		for {
			conn, err := l.Accept()
			if err != nil {
				if p.shutdown {
					log.Debugf("Shutting down listener on port %d", port)
					return
				}
				log.Error(err)
				continue
			}
			// [Alice] long-lived goroutine that handles any request coming over this connection
			// there is a goroutine call for each connection made by a client
			go handler(conn)
		}
	}()

	return nil
}

// [Alice] long-lived method that will run indefinitely and spawn a new goroutine for each
// TODO infinite for loop. receive requests, determine type and if read call handleReadRequest, otherwise call handleWriteRequest
// TODO both these calls are launched as goroutines
func (p *CloudgateProxy) listenOnClientConnection(clientAppConn net.Conn) error {

	log.Debugf("listenOnClientConnection")

	p.initialize(clientAppConn)
	clientApplicationIP := clientAppConn.RemoteAddr().String()
	authenticated := false

	// Main listening loop
	// [Alice] creating this outside the loop to avoid creating a slice every time
	// which would be heavy on the GC (see https://medium.com/go-walkthrough/go-walkthrough-io-package-8ac5e95a9fbd)
	frameHeader := make([]byte, cassHdrLen)
	for {

		/*  - parse frame
		    - parse request
		    - create query object
		    - get type
		    - determine if read or write and also if prepared statement is involved
		*/
	// TODO the content of this for loop should become a goroutine, so we handle all incoming requests concurrently
		f, err := parseFrame(clientAppConn, frameHeader, p.Metrics)
		//if err != nil {
		//	// the error has been logged by the parseFrame function, we just want to skip this frame and continue
		//	continue
		//}

		if err != nil {
			if err != io.EOF {
				log.Debugf("%s disconnected", clientAppConn)
			} else {
				log.Debugf("error reading frame header")
				log.Error(err)
			}
			return err
		}

		if f.Direction != 0 {
			log.Debugf("Unexpected frame direction %d", f.Direction) // [Alice]
			log.Error(errors.New("unexpected direction: frame not from client to db - skipping frame"))
			continue
		}

		if !authenticated {
			log.Debugf("not authenticated")
			// Handle client authentication
			authenticated, err = p.handleStartupFrame(f, clientAppConn)
			if err != nil {
				log.Error(err)
			}
			log.Debugf("authenticated? %t", authenticated)
			continue
		}

		// CassandraParseRequest returns an array of paths (just strings) with the format "/opcode/action/table"
		// one path if a simple request, multiple paths if a batch
		// parsing requests is cluster-specific because it handles prepared statements from the target cluster's cache
		// first of all parse the request for the OriginCassandra cluster, as all requests have to be forwarded to it anyway
		// TODO review the whole flow for handling of prepared statements!

		originCassandraPaths, isWriteRequest, err := cqlparser.CassandraParseRequest(p.preparedQueryInfoOriginCassandra, f.RawBytes)
		if err != nil {
			return err
		}

		log.Debugf("parsed request for OriginCassandra, writeRequest? %t, resulting path(s) %v", isWriteRequest, originCassandraPaths)
		originCassandraQuery, err := p.createQuery(f, clientApplicationIP, originCassandraPaths, false)
		if err != nil {
			return err
		}

		// TODO handle prepared statement flow here

		go p.forwardToCluster(p.originCassandraConnections[clientApplicationIP], originCassandraQuery)
		log.Debugf("Back from forwardToCluster")
		// if it is a write request (also a batch involving at least one write) then also parse it for the Astra cluster
		if isWriteRequest {
			log.Debugf("Write request, now parsing it for Astra")
			astraPaths, _, err := cqlparser.CassandraParseRequest(p.preparedQueryInfoAstra, f.RawBytes)
			if err != nil {
				return err
			}
			log.Debugf("parsed write request for Astra, resulting path(s) %v", isWriteRequest, astraPaths)
			astraQuery, err := p.createQuery(f, clientApplicationIP, astraPaths, true)
			if err != nil {
				return err
			}
			go p.forwardToCluster(p.astraConnections[clientApplicationIP], astraQuery)
		}


	}
}
//	Function that initializes everything when a new client connects to the proxy.
func (p *CloudgateProxy) initialize(clientAppConn net.Conn) {
	originCassandraConn := establishConnection(p.originCassandraIP)
	astraConn := establishConnection(p.astraIP)

	clientApplicationIP := clientAppConn.RemoteAddr().String()
	log.Debugf("clientApplicationIP %s", clientApplicationIP) // [Alice]

	p.lock.Lock()
	p.originCassandraConnections[clientApplicationIP] = originCassandraConn
	p.astraConnections[clientApplicationIP] = astraConn
	p.connectionLocks[clientApplicationIP] = &sync.RWMutex{}
	p.lock.Unlock()
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
		log.Debugf("len(fields) < 2: %s, %v", fields, len(fields))
		if fields[1] == "register" {
			log.Debugf("Register!!")
			q = query.New(query.REGISTER, f, clientIPAddress, paths)
		} else {
			log.Debugf("Query to be ignored??")
			q = query.New(query.MISC, f, clientIPAddress, paths)
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

	q = query.New(query.PREPARE, f, clientIPAddress, paths)
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

	q = query.New(query.USE, f, clientIPAddress, paths)
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

	q = query.New(queryType, f, clientIPAddress, paths).UsingTimestamp()
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

	q = query.New(query.SELECT, f, clientIPAddress, paths).UsingTimestamp()
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

	q = query.New(query.BATCH, f, clientIPAddress, paths).UsingTimestamp()
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

func (p *CloudgateProxy) forwardToCluster(clusterConn net.Conn, query *query.Query) {
	// TODO submits the request on cluster connection (initially single connection to keep it simple, but it will probably have to be a pool)
	// creates a channel on which it will send the outcome (outcomeChan). this will be returned to the caller (handleWriteRequest or  handleReadRequest)
	// adds an entry to a pendingRequestMap keyed on streamID and whose value is a channel. this channel is used by the dequeuer to communicate the response back to this goroutine
	// it is this goroutine that has to receive the response, so it can enforce the timeout in case of connection disruption
	log.Debugf("Forwarding query of type %v with opcode %v and path %v for stream %v", query.Type, query.Opcode, query.Paths[0], query.Stream)
	log.Debugf("Forwarding this query to cluster %v", clusterConn.RemoteAddr().String())
}

func (p *CloudgateProxy) dequeueResponsesFromClusterConnection() {
	// TODO infinite loop that reads responses out of the cluster connection. make sure it blocks on something to avoid using up CPU for no reason
	// it looks for the channel for that streamID in the request map and puts the response on it
}



// Establishes a TCP connection with the passed in IP. Retries using exponential backoff.
func establishConnection(ip string) net.Conn {
	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: false,
	}

	log.Debugf("Attempting to connect to %s...", ip)
	for {
		conn, err := net.Dial("tcp", ip)
		if err != nil {
			nextDuration := b.Duration()
			log.Errorf("Couldn't connect to %s, retrying in %s...", ip, nextDuration.String())
			time.Sleep(nextDuration)
			continue
		}
		log.Infof("Successfully established connection with %s", conn.RemoteAddr())
		return conn
	}
}

// handleStartupFrame will check the frame opcodes to determine what startup actions to take
// The process, at a high level, is that the proxy directly tunnels startup communications
// to Astra (since the client logs on with Astra credentials), and then the proxy manually
// initiates startup with the client's old database
func (p *CloudgateProxy) handleStartupFrame(f *frame.Frame, clientAppConn net.Conn) (bool, error) {
	clientAppIP := clientAppConn.RemoteAddr().String()
	astraSession := p.getAstraConnection(clientAppIP)

	switch f.Opcode {
		// OPTIONS
		case 0x05:
			// forward OPTIONS to Astra
			// [Alice] this call sends the options message and deals with the response (supported / not supported)
			// this exchange does not authenticate yet
			// is this also where the native protocol version is negotiated?
			log.Debugf("Handling OPTIONS message")
			err := auth.HandleOptions(clientAppConn, astraSession, f.RawBytes)
			if err != nil {
				return false, fmt.Errorf("client %s unable to negotiate options with %s",
					clientAppIP, astraSession.RemoteAddr().String())
			}
			log.Debugf("OPTIONS message successfully handled")
			// TODO what does this method return here? it should return false

		// STARTUP
		case 0x01:
			log.Debugf("Handling STARTUP message")
			err := auth.HandleAstraStartup(clientAppConn, astraSession, f.RawBytes)
			if err != nil {
				return false, err
			}
			log.Debugf("STARTUP message successfully handled on Astra, now proceeding with OriginCassandra")
			err = auth.HandleOriginCassandraStartup(clientAppConn, p.getOriginCassandraConnection(clientAppIP), f.RawBytes,
													p.Conf.SourceUsername, p.Conf.SourcePassword)
			//err = auth.HandleAstraStartup(client, sourceDB, f.RawBytes)
			if err != nil {
				return false, err
			}
			log.Debugf("STARTUP message successfully handled on OriginCassandra")
			return true, nil
	}
	return false, fmt.Errorf("received non STARTUP or OPTIONS query from unauthenticated client %s", clientAppIP)
}

func (p *CloudgateProxy) getAstraConnection(client string) net.Conn {
	p.lock.Lock()
	defer p.lock.Unlock()

	//for _, connection := range p.astraConnections {
	//	log.Debugf("connection local: %v",connection.LocalAddr().String());
	//	log.Debugf("connection remote: %v", connection.RemoteAddr().String());
	//}

	return p.astraConnections[client]
}

func (p *CloudgateProxy) getOriginCassandraConnection(client string) net.Conn {
	p.lock.Lock()
	defer p.lock.Unlock()

	//for _, session := range p.astraSessions {
	//	log.Debugf("session local: %v",session.LocalAddr().String());
	//	log.Debugf("session remote: %v", session.RemoteAddr().String());
	//}

	return p.originCassandraConnections[client]
}

// reset will reset all context within the proxy service
//p.queryResponses = make(map[string]map[uint16]chan bool)
func (p *CloudgateProxy) reset() {
	p.ReadyChan = make(chan struct{})
	//p.ShutdownChan = make(chan struct{})
	p.shutdown = false
	p.listeners = []net.Listener{}
	p.ReadyForRedirect = make(chan struct{})
	p.lock = &sync.RWMutex{}
	p.Metrics = metrics.New(p.Conf.ProxyMetricsPort)
	p.Metrics.Expose()
	p.originCassandraIP = fmt.Sprintf("%s:%d", p.Conf.SourceHostname, p.Conf.SourcePort)
	p.astraIP = fmt.Sprintf("%s:%d", p.Conf.AstraHostname, p.Conf.AstraPort)
	p.originCassandraConnections = make(map[string]net.Conn)
	p.astraConnections = make(map[string]net.Conn)
	p.connectionLocks = make(map[string]*sync.RWMutex)
	p.preparedQueryInfoOriginCassandra = make(map[string]cqlparser.PreparedQueryInfo)
	p.preparedQueryInfoAstra = make(map[string]cqlparser.PreparedQueryInfo)
	p.astraPreparedIdsByOriginPreparedIds =  make(map[string]string)
	p.currentOriginCassandraKeyspacePerClient =  make(map[string]string)
	p.currentAstraKeyspacePerClient =  make(map[string]string)
	//p.outstandingQueries = make(map[string]map[uint16]*frame.Frame)
	//p.outstandingUpdates = make(map[string]chan bool)
	//p.migrationComplete = p.Conf.MigrationComplete
	//p.preparedQueries = &cqlparser.PreparedQueries{
	//	PreparedQueryPathByStreamID:   make(map[uint16]string),
	//	PreparedQueryPathByPreparedID: make(map[string]string),
	//}
	//p.preparedIDs = make(map[uint16]string)
	//p.mappedPreparedIDs = make(map[string]string)
	//p.outstandingPrepares = make(map[uint16][]byte)
	//p.prepareIDToKeyspace = make(map[string]string)
}
