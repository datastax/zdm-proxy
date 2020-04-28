package filter

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jpillora/backoff"

	"cloud-gate/config"
	"cloud-gate/migration/migration"
	"cloud-gate/proxy/cqlparser"
	"cloud-gate/updates"

	log "github.com/sirupsen/logrus"
)

const (
	// TODO: Finalize queue size to use
	queueSize = 1000

	cassHdrLen = 9
	cassMaxLen = 268435456 // 256 MB, per spec

	maxQueryRetries = 5
	queryTimeout    = 2 * time.Second
)

type CQLProxy struct {
	Conf *config.Config

	sourceIP           string
	astraIP            string
	astraSession       net.Conn
	migrationServiceIP string
	migrationSession   net.Conn

	listeners []net.Listener

	queues         map[string]map[string]chan *Query
	queueLocks     map[string]map[string]*sync.Mutex
	queueSizes     map[string]map[string]int
	tablePaused    map[string]map[string]bool
	queryResponses map[uint16]chan bool
	lock           *sync.Mutex

	outstandingUpdates map[string]chan bool
	migrationStatus    *migration.Status
	migrationComplete  bool

	// Channels for dealing with updates from migration service
	MigrationStart chan *migration.Status
	MigrationDone  chan struct{}

	// Channel to signal when the Proxy should stop all forwarding and close all connections
	ShutdownChan chan struct{}
	shutdown     bool

	// Channel signalling that the proxy is now ready to process queries
	ReadyChan chan struct{}

	// Channel to signal to coordinator that there are no more open connections to the Client's Database
	// and that the coordinator can redirect Envoy to point directly to Astra without any negative side effects
	ReadyForRedirect chan struct{}

	// Holds prepared queries by StreamID and by PreparedID
	preparedQueries *cqlparser.PreparedQueries

	// Keeps track of the current keyspace queries are being ran in per connection
	Keyspaces map[string]string

	// Metrics
	Metrics Metrics
}

// Start starts up the proxy. The proxy creates a connection with the Astra Database,
// creates a communication channel with the migration service and then begins listening
// on $PROXY_QUERY_PORT for queries to the database.
func (p *CQLProxy) Start() error {
	p.reset()

	var err error
	// Attempt to connect to astra database using given credentials
	p.astraSession = p.establishConnection("Astra", "tcp", p.astraIP)
	p.migrationSession = p.establishConnection("Migration Service", "tcp", p.migrationServiceIP)

	go p.statusLoop()
	go p.runMetrics()

	err = p.listen(p.Conf.ProxyCommunicationPort, p.handleMigrationCommunication)
	if err != nil {
		return err
	}

	<-p.ReadyChan
	err = p.listen(p.Conf.ProxyQueryPort, p.handleClientConnection)
	if err != nil {
		return err
	}

	log.Infof("Proxy started and ready to accept queries on port %d", p.Conf.ProxyQueryPort)
	return nil
}

func (p *CQLProxy) establishConnection(name string, network string, port string) net.Conn {
	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: false,
	}

	log.Debugf("Attempting to connect to %s at %s...", name, port)
	for {
		conn, err := net.Dial(network, port)
		if err != nil {
			nextDuration := b.Duration()
			log.Debugf("Couldn't connect to %s, retrying in %s...", name, nextDuration.String())
			time.Sleep(nextDuration)
			continue
		}
		log.Infof("Successfully established connection with %s", name)
		return conn
	}
}

// TODO: May just get rid of this entire function, as it can all be handled by p.handleUpdate() directly
// statusLoop listens for updates to the overall migration process and processes them accordingly
func (p *CQLProxy) statusLoop() {
	log.Debugf("Migration Complete: %t", p.migrationComplete)

	if !p.migrationComplete {
		log.Info("Proxy waiting for migration start signal.")
		for {
			select {
			case status := <-p.MigrationStart:
				p.loadMigrationInfo(status)

			case <-p.MigrationDone:
				log.Info("Migration Complete. Directing all new connections to Astra Database.")
				p.migrationComplete = true
				p.checkRedirect()

			case <-p.ShutdownChan:
				p.Shutdown()
				return
			}
		}
	}
}

// loadMigrationInfo initializes all the maps needed for the proxy, using
// a migration.Status object to get all the keyspaces and tables
func (p *CQLProxy) loadMigrationInfo(status *migration.Status) {
	p.migrationStatus = status

	for keyspace, tables := range status.Tables {
		p.queues[keyspace] = make(map[string]chan *Query)
		p.queueLocks[keyspace] = make(map[string]*sync.Mutex)
		p.queueSizes[keyspace] = make(map[string]int)
		p.tablePaused[keyspace] = make(map[string]bool)
		for tableName := range tables {
			p.queues[keyspace][tableName] = make(chan *Query, queueSize)
			p.queueLocks[keyspace][tableName] = &sync.Mutex{}

			go p.consumeQueue(keyspace, tableName)
		}
	}

	p.ReadyChan <- struct{}{}
}

// listen creates a listener on the passed in port argument, and every connection
// that is received over that port is handled by the passed in handler function.
func (p *CQLProxy) listen(port int, handler func(net.Conn)) error {
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
			go handler(conn)
		}
	}()

	return nil
}

// handleClientConnection takes a connection from the client and begins forwarding
// packets to/from the client's old DB, and mirroring writes to the Astra DB.
func (p *CQLProxy) handleClientConnection(conn net.Conn) {
	hostname := p.sourceIP
	if p.migrationComplete {
		hostname = p.astraIP
	}

	dst, err := net.Dial("tcp", hostname)
	if err != nil {
		log.Error(err)
		return
	}

	// Begin two way packet forwarding
	go p.forward(conn, dst)
	go p.forward(dst, conn)
}

func (p *CQLProxy) handleMigrationCommunication(conn net.Conn) {
	defer conn.Close()

	// TODO: change buffer size
	buf := make([]byte, 0xfffffff)
	for {
		bytesRead, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				log.Error(err)
				return
			} else {
				continue
			}
		}

		b := buf[:bytesRead]
		log.Debug(string(b))
		var update updates.Update
		err = json.Unmarshal(b, &update)
		if err != nil {
			log.Error(err)
			continue
		}

		var resp []byte
		err = p.handleUpdate(&update)
		if err != nil {
			resp = update.Failure(err)
		} else {
			resp = update.Success()
		}

		_, err = p.migrationSession.Write(resp)
		if err != nil {
			log.Error(err)
		}
	}

}

func (p *CQLProxy) handleUpdate(update *updates.Update) error {
	switch update.Type {
	case updates.Start:
		var status migration.Status
		err := json.Unmarshal(update.Data, &status)
		if err != nil {
			return errors.New("unable to unmarshal json")
		}

		p.MigrationStart <- &status
	case updates.TableUpdate:
		var tableUpdate migration.Table
		err := json.Unmarshal(update.Data, &tableUpdate)
		if err != nil {
			return errors.New("unable to unmarshal json")
		}

		if table, ok := p.migrationStatus.Tables[tableUpdate.Keyspace][tableUpdate.Name]; ok {
			if p.tablePaused[table.Keyspace][table.Name] && tableUpdate.Step == migration.LoadingDataComplete {
				p.startTable(table.Keyspace, table.Name)
			}
			table.Update(&tableUpdate)
		} else {
			return fmt.Errorf("table %s.%s does not exist", tableUpdate.Keyspace, tableUpdate.Name)
		}

	case updates.Complete:
		p.MigrationDone <- struct{}{}
	case updates.Shutdown:
		p.ShutdownChan <- struct{}{}
	case updates.Success, updates.Failure:
		p.lock.Lock()
		if resp, ok := p.outstandingUpdates[update.ID]; ok {
			resp <- update.Type == updates.Success
		}
		p.lock.Unlock()
	}

	return nil
}

// --Unused for now, but will be used in the near future--
// sendPriorityUpdate will send a tableUpdate to the migration service with an
// updated priority value for the table. We do this as if there is a table
// with a very large queue, we want that to be migrated ASAP, so we communicate
// the priority of a table's migration
func (p *CQLProxy) sendPriorityUpdate(table *migration.Table) error {
	marshaledTable, err := json.Marshal(table)
	if err != nil {
		return err
	}

	update := updates.New(updates.TableUpdate, marshaledTable)
	marshaledUpdate, err := json.Marshal(update)
	if err != nil {
		return err
	}

	b := &backoff.Backoff{
		Min:    200 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: false,
	}

	go func() {
		for {
			_, err = p.migrationSession.Write(marshaledUpdate)
			if err != nil {
				duration := b.Duration()
				log.Debugf("Unable to send update %s to migration service. Retrying in %s...",
					update.ID, duration.String())
				time.Sleep(duration)
			}
		}
	}()

	return nil
}

func (p *CQLProxy) forward(src, dst net.Conn) {
	defer src.Close()
	defer dst.Close()

	sourceAddress := src.RemoteAddr().String()
	destAddress := dst.RemoteAddr().String()

	defer func() {
		p.lock.Lock()
		if _, ok := p.Keyspaces[sourceAddress]; ok {
			delete(p.Keyspaces, sourceAddress)
		}
		p.lock.Unlock()
	}()

	if destAddress == p.sourceIP {
		p.Metrics.incrementConnections()
		defer func() {
			p.Metrics.decrementConnections()
			p.checkRedirect()
		}()
	}

	buf := make([]byte, 0xffff)
	data := make([]byte, 0)
	for {
		bytesRead, err := src.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Debugf("%s disconnected", src.RemoteAddr())
			} else {
				log.Error(err)
			}
			return
		}
		data = append(data, buf[:bytesRead]...)

		// Build queries reading in at most 0xffff size at a time.
		for true {

			//if there is not a full CQL header
			if len(data) < cassHdrLen {
				break
			}

			bodyLength := binary.BigEndian.Uint32(data[5:9])
			fullLength := cassHdrLen + int(bodyLength)
			if len(data) < fullLength {
				break
			} else if fullLength > cassMaxLen {
				log.Error("query larger than max allowed by Cassandra, ignoring.")
				data = data[fullLength:]
				break
			}

			p.Metrics.incrementFrames()
			frame := data[:fullLength]

			// If the frame is a reply from Astra, handle the reply. If the frame
			// is a query from the user, always send to Astra through writeToAstra()
			if frame[0] > 0x80 {
				if sourceAddress == p.astraIP {
					p.handleAstraReply(frame)
				}
			} else {
				err := p.writeToAstra(frame, sourceAddress)
				if err != nil {
					log.Error(err)
				}
			}

			// Only tunnel packets through if we are directly connected to the source DB.
			// If we are connected directly to the Astra DB, we don't need to write to dst,
			// as the write was already handled by writeToAstra above
			if sourceAddress == p.sourceIP || destAddress == p.sourceIP {
				_, err := dst.Write(frame)
				if err != nil {
					log.Error(err)
					continue
				}
			}

			// Keep any extra bytes in the buffer that are part of the next query
			data = data[fullLength:]
		}
	}
}

// handleAstraReply checks if this frame was a reply from a recently sent PREPARE statement
// and if it was successful, then it stores the path. If this reply was Astra's success response
// from us running a write query, then we send the success/failure message to the channel
// corresponding to the Stream ID of the response
func (p *CQLProxy) handleAstraReply(data []byte) {
	streamID := binary.BigEndian.Uint16(data[2:4])
	opcode := data[4]

	log.Debugf("Reply with opcode %d and stream-id %d", data[4], streamID)

	// if this is an opcode == RESULT message of type 'prepared', associate the prepared
	// statement id with the full query string that was included in the
	// associated PREPARE request.  The stream-id in this reply allows us to
	// find the associated prepare query string.
	if opcode == 0x08 {
		resultKind := binary.BigEndian.Uint32(data[9:13])
		log.Debugf("resultKind = %d", resultKind)
		if resultKind == 0x0004 {
			idLen := binary.BigEndian.Uint16(data[13:15])
			preparedID := string(data[15 : 15+idLen])
			log.Debugf("Result with prepared-id = '%s' for stream-id %d", preparedID, streamID)
			path := p.preparedQueries.PreparedQueryPathByStreamID[streamID]
			if len(path) > 0 {
				// found cached query path to associate with this preparedID
				p.preparedQueries.PreparedQueryPathByPreparedID[preparedID] = path
				log.Debugf("Associating query path '%s' with prepared-id %s as part of stream-id %d",
					path, preparedID, streamID)
			} else {
				log.Warnf("Unable to find prepared query path associated with stream-id %d", streamID)
			}
		}
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	// If this is a response to a previous query we ran, send result over success channel
	// Failed query only if opcode is ERROR (0x0000)
	if success, ok := p.queryResponses[streamID]; ok {
		success <- opcode != 0x0000
		return
	}
}

// writeToAstra takes a query frame and ensures it is properly relayed to the Astra DB
func (p *CQLProxy) writeToAstra(data []byte, client string) error {
	compressionFlag := data[1] & 0x01
	if compressionFlag == 1 {
		return errors.New("compression flag set, unable to parse reply beyond header")
	}

	// Returns list of []string paths in form /opcode/action/table
	// opcode is "startup", "query", "batch", etc.
	// action is "select", "insert", "update", etc,
	// table is the table as written in the command
	paths, err := cqlparser.CassandraParseRequest(p.preparedQueries, data)
	if err != nil {
		return err
	}

	if len(paths) == 0 {
		return errors.New("invalid request")
	}

	// FIXME: Handle more actions based on paths
	// currently handles query and prepare statements that involve 'use, insert, update, delete, and truncate'
	if len(paths) > 1 {
		return nil
		// return p.handleBatchQuery(data, paths)
		// TODO: Handle batch statements
	}

	if paths[0] == cqlparser.UnknownPreparedQueryPath {
		log.Debug("Err: Encountered unknown prepared query. Query Ignored")
		return nil
	}

	fields := strings.Split(paths[0], "/")
	if len(fields) > 2 {
		if fields[1] == "prepare" {
			q := newQuery(nil, prepareQuery, data)
			return p.execute(q)
		} else if fields[1] == "query" || fields[1] == "execute" {
			queryType := QueryType(fields[2])

			switch queryType {
			case useQuery:
				return p.handleUseQuery(fields[3], data, client)
			case insertQuery, updateQuery, deleteQuery, truncateQuery:
				return p.handleWriteQuery(fields[3], queryType, data, client)
			case selectQuery:
				p.Metrics.incrementReads()
			}
		}
	} else {
		// path is '/opcode' case
		// FIXME: decide if there are any cases we need to handle here
		q := newQuery(nil, miscQuery, data)
		return p.execute(q)
	}

	return nil
}

func (p *CQLProxy) handleUseQuery(keyspace string, query []byte, client string) error {
	// Cassandra assumes case-insensitive unless keyspace is encased in quotation marks
	if strings.HasPrefix(keyspace, "\"") && strings.HasSuffix(keyspace, "\"") {
		keyspace = keyspace[1 : len(keyspace)-1]
	} else {
		keyspace = strings.ToLower(keyspace)
	}

	if _, ok := p.migrationStatus.Tables[keyspace]; !ok {
		return errors.New("invalid keyspace")
	}

	p.lock.Lock()
	p.Keyspaces[client] = keyspace
	p.lock.Unlock()

	q := newQuery(nil, useQuery, query)

	return p.execute(q)
}

func (p *CQLProxy) handleWriteQuery(fromClause string, queryType QueryType, data []byte, client string) error {
	keyspace, tableName := extractTableInfo(fromClause)

	// Is the keyspace already in the table clause of the query, or do we need to add it
	addKeyspace := false
	if keyspace == "" {
		keyspace = p.Keyspaces[client]
		if keyspace == "" {
			return errors.New("invalid keyspace")
		}

		addKeyspace = true
	}

	table, ok := p.migrationStatus.Tables[keyspace][tableName]
	if !ok {
		return fmt.Errorf("table %s.%s does not exist", keyspace, tableName)
	}

	q := newQuery(table, queryType, data).usingTimestamp()
	if addKeyspace {
		q = q.addKeyspace(keyspace)
	}

	// If we have a write query that depends on all values already being present in the database,
	// if migration of this table is currently in progress (or about to begin), then pause consumption
	// of queries for this table.
	if queryType != insertQuery {
		status := p.tableStatus(keyspace, tableName)
		if !p.tablePaused[keyspace][tableName] && status >= migration.WaitingToUnload && status < migration.LoadingDataComplete {
			p.stopTable(keyspace, tableName)
		}
	}

	p.queueQuery(q)

	return nil
}

//TODO: Handle batch statements
func (p *CQLProxy) handleBatchQuery(query []byte, paths []string) error {
	return nil
}

func (p *CQLProxy) queueQuery(query *Query) {
	p.queues[query.Table.Keyspace][query.Table.Name] <- query

	p.lock.Lock()
	defer p.lock.Unlock()
	p.queueSizes[query.Table.Keyspace][query.Table.Name]++
}

// consumeQueue executes all queries for a particular table, in the order that they are received.
func (p *CQLProxy) consumeQueue(keyspace string, table string) {
	log.Debugf("Beginning consumption of queries for %s.%s", keyspace, table)

	for {
		select {
		case query := <-p.queues[keyspace][table]:
			p.queueLocks[keyspace][table].Lock()

			// Driver is async, so we don't need a lock around query execution
			err := p.executeWrite(query)
			if err != nil {
				// TODO: Figure out exactly what to do if we're unable to write
				// 	If it's a bad query, no issue, but if it's a good query that isn't working for some reason
				// 	we need to figure out what to do
				log.Error(err)
				p.Metrics.incrementWriteFails()
			} else {
				p.Metrics.incrementWrites()
			}

			p.lock.Lock()
			p.queueSizes[keyspace][table]--
			p.lock.Unlock()

			p.queueLocks[keyspace][table].Unlock()
		}

	}
}

// TODO: Change stream when retrying or else cassandra doesn't respond
// executeWrite will keep retrying a query up to maxQueryRetries number of times
// if it's unsuccessful
func (p *CQLProxy) executeWrite(q *Query, attempts ...int) error {
	attempt := 1
	if attempts != nil {
		attempt = attempts[0]
	}

	if attempt > maxQueryRetries {
		return fmt.Errorf("query on stream %d unsuccessful", q.Stream)
	}

	err := p.executeAndCheckReply(q)
	if err != nil {
		log.Errorf("%s. Retrying query %d", err.Error(), q.Stream)
		return p.executeWrite(q, attempt+1)
	}

	return nil
}

// executeAndCheckReply will send a query to the Astra DB, and listen for a response.
// Returns an error if the duration of queryTimeout passes without a response,
// or if the Astra DB responds saying that there was an error with the query.
func (p *CQLProxy) executeAndCheckReply(q *Query) error {
	resp := p.createQueryResponseChan(q)
	defer func() {
		p.lock.Lock()
		delete(p.queryResponses, q.Stream)
		close(resp)
		p.lock.Unlock()
	}()

	err := p.execute(q)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(queryTimeout)
	for {
		select {
		case <-ticker.C:
			return fmt.Errorf("timeout for query %d", q.Stream)
		case success := <-resp:
			if success {
				log.Debugf("received successful response for query %d", q.Stream)
				return nil
			}
			return fmt.Errorf("received unsuccessful response for query %d", q.Stream)
		}
	}
}

func (p *CQLProxy) createQueryResponseChan(q *Query) chan bool {
	p.lock.Lock()
	defer p.lock.Unlock()

	respChan := make(chan bool, 1)
	p.queryResponses[q.Stream] = respChan

	return respChan
}

// TODO: is this function even needed? Do we need to be retrying here too?
func (p *CQLProxy) execute(query *Query) error {
	log.Debugf("Executing %v", *query)

	var err error
	for i := 1; i <= 5; i++ {
		_, err := p.astraSession.Write(query.Query)
		if err == nil {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	return err
}

func (p *CQLProxy) tableStatus(keyspace string, tableName string) migration.Step {
	table := p.migrationStatus.Tables[keyspace][tableName]
	table.Lock.Lock()
	defer table.Lock.Unlock()

	return table.Step
}

// stopTable grabs the queueLock for a table so that the consumeQueue function cannot
// continue processing queries for the table.
func (p *CQLProxy) stopTable(keyspace string, tableName string) {
	log.Debugf("Stopping query consumption on %s.%s", keyspace, tableName)
	p.lock.Lock()
	defer p.lock.Unlock()

	p.tablePaused[keyspace][tableName] = true
	p.queueLocks[keyspace][tableName].Lock()
}

// startTable releases the queueLock for a table so that the consumeQueue function
// can resume processing queries for the table.
func (p *CQLProxy) startTable(keyspace string, tableName string) {
	log.Debugf("Starting query consumption on %s.%s", keyspace, tableName)
	p.lock.Lock()
	defer p.lock.Unlock()

	p.tablePaused[keyspace][tableName] = false
	p.queueLocks[keyspace][tableName].Unlock()
}

// checkRedirect communicates over the ReadyForRedirect channel when migration is complete
// and there are no direct connections to the client's source DB any longer
// (envoy can point directly to the Astra DB now, skipping over proxy)
func (p *CQLProxy) checkRedirect() {
	if p.migrationComplete && p.Metrics.sourceConnections() == 0 {
		log.Debug("No more connections to client database; ready for redirect.")
		p.ReadyForRedirect <- struct{}{}
	}
}

// Shutdown shuts down the proxy service
func (p *CQLProxy) Shutdown() {
	log.Info("Proxy shutting down...")
	p.shutdown = true
	for _, listener := range p.listeners {
		listener.Close()
	}

	// TODO: Stop all goroutines
}

func (p *CQLProxy) reset() {
	p.queues = make(map[string]map[string]chan *Query)
	p.queueLocks = make(map[string]map[string]*sync.Mutex)
	p.queueSizes = make(map[string]map[string]int)
	p.tablePaused = make(map[string]map[string]bool)
	p.queryResponses = make(map[uint16]chan bool)
	p.ReadyChan = make(chan struct{})
	p.ShutdownChan = make(chan struct{})
	p.shutdown = false
	p.outstandingUpdates = make(map[string]chan bool)
	p.migrationComplete = p.Conf.MigrationComplete
	p.listeners = []net.Listener{}
	p.ReadyForRedirect = make(chan struct{})
	p.lock = &sync.Mutex{}
	p.Metrics = Metrics{}
	p.Metrics.lock = &sync.Mutex{}
	p.sourceIP = fmt.Sprintf("%s:%d", p.Conf.SourceHostname, p.Conf.SourcePort)
	p.astraIP = fmt.Sprintf("%s:%d", p.Conf.AstraHostname, p.Conf.AstraPort)
	p.migrationServiceIP = fmt.Sprintf("%s:%d", p.Conf.MigrationServiceHostname, p.Conf.MigrationCommunicationPort)
	p.preparedQueries = &cqlparser.PreparedQueries{
		PreparedQueryPathByStreamID:   make(map[uint16]string),
		PreparedQueryPathByPreparedID: make(map[string]string),
	}
	p.MigrationStart = make(chan *migration.Status, 1)
	p.MigrationDone = make(chan struct{})
	p.Keyspaces = make(map[string]string)
}

// Given a FROM argument, extract the table name
// ex: table, keyspace.table, keyspace.table;, keyspace.table(, etc..
func extractTableInfo(fromClause string) (string, string) {
	var keyspace string

	// Remove keyspace if table in format keyspace.table
	if i := strings.IndexRune(fromClause, '.'); i != -1 {
		keyspace = fromClause[:i]
	}

	// Remove semicolon if it is attached to the table name from the query
	tableName := strings.TrimSuffix(fromClause, ";")

	// Remove keyspace if table in format keyspace.table
	if i := strings.IndexRune(tableName, '.'); i != -1 {
		tableName = tableName[i+1:]
	}

	// Remove column names if part of an INSERT query: ex: TABLE(col, col)
	if i := strings.IndexRune(tableName, '('); i != -1 {
		tableName = tableName[:i]
	}

	return keyspace, tableName
}

func (p *CQLProxy) runMetrics() {
	http.HandleFunc("/", p.Metrics.write)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", p.Conf.ProxyMetricsPort), nil))
}

type Metrics struct {
	FrameCount int
	Reads      int
	Writes     int

	WriteFails int
	ReadFails  int

	ConnectionsToSource int

	lock *sync.Mutex
}

func (m *Metrics) write(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	marshaled, err := json.Marshal(m)
	if err != nil {
		w.Write([]byte(`{"error": "unable to grab metrics"}`))
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(marshaled)
}

func (m *Metrics) incrementFrames() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.FrameCount++
}

func (m *Metrics) incrementReads() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.Reads++
}

func (m *Metrics) incrementWrites() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.Writes++
}

func (m *Metrics) incrementWriteFails() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.WriteFails++
}

func (m *Metrics) incrementReadFails() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.ReadFails++
}

func (m *Metrics) incrementConnections() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.ConnectionsToSource++
}

func (m *Metrics) decrementConnections() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.ConnectionsToSource--
}

func (m *Metrics) sourceConnections() int {
	m.lock.Lock()
	defer m.lock.Unlock()

	return m.ConnectionsToSource
}
