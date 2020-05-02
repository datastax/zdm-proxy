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

	"cloud-gate/migration/migration"
	"cloud-gate/proxy/pkg/auth"
	"cloud-gate/proxy/pkg/config"
	"cloud-gate/proxy/pkg/cqlparser"
	"cloud-gate/proxy/pkg/frame"
	"cloud-gate/proxy/pkg/query"
	"cloud-gate/updates"

	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"
)

const (
	// TODO: Finalize queue size to use
	queueSize = 1000

	thresholdToRedirect = 5

	cassHdrLen = 9
	cassMaxLen = 268435456 // 256 MB, per spec

	maxQueryRetries = 5
	queryTimeout    = 2 * time.Second

	priorityUpdateSize = 50
)

type CQLProxy struct {
	Conf *config.Config

	sourceIP           string
	astraIP            string
	migrationServiceIP string
	migrationSession   net.Conn

	listeners []net.Listener

	queues         map[string]map[string]chan *query.Query
	queueLocks     map[string]map[string]*sync.Mutex
	tablePaused    map[string]map[string]bool
	queryResponses map[uint16]chan bool
	lock           *sync.Mutex

	astraSessions map[string]net.Conn

	outstandingQueries map[string]map[uint16]*frame.Frame
	outstandingUpdates map[string]chan bool
	migrationStatus    *migration.Status
	migrationComplete  bool

	// Used to broadcast to all suspended forward threads once all queues are empty
	queuesCompleteCond  *sync.Cond
	queuesComplete      bool
	redirectActiveConns bool

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

	// Ensure source is up and running
	//conn := p.establishConnection(p.sourceIP)
	//conn.Close()

	// Ensure Astra is up and running
	//conn = p.establishConnection(p.astraIP)
	//conn.Close()

	p.migrationSession = p.establishConnection(p.migrationServiceIP)

	go p.statusLoop()
	go p.runMetrics()

	err := p.listen(p.Conf.ProxyCommunicationPort, p.handleMigrationConnection)
	if err != nil {
		return err
	}

	<-p.ReadyChan
	err = p.listen(p.Conf.ProxyQueryPort, p.handleClientConnection)
	if err != nil {
		return err
	}

	log.Infof("Proxy connected and ready to accept queries on port %d", p.Conf.ProxyQueryPort)
	return nil
}

func (p *CQLProxy) establishConnection(ip string) net.Conn {
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
		log.Infof("Successfully established connection with %s", ip)
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
				go p.redirectActiveConnectionsToAstra()
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
		p.queues[keyspace] = make(map[string]chan *query.Query)
		p.queueLocks[keyspace] = make(map[string]*sync.Mutex)
		p.tablePaused[keyspace] = make(map[string]bool)
		for tableName := range tables {
			p.queues[keyspace][tableName] = make(chan *query.Query, queueSize)
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
func (p *CQLProxy) handleClientConnection(client net.Conn) {
	// If our service has not completed yet
	if !p.queuesComplete {
		database := p.establishConnection(p.sourceIP)

		p.lock.Lock()
		p.outstandingQueries[client.RemoteAddr().String()] = make(map[uint16]*frame.Frame)
		p.astraSessions[client.RemoteAddr().String()] = p.establishConnection(p.astraIP)
		p.lock.Unlock()

		// Begin two way packet forwarding
		go p.forward(client, database)
	} else {
		astra := p.establishConnection(p.astraIP)
		go p.forwardDirect(client, astra)
		go p.forwardDirect(astra, client)
	}
}

// Should only be called when migration is currently running
func (p *CQLProxy) forward(src, dst net.Conn) {
	sourceAddress := src.RemoteAddr().String()
	destAddress := dst.RemoteAddr().String()

	// So we don't close the src twice.
	// Allows us to stop (oldDB -> client) goroutine for existing connections when migration is complete
	if destAddress == p.sourceIP {
		defer src.Close()
		defer dst.Close()
	}

	var pointsToSource bool
	// TODO: not sure if this check is necessary, since we will be having two forward function.
	// TODO: One for forwarding to oldDB and one to astra direct for later
	if sourceAddress == p.sourceIP || destAddress == p.sourceIP {
		pointsToSource = true
	}

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

	isClient := sourceAddress != p.sourceIP && sourceAddress != p.astraIP
	authenticated := false

	frameHeader := make([]byte, cassHdrLen)
	for {
		_, err := src.Read(frameHeader)
		if err != nil {
			if err != io.EOF {
				log.Debugf("%s disconnected", src.RemoteAddr())
			} else {
				log.Error(err)
			}
			return
		}

		bodyLen := binary.BigEndian.Uint32(frameHeader[5:9])
		frameBody := make([]byte, bodyLen)
		if bodyLen != 0 {
			_, err := src.Read(frameBody)
			if err != nil {
				log.Error(err)
				continue
			}
		}

		data := append(frameHeader, frameBody...)

		// log.Debugf("body len %s", bodyLen)
		log.Debugf("%s -> %s: %v", src.RemoteAddr(), dst.RemoteAddr(), data)

		if len(data) > cassMaxLen {
			log.Error("query larger than max allowed by Cassandra, ignoring.")
			continue
		}

		//log.Infof("%s sent %v", src.RemoteAddr(), data)

		if isClient && !authenticated {
			// STARTUP packet from client
			if data[4] == 0x01 {
				username := p.Conf.SourceUsername
				password := p.Conf.SourcePassword

				err := auth.HandleStartup(src, dst, username, password, data, true)
				if err != nil {
					// TODO: probably write an error back in CQL protocol, so it can be displayed on client correctly
					// 	Maybe just make a method that will do this
					log.Errorf("Could not start session for client %s", sourceAddress)
					return
				}

				username = p.Conf.AstraUsername
				password = p.Conf.AstraPassword

				err = auth.HandleStartup(src, p.astraSessions[src.RemoteAddr().String()], username, password, data, false)
				// Start sending responses from database back to client
				go p.forward(dst, src)
				go p.astraReplyHandler(src)
				authenticated = true
			}
			continue
		}

		// || pointsToSource is to allow the first query after beginning redirect to go through
		// this makes it so the forward from dst -> client doesn't permanently block in src.Read
		if !p.redirectActiveConns || pointsToSource {
			p.Metrics.incrementFrames()
			f := frame.New(data)

			// Sent from client
			if f.Direction == 0 {
				p.lock.Lock()
				p.outstandingQueries[sourceAddress][f.Stream] = f
				p.lock.Unlock()
			} else {
				// Response from database
				p.lock.Lock()
				if f.Opcode == 0x00 {
					// ERROR
					delete(p.outstandingQueries[destAddress], f.Stream)
					log.Debug("Source errored")
				} else if _, ok := p.outstandingQueries[destAddress][f.Stream]; ok {
					// SUCCESS
					go p.mirrorToAstra(destAddress, f.Stream)
					log.Debugf("success, mirroring to Astra %s %d", destAddress, f.Stream)
				}
				p.lock.Unlock()
			}
		}

		//log.Debugf("writing %s -> %s", sourceAddress, destAddress)
		_, err = dst.Write(data)
		if err != nil {
			log.Error(err)
			continue
		}

		// handle redirect, if necessary
		if p.redirectActiveConns && pointsToSource {
			// Suspends forwarding until all queues have finished consuming their contents
			p.queuesCompleteCond.L.Lock()
			if !p.queuesComplete {
				log.Debugf("Sleeping connection %s -> %s", src.RemoteAddr(), dst.RemoteAddr())
				p.queuesCompleteCond.Wait()
			}
			p.queuesCompleteCond.L.Unlock()

			log.Debugf("Redirecting connection %s -> %s", src.RemoteAddr(), dst.RemoteAddr())
			if destAddress == p.sourceIP {
				clientIP := sourceAddress
				dst = p.astraSessions[clientIP]
				pointsToSource = false
			} else if sourceAddress == p.sourceIP {
				src.Close()
				// uses astraReplyHandler to write replies to client
				return
			}
		}
	}
}

// ForwardDirect handles the direct forwarding of traffic between SRC and DST with no modifications
func (p *CQLProxy) forwardDirect(src, dst net.Conn) {
	defer src.Close()
	defer dst.Close()

	if dst.RemoteAddr().String() == p.astraIP {
		p.Metrics.incrementConnections()
		defer func() {
			p.Metrics.decrementConnections()
			p.checkRedirect()
		}()
	}

	buffer := make([]byte, 0xffff)
	for {
		bytesRead, err := src.Read(buffer)
		if err != nil {
			if err != io.EOF {
				log.Debugf("%s disconnected from Astra", src.RemoteAddr())
			} else {
				log.Error(err)
			}
			return
		}

		_, err = dst.Write(buffer[:bytesRead])
		if err != nil {
			log.Error(err)
			continue
		}
	}
}

func (p *CQLProxy) mirrorToAstra(clientIP string, streamID uint16) {
	p.lock.Lock()
	f := p.outstandingQueries[clientIP][streamID]
	p.lock.Unlock()

	err := p.writeToAstra(f, clientIP)
	if err != nil {
		log.Error(err)
		return
	}
}

// writeToAstra takes a query frame and ensures it is properly relayed to the Astra DB
func (p *CQLProxy) writeToAstra(f *frame.Frame, client string) error {
	if f.Flags&0x01 == 1 {
		return errors.New("compression flag set, unable to parse reply beyond header")
	}

	// Returns list of []string paths in form /opcode/action/table
	// opcode is "startup", "query", "batch", etc.
	// action is "select", "insert", "update", etc,
	// table is the table as written in the command
	paths, err := cqlparser.CassandraParseRequest(p.preparedQueries, f.RawBytes)
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
			q := query.New(nil, query.PREPARE, f, client)
			return p.execute(q)
		} else if fields[1] == "query" || fields[1] == "execute" {
			queryType := query.Type(fields[2])

			switch queryType {
			case query.USE:
				return p.handleUseQuery(fields[3], f, client)
			case query.INSERT, query.UPDATE, query.DELETE, query.TRUNCATE:
				return p.handleWriteQuery(fields[3], queryType, f, client)
			case query.SELECT:
				p.Metrics.incrementReads()
			}
		}
	} else {
		// path is '/opcode' case
		// FIXME: decide if there are any cases we need to handle here
		q := query.New(nil, query.MISC, f, client)
		return p.execute(q)
	}

	return nil
}

func (p *CQLProxy) astraReplyHandler(client net.Conn) {
	clientIP := client.RemoteAddr().String()
	p.lock.Lock()
	session := p.astraSessions[clientIP]
	p.lock.Unlock()

	frameHeader := make([]byte, cassHdrLen)
	for {
		_, err := session.Read(frameHeader)
		if err != nil {
			log.Error(err)
			return
		}

		bodyLen := binary.BigEndian.Uint32(frameHeader[5:9])
		frameBody := make([]byte, bodyLen)
		if bodyLen != 0 {
			_, err = session.Read(frameBody)
			if err != nil {
				log.Error(err)
				continue
			}
		}

		data := append(frameHeader, frameBody...)

		resp := frame.New(data)

		p.lock.Lock()
		if _, ok := p.outstandingQueries[clientIP][resp.Stream]; ok {
			success := resp.Opcode != 0x00
			if resp, ok := p.queryResponses[resp.Stream]; ok {
				resp <- success
			}

			if success {
				log.Debugf("Received success response from Astra from query %d", resp.Stream)
				delete(p.outstandingQueries[clientIP], resp.Stream)
			} else {
				log.Debugf("Received error response from Astra from query %d", resp.Stream)
				p.checkError(resp.RawBytes)
			}
		}
		p.lock.Unlock()

		if p.queuesComplete {
			_, err = client.Write(data)
			if err != nil {
				log.Error(err)
				continue
			}
		}
	}
}

func (p *CQLProxy) checkError(body []byte) {
	errCode := binary.BigEndian.Uint16(body[0:2])
	switch errCode {
	case 0x0000:
		// Server Error
		p.Metrics.incrementServerErrors()
	case 0x1100:
		// Write Timeout
		p.Metrics.incrementWriteFails()
	case 0x1200:
		// Read Timeout
		p.Metrics.incrementReadFails()
	}

}

func (p *CQLProxy) handleUseQuery(keyspace string, f *frame.Frame, client string) error {
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

	q := query.New(nil, query.USE, f, client)

	return p.execute(q)
}

func (p *CQLProxy) handleWriteQuery(fromClause string, queryType query.Type, f *frame.Frame, client string) error {
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

	q := query.New(table, queryType, f, client).UsingTimestamp()
	if addKeyspace {
		q = q.AddKeyspace(keyspace)
	}

	// If we have a write query that depends on all values already being present in the database,
	// if migration of this table is currently in progress (or about to begin), then pause consumption
	// of queries for this table.
	if queryType != query.INSERT {
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

func (p *CQLProxy) queueQuery(query *query.Query) {
	queue := p.queues[query.Table.Keyspace][query.Table.Name]
	queue <- query

	queriesRemaining := len(queue)
	if queriesRemaining > 0 && queriesRemaining % priorityUpdateSize == 0 {
		err := p.sendPriorityUpdate(query.Table.Keyspace, query.Table.Name, queriesRemaining)
		if err != nil {
			log.Error(err)
		}
	}
}

// consumeQueue executes all queries for a particular table, in the order that they are received.
func (p *CQLProxy) consumeQueue(keyspace string, table string) {
	log.Debugf("Beginning consumption of queries for %s.%s", keyspace, table)
	queue := p.queues[keyspace][table]

	for {
		select {
		case query := <-queue:
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

			p.queueLocks[keyspace][table].Unlock()
		}

	}
}

// TODO: Change stream when retrying or else cassandra doesn't respond
// executeWrite will keep retrying a query up to maxQueryRetries number of times
// if it's unsuccessful
func (p *CQLProxy) executeWrite(q *query.Query, retries ...int) error {
	retry := 0
	if retries != nil {
		retry = retries[0]
	}

	if retry > maxQueryRetries {
		return fmt.Errorf("query on stream %d unsuccessful", q.Stream)
	}

	err := p.executeAndCheckReply(q)
	if err != nil {
		log.Errorf("%s. Retrying query %d", err.Error(), q.Stream)
		return p.executeWrite(q, retry+1)
	}

	return nil
}

// executeAndCheckReply will send a query to the Astra DB, and listen for a response.
// Returns an error if the duration of queryTimeout passes without a response,
// or if the Astra DB responds saying that there was an error with the query.
func (p *CQLProxy) executeAndCheckReply(q *query.Query) error {
	resp := p.createResponseChan(q)
	defer p.closeResponseChan(q, resp)

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

func (p *CQLProxy) createResponseChan(q *query.Query) chan bool {
	p.lock.Lock()
	defer p.lock.Unlock()

	respChan := make(chan bool, 1)
	p.queryResponses[q.Stream] = respChan

	return respChan
}

func (p *CQLProxy) closeResponseChan(q *query.Query, resp chan bool) {
	p.lock.Lock()
	defer p.lock.Unlock()

	delete(p.queryResponses, q.Stream)
	close(resp)
}

// TODO: is this function even needed? Do we need to be retrying here too?
func (p *CQLProxy) execute(query *query.Query) error {
	log.Debugf("Executing %v", *query)
	p.lock.Lock()
	session := p.astraSessions[query.Source]
	p.lock.Unlock()

	var err error
	for i := 1; i <= 5; i++ {
		_, err := session.Write(query.Query)
		if err == nil {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	return err
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

func (p *CQLProxy) handleMigrationConnection(conn net.Conn) {
	updates.CommunicationHandler(conn, p.migrationSession, p.handleUpdate)
}

func (p *CQLProxy) handleUpdate(update *updates.Update) error {
	switch update.Type {
	case updates.Start:
		var status migration.Status
		err := json.Unmarshal(update.Data, &status)
		if err != nil {
			return errors.New("unable to unmarshal json")
		}
		status.Lock = &sync.Mutex{}

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
func (p *CQLProxy) sendPriorityUpdate(keyspace string, tableName string, newPriority int) error {
	p.migrationStatus.Lock.Lock()
	table := p.migrationStatus.Tables[keyspace][tableName]
	p.migrationStatus.Lock.Unlock()

	table.SetPriority(newPriority)
	marshaledTable, err := json.Marshal(table)
	if err != nil {
		return err
	}

	update := updates.New(updates.TableUpdate, marshaledTable)
	return updates.Send(update, p.migrationSession)
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
	p.queues = make(map[string]map[string]chan *query.Query)
	p.queueLocks = make(map[string]map[string]*sync.Mutex)
	p.tablePaused = make(map[string]map[string]bool)
	p.queryResponses = make(map[uint16]chan bool)
	p.ReadyChan = make(chan struct{})
	p.ShutdownChan = make(chan struct{})
	p.shutdown = false
	p.outstandingQueries = make(map[string]map[uint16]*frame.Frame)
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
	p.astraSessions = make(map[string]net.Conn)
	p.queuesCompleteCond = sync.NewCond(&sync.Mutex{})
}

func (p *CQLProxy) redirectActiveConnectionsToAstra() {
	redirected := false
	var queuesAllSmall, queuesAllComplete bool

	for !redirected {
		queuesAllSmall, queuesAllComplete = p.checkQueueLens()

		if queuesAllSmall {
			p.redirectActiveConns = true
		}
		if queuesAllComplete {
			p.queuesCompleteCond.L.Lock()
			p.queuesComplete = true
			p.queuesCompleteCond.Broadcast()
			p.queuesCompleteCond.L.Unlock()
			redirected = true
		}

		if p.redirectActiveConns {
			time.Sleep(100)
		} else {
			time.Sleep(5000)
		}
	}
}

// Returns (all queues < thresholdToRedirect, all queues empty) as (bool, bool)
func (p *CQLProxy) checkQueueLens() (bool, bool) {
	allSmall := true
	allComplete := true
	for keyspace, tableMap := range p.migrationStatus.Tables {
		for tablename, _ := range tableMap {
			p.queueLocks[keyspace][tablename].Lock()
			queueLen := len(p.queues[keyspace][tablename])
			p.queueLocks[keyspace][tablename].Unlock()

			if queueLen > 0 {
				allComplete = false
			}
			if queueLen > thresholdToRedirect {
				allSmall = false
			}
			if !allSmall && !allComplete {
				return false, false
			}
		}
	}
	return allSmall, allComplete
}

// Given a FROM argument, extract the table name
// ex: table, keyspace.table, keyspace.table;, keyspace.table(, etc..
func extractTableInfo(fromClause string) (string, string) {
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

	ServerErrors int
	WriteFails   int
	ReadFails    int

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

func (m *Metrics) incrementServerErrors() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.ServerErrors++
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
