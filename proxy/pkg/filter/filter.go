package filter

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"cloud-gate/migration/migration"
	"cloud-gate/proxy/pkg/auth"
	"cloud-gate/proxy/pkg/config"
	"cloud-gate/proxy/pkg/cqlparser"
	"cloud-gate/proxy/pkg/frame"
	"cloud-gate/proxy/pkg/metrics"
	"cloud-gate/proxy/pkg/query"
	"cloud-gate/updates"

	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"
)

const (
	// TODO: Make these configurable
	thresholdToRedirect = 5
	maxQueryRetries     = 5
	queryTimeout        = 2 * time.Second
	priorityUpdateSize  = 50

	cassHdrLen = 9
	cassMaxLen = 268435456 // 256 MB, per spec
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
	lock           *sync.Mutex // TODO: maybe change this to a RWMutex for better performance

	astraSessions map[string]net.Conn
	sessionLocks  map[string]*sync.Mutex

	outstandingQueries map[string]map[uint16]*frame.Frame
	outstandingUpdates map[string]chan bool
	outstandingUses    map[string]map[uint16]chan bool
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

	// Keeps track of the current keyspace that each CLIENT is in
	Keyspaces map[string]string
	// Keeps track of the current keyspace that the PROXY is in while connected to Astra
	astraKeyspace map[string]string

	// Metrics
	Metrics *metrics.Metrics

	preparedIDs         map[uint16]string
	mappedPreparedIDs   map[string]string
	outstandingPrepares map[uint16][]byte
	preparedQueryBytes  map[string][]byte
	prepareIDToKeyspace map[string]string
}

// Start starts up the proxy. The proxy creates a connection with the Astra Database,
// creates a communication channel with the migration service and then begins listening
// on $PROXY_QUERY_PORT for queries to the database.
func (p *CQLProxy) Start() error {
	p.reset()
	p.checkDatabaseConnections()

	// p.migrationSession = establishConnection(p.migrationServiceIP)

	go p.statusLoop()

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

// TODO: Is there a better way to check that we can connect to both databases?
func (p *CQLProxy) checkDatabaseConnections() {
	// Wait until the source database is up and ready to accept TCP connections.
	source := establishConnection(p.sourceIP)
	source.Close()

	// Wait until the Astra database is up and ready to accept TCP connections.
	astra := establishConnection(p.astraIP)
	astra.Close()
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
			p.queues[keyspace][tableName] = make(chan *query.Query, p.Conf.MaxQueueSize)
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
		sourceSession := establishConnection(p.sourceIP)
		astraSession := establishConnection(p.astraIP)

		clientIP := client.RemoteAddr().String()

		p.lock.Lock()
		p.outstandingQueries[clientIP] = make(map[uint16]*frame.Frame)
		p.outstandingUses[clientIP] = make(map[uint16]chan bool)
		p.astraSessions[clientIP] = astraSession
		p.sessionLocks[clientIP] = &sync.Mutex{}
		p.lock.Unlock()

		go p.forward(client, sourceSession)
	} else {
		astraSession := establishConnection(p.astraIP)
		p.forwardDirect(client, astraSession)
	}
}

// Should only be called when migration is currently running
func (p *CQLProxy) forward(src, dst net.Conn) {
	authenticated := false
	sourceAddress := src.RemoteAddr().String()
	destAddress := dst.RemoteAddr().String()
	pointsToSource := sourceAddress == p.sourceIP || destAddress == p.sourceIP

	if destAddress == p.sourceIP {
		defer func() {
			src.Close()
			dst.Close()

			p.clientDisconnect(sourceAddress)
		}()

	}

	frameHeader := make([]byte, cassHdrLen)
	for {
		_, err := src.Read(frameHeader)
		if err != nil {
			if err != io.EOF {
				log.Debugf("%s disconnected", sourceAddress)
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

		if len(data) > cassMaxLen {
			log.Error("query larger than max allowed by Cassandra, ignoring.")
			continue
		}

		log.Debugf("(%s -> %s): %v", src.RemoteAddr(), dst.RemoteAddr(), data)

		f := frame.New(data)
		p.Metrics.IncrementFrames()

		if f.Flags&0x01 == 1 {
			log.Errorf("compression flag for stream %d set, unable to parse query beyond header", f.Stream)
			continue
		}

		// Frame from client
		if f.Direction == 0 {
			if !authenticated {
				// Handle client authentication
				authenticated, err = p.handleStartupFrame(f, src, dst)
				if err != nil {
					log.Error(err)
				}
				continue
			}

			if !p.redirectActiveConns || pointsToSource {
				p.lock.Lock()
				p.outstandingQueries[sourceAddress][f.Stream] = f
				if f.Opcode == 0x09 {
					p.outstandingPrepares[f.Stream] = f.RawBytes
				}
				p.lock.Unlock()
			}
		} else {
			p.lock.Lock()
			// Response frame from database
			if f.Opcode == 0x00 {
				// ERROR response
				delete(p.outstandingQueries[destAddress], f.Stream)
			}

			if _, ok := p.outstandingQueries[destAddress][f.Stream]; ok {
				if f.Opcode == 0x08 {
					// RESULT response
					resultKind := binary.BigEndian.Uint32(data[9:13])
					log.Debugf("resultKind = %d", resultKind)
					if resultKind == 0x0004 {
						// PREPARE RESULT
						if queryBytes, ok := p.outstandingPrepares[f.Stream]; ok {
							idLen := binary.BigEndian.Uint16(data[13:15])
							preparedID := string(data[15 : 15+idLen])
							p.preparedIDs[f.Stream] = preparedID
							p.preparedQueryBytes[preparedID] = queryBytes
							log.Debugf("Mapped stream %d to prepared ID %s", f.Stream, preparedID)
						}
					}
				}

				go p.mirrorToAstra(destAddress, f.Stream)
				log.Debugf("Received success response from source database for stream (%s, %d). Mirroring to "+
					"Astra database", sourceAddress, f.Stream)
			}
			p.lock.Unlock()
		}

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

func (p *CQLProxy) handleStartupFrame(f *frame.Frame, client, db net.Conn) (bool, error) {
	switch f.Opcode {
	case 0x05:
		// OPTIONS
		err := auth.HandleOptions(client, db, f.RawBytes)
		if err != nil {
			return false, fmt.Errorf("client %s unable to negotiate options with %s",
				client.RemoteAddr(), db.RemoteAddr())
		}
	case 0x01:
		// STARTUP

		// Ensure that the user provided valid Astra credentials
		err := auth.CheckAuthentication(client, p.Conf.AstraUsername, p.Conf.AstraPassword, f.RawBytes)
		if err != nil {
			return false, err
		}

		// Start CQL session to source database
		err = auth.HandleStartup(client, db, p.Conf.SourceUsername, p.Conf.SourcePassword, f.RawBytes, false)
		if err != nil {
			return false, err
		}

		// Start CQL session to Astra database
		astraSession := p.getAstraSession(client.RemoteAddr().String())
		err = auth.HandleStartup(client, astraSession, p.Conf.AstraUsername, p.Conf.AstraPassword, f.RawBytes, false)
		if err != nil {
			return false, err
		}

		// Start sending responses from source database back to client
		go p.forward(db, client)
		go p.astraReplyHandler(client)

		return true, nil
	}

	return false, fmt.Errorf("received non STARTUP or OPTIONS query from unauthenticated client %s",
		client.RemoteAddr())

}

func (p *CQLProxy) clientDisconnect(client string) {
	p.lock.Lock()
	delete(p.Keyspaces, client)
	p.lock.Unlock()

	p.Metrics.DecrementConnections()
	p.checkRedirect()
}

// forwardDirect directly forwards traffic from src to dst.
func (p *CQLProxy) forwardDirect(src, dst net.Conn) {
	log.Debugf("Directly forwarding all data between %s and %s",
		src.RemoteAddr(), dst.RemoteAddr())

	go func() {
		defer src.Close()
		defer dst.Close()
		io.Copy(src, dst)
	}()

	go func() {
		defer src.Close()
		defer dst.Close()
		io.Copy(dst, src)
	}()
}

func (p *CQLProxy) mirrorToAstra(clientIP string, streamID uint16) {
	f := p.getOutstandingQuery(clientIP, streamID)

	err := p.writeToAstra(f, clientIP)
	if err != nil {
		log.Error(err)
		return
	}
}

// writeToAstra takes a query frame and ensures it is properly relayed to the Astra DB
func (p *CQLProxy) writeToAstra(f *frame.Frame, client string) error {
	// Returns list of []string paths in form /opcode/action/table
	// opcode is "startup", "query", "batch", etc.
	// action is "select", "insert", "update", etc,
	// table is the table as written in the command
	paths, err := cqlparser.CassandraParseRequest(p.preparedQueryBytes, f.RawBytes)
	if err != nil {
		return err
	}

	if paths[0] == cqlparser.UnknownPreparedQueryPath {
		return fmt.Errorf("encountered unknown prepared query for stream %d, ignoring", f.Stream)
	}

	if len(paths) > 1 {
		return p.handleBatchQuery(f, paths, client)
	}

	// currently handles query and prepare statements that involve 'use, insert, update, delete, and truncate'
	fields := strings.Split(paths[0], "/")
	if len(fields) > 2 {
		switch fields[1] {
		case "prepare":
			return p.handlePrepareQuery(fields[3], f, client, paths);
		case "query", "execute":
			if fields[1] == "execute" {
				err = p.updatePrepareID(f)
				if err != nil {
					return err
				}
			}

			queryType := query.Type(fields[2])
			switch queryType {
			case query.USE:
				return p.handleUseQuery(fields[3], f, client, paths)
			case query.INSERT, query.UPDATE, query.DELETE, query.TRUNCATE:
				return p.handleWriteQuery(fields[3], queryType, f, client, paths)
			case query.SELECT:
				p.Metrics.IncrementReads()
			}
		case "batch":
			return p.handleBatchQuery(f, paths, client)
		}
	} else {
		// path is '/opcode' case
		// FIXME: decide if there are any cases we need to handle here
		q := query.New(nil, query.MISC, f, client, paths)
		return p.execute(q)
	}

	return nil
}

func (p *CQLProxy) updatePrepareID(f *frame.Frame) error {
	data := f.RawBytes
	idLength := binary.BigEndian.Uint16(data[9:11])
	preparedID := data[11 : 11+idLength]

	// Ensures that the mapping of source preparedID to astraPreparedID has finished executing
	// TODO: do this in a better way
	for {
		_, ok := p.mappedPreparedIDs[string(preparedID)]
		if ok {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if newPreparedID, ok := p.mappedPreparedIDs[string(preparedID)]; ok {
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

	return fmt.Errorf("no mapping for source prepared id %s to astra prepared id found", preparedID)
}

func (p *CQLProxy) astraReplyHandler(client net.Conn) {
	clientIP := client.RemoteAddr().String()
	session := p.getAstraSession(clientIP)

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
		success := resp.Opcode != 0x00
		if _, ok := p.outstandingQueries[clientIP][resp.Stream]; ok {
			if resp, ok := p.queryResponses[resp.Stream]; ok {
				resp <- success
			}

			if success {
				// if this is an opcode == RESULT message of type 'prepared', associate the prepared
				// statement id with the full query string that was included in the
				// associated PREPARE request.  The stream-id in this reply allows us to
				// find the associated prepare query string.
				if resp.Opcode == 0x08 {
					resultKind := binary.BigEndian.Uint32(data[9:13])
					if resultKind == 0x0004 {
						if sourcePreparedID, ok := p.preparedIDs[resp.Stream]; ok {
							idLen := binary.BigEndian.Uint16(data[13:15])
							astraPreparedID := string(data[15 : 15+idLen])

							p.mappedPreparedIDs[sourcePreparedID] = astraPreparedID
							log.Debugf("Mapped source PreparedID %s to Astra PreparedID %s", sourcePreparedID,
								astraPreparedID)
						}
					}
				}

				log.Debugf("Received success response from Astra from query (%s, %d)", clientIP, resp.Stream)
				delete(p.outstandingQueries[clientIP], resp.Stream)
			} else {
				log.Debugf("Received error response from Astra from query (%s, %d)", clientIP, resp.Stream)
				p.checkError(resp.RawBytes)
			}
		} else if useRespChan, ok := p.outstandingUses[clientIP][resp.Stream]; ok {
			useRespChan <- success
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
		p.Metrics.IncrementServerErrors()
	case 0x1100:
		// Write Timeout
		p.Metrics.IncrementWriteFails()
	case 0x1200:
		// Read Timeout
		p.Metrics.IncrementReadFails()
	}

}

func (p *CQLProxy) handlePrepareQuery(fromClause string, f *frame.Frame, client string, parsedPaths []string) error {
	keyspace, tableName := extractTableInfo(fromClause)

	// Is the keyspace already in the table clause of the query, or do we need to add it
	if keyspace == "" {
		keyspace = p.Keyspaces[client]
		if keyspace == "" {
			return errors.New("invalid keyspace")
		}
	}

	data := f.RawBytes
	idLength := binary.BigEndian.Uint16(data[9:11])
	preparedID := data[11 : 11+idLength]
	p.prepareIDToKeyspace[string(preparedID)] = keyspace

	table, ok := p.migrationStatus.Tables[keyspace][tableName]
	if !ok {
		return fmt.Errorf("table %s.%s does not exist", keyspace, tableName)
	}

	q := query.New(table, query.PREPARE, f, client, parsedPaths)
	return p.execute(q)
}

func (p *CQLProxy) handleUseQuery(keyspace string, f *frame.Frame, client string, parsedPaths []string) error {
	// Cassandra assumes case-insensitive unless keyspace is encased in quotation marks
	if strings.HasPrefix(keyspace, "\"") && strings.HasSuffix(keyspace, "\"") {
		keyspace = keyspace[1 : len(keyspace)-1]
	} else {
		keyspace = strings.ToLower(keyspace)
	}

	if _, ok := p.migrationStatus.Tables[keyspace]; !ok {
		return fmt.Errorf("keyspace %s does not exist", keyspace)
	}

	p.setKeyspace(client, keyspace)

	q := query.New(nil, query.USE, f, client, parsedPaths)

	return p.execute(q)
}

// HandleWriteQuery can handle QUERY and EXECUTE opcodes of type INSERT, UPDATE, DELETE, TRUNCATE
func (p *CQLProxy) handleWriteQuery(fromClause string, queryType query.Type, f *frame.Frame, client string, parsedPaths []string) error {
	keyspace, tableName := extractTableInfo(fromClause)

	// Is the keyspace already in the table clause of the query, or do we need to add it
	if keyspace == "" {
		if f.Opcode == 0x0a { //if execute
			data := f.RawBytes
			idLen := binary.BigEndian.Uint16(data[9:11])
			preparedID := string(data[11:(11 + idLen)])
			keyspace = p.prepareIDToKeyspace[preparedID]
		} else {
			keyspace = p.Keyspaces[client]
		}
		if keyspace == "" {
			return errors.New("invalid keyspace")
		}
	}

	table, ok := p.migrationStatus.Tables[keyspace][tableName]
	if !ok {
		return fmt.Errorf("table %s.%s does not exist", keyspace, tableName)
	}

	q := query.New(table, queryType, f, client, parsedPaths).UsingTimestamp()

	// If we have a write query that depends on all values already being present in the database,
	// if migration of this table is currently in progress (or about to begin), then pause consumption
	// of queries for this table.
	if queryType == query.UPDATE || queryType == query.TRUNCATE {
		p.checkStop(keyspace, tableName)
	}

	p.queueQuery(q)

	return nil
}

func (p *CQLProxy) handleBatchQuery(f *frame.Frame, paths []string, client string) error {
	batchWG := sync.WaitGroup{}
	queries := []*query.Query{}

	// Set to hold which tables we've already included queries for
	includedTables := make(map[string]bool)

	for i, path := range paths {
		fields := strings.Split(path, "/")
		keyspace, tableName := extractTableInfo(fields[3])
		if keyspace == "" {
			keyspace = p.Keyspaces[client]
			if keyspace == "" {
				return fmt.Errorf("invalid keyspace for batch query (%s, %d)", client, f.Stream)
			}
		}

		table, ok := p.migrationStatus.Tables[keyspace][tableName]
		if !ok {
			return fmt.Errorf("table %s.%s does not exist", keyspace, tableName)
		}

		explicitTable := fmt.Sprintf("%s.%s", keyspace, tableName)

		// Only create max one dummy query per table
		if _, ok := includedTables[explicitTable]; ok {
			continue
		}

		// Only put data in the first batch statement. The other batch statements act like dummy
		// statements so that each tables query are consumed only up until the point that the batch
		// statement was ran. This ensures that the state of the Astra database is consistent with the
		// state of the client database when the batch statement was queried.
		var q *query.Query
		if i == 0 {
			q = query.New(table, query.BATCH, f, client, paths).WithWaitGroup(&batchWG).UsingTimestamp()
		} else {
			q = query.New(table, query.BATCH, &frame.Frame{}, client, paths).WithWaitGroup(&batchWG)
		}
		batchWG.Add(1)

		// BATCH statements only contain INSERT, DELETE, and UPDATE. This stops any tables corresponding
		// to DELETE or UPDATE queries, as we need to ensure that they are fully migrated before we can
		// run those types of queries on them
		if query.Type(fields[2]) == query.UPDATE {
			p.checkStop(keyspace, tableName)
		}

		queries = append(queries, q)
		includedTables[explicitTable] = true
	}

	for _, q := range queries {
		p.queueQuery(q)
	}

	return nil
}

func (p *CQLProxy) queueQuery(query *query.Query) {
	queue := p.queues[query.Table.Keyspace][query.Table.Name]
	queue <- query

	queriesRemaining := len(queue)
	if queriesRemaining > 0 && queriesRemaining%priorityUpdateSize == 0 {
		err := p.updatePriority(query.Table, queriesRemaining)
		if err != nil {
			log.Errorf("Unable to send priority update (%s.%s : %d)",
				query.Table.Keyspace, query.Table.Name, queriesRemaining)
		}
	}
}

// consumeQueue executes all queries for a particular table, in the order that they are received.
func (p *CQLProxy) consumeQueue(keyspace string, table string) {
	log.Debugf("Beginning consumption of queries for %s.%s", keyspace, table)
	queue := p.queues[keyspace][table]

	for {
		select {
		case q := <-queue:
			p.queueLocks[keyspace][table].Lock()

			// Pauses all tables in a BATCH statement until it executes
			if q.Type == query.BATCH {
				q.WG.Done()
				q.WG.Wait()
			}

			err := p.executeWrite(q)
			if err != nil {
				// If for some reason, on the off chance that Astra cannot handle this query (but the client
				// database was able to), we must maintain consistency, so we need the migration service to
				// restart the migration of this table from the start, since the query is already reflected
				// in the client's database.
				log.Error(err)

				// Save queue length before we send the update, as once we send the update we have no
				// guarantees if the queries in the queue came before or after they restarted the migration,
				// and we don't want to delete any of them that occurred after the restart.
				queueLen := len(queue)

				err = p.sendTableRestart(q.Table)
				if err != nil {
					// If this happens, there is a much bigger issue with the current state of the entire
					// proxy service.
					// TODO: maybe just send hard restart to Migration Service & restart proxy
					log.Error(err)
				}

				// We can clear the queue (up to the point when we told the migration service to restart)
				// as we know that any queries that are currently in the queue were already executed on
				// the client's database and thus will already be reflected in the new migration of the table.
				for i := 0; i < queueLen; i++ {
					_ = <-queue
				}

			}

			p.queueLocks[keyspace][table].Unlock()
		}

	}
}

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

	p.Metrics.IncrementWrites()
	return nil
}

// executeAndCheckReply will send a query to the Astra DB, and listen for a response.
// Returns an error if the duration of queryTimeout passes without a response,
// or if the Astra DB responds saying that there was an error with the query.
func (p *CQLProxy) executeAndCheckReply(q *query.Query) error {
	resp := p.createResponseChan(q)
	defer p.deleteResponseChan(q)

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

	p.queryResponses[q.Stream] = make(chan bool, 1)
	return p.queryResponses[q.Stream]
}

func (p *CQLProxy) deleteResponseChan(q *query.Query) {
	p.lock.Lock()
	defer p.lock.Unlock()

	close(p.queryResponses[q.Stream])
	delete(p.queryResponses, q.Stream)
}

func (p *CQLProxy) execute(q *query.Query) error {
	session := p.getAstraSession(q.Source)
	p.sessionLocks[q.Source].Lock()
	defer p.sessionLocks[q.Source].Unlock()

	// switch to the keyspace that the query should be ran in
	if q.Type != query.MISC && q.Type != query.USE {
		if p.astraKeyspace[q.Source] != q.Table.Keyspace {
			if err := p.switchToQueryKeyspace(session, q); err != nil {
				return err
			}
		}
	}

	log.Debugf("Executing %v on Astra.", *q)

	var err error
	for i := 1; i <= 5; i++ {
		_, err := session.Write(q.Query)
		if err == nil {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	return err
}

func (p *CQLProxy) switchToQueryKeyspace(session net.Conn, q *query.Query, attempts ...int) error {
	attempt := 1
	if attempts != nil {
		attempt = attempts[0]
	}

	if attempt > maxQueryRetries {
		return fmt.Errorf("failed to switch to keyspace %s for query %d on session %s", q.Table.Keyspace, q.Stream, q.Source)
	}

	queryString := fmt.Sprintf("USE %s;", q.Table.Keyspace)

	// generate a random, unused StreamID for this USE query
	streamID := uint16(rand.Int())
	for _, ok := p.outstandingQueries[q.Source][streamID]; ok; _, ok = p.outstandingQueries[q.Source][streamID] {
		streamID = uint16(rand.Int())
	}

	// length of frame is header length + [long string] + 2 bytes for consistency + 1 byte for flags
	totalLen := cassHdrLen + 4
	useFrame := make([]byte, totalLen)

	useFrame[0] = q.Query[0]
	useFrame[1] = 0x00 // no flags
	binary.BigEndian.PutUint16(useFrame[2:4], streamID)
	useFrame[4] = 0x07
	binary.BigEndian.PutUint32(useFrame[5:9], 4+uint32(len(queryString)+2+1))
	binary.BigEndian.PutUint32(useFrame[9:13], uint32(len(queryString)))

	body := append([]byte(queryString), 0x00, 0x01, 0x00)
	useFrame = append(useFrame, body...)

	useRespChan := make(chan bool, 1)
	p.lock.Lock()
	p.outstandingUses[q.Source][streamID] = useRespChan
	p.lock.Unlock()

	_, err := session.Write(useFrame)
	if err != nil {
		return p.switchToQueryKeyspace(session, q, attempt+1)
	}

	if success := <-useRespChan; success {
		p.lock.Lock()
		defer p.lock.Unlock()

		close(useRespChan)
		delete(p.outstandingUses[q.Source], streamID)
		p.astraKeyspace[q.Source] = q.Table.Keyspace
		log.Debugf("Successfully switched to keyspace %s for query (%s, %d)", q.Table.Keyspace, q.Source, streamID)
		return nil
	} else {
		log.Debugf("Unable to switch to keyspace %s for query (%s, %d). Retrying...", q.Table.Keyspace, q.Source, streamID)
		return p.switchToQueryKeyspace(session, q, attempt+1)
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

		// TODO: Should probably restart the entire service if it's already running
		//p.restartIfRunning()
		p.MigrationStart <- &status
	case updates.TableUpdate:
		var tableUpdate migration.Table
		err := json.Unmarshal(update.Data, &tableUpdate)
		if err != nil {
			return errors.New("unable to unmarshal json")
		}

		if table, ok := p.migrationStatus.Tables[tableUpdate.Keyspace][tableUpdate.Name]; ok {
			table.Update(&tableUpdate)
			p.CheckStart(tableUpdate.Keyspace, tableUpdate.Name)
		} else {
			return fmt.Errorf("table %s.%s does not exist", tableUpdate.Keyspace, tableUpdate.Name)
		}

	case updates.Complete:
		p.MigrationDone <- struct{}{}
	case updates.Shutdown:
		p.ShutdownChan <- struct{}{}
	case updates.Success, updates.Failure:
		p.lock.Lock()
		if respChan, ok := p.outstandingUpdates[update.ID]; ok {
			respChan <- update.Type == updates.Success
		}
		p.lock.Unlock()
	}

	return nil
}

func (p *CQLProxy) sendTableRestart(table *migration.Table) error {
	marshaledTable, err := json.Marshal(table)
	if err != nil {
		return err
	}

	update := updates.New(updates.TableRestart, marshaledTable)
	return updates.Send(update, p.migrationSession)
}

// updatePriority will send a tableUpdate to the migration service with an
// updated priority value for the table. We do this as if there is a table
// with a very large queue, we want that to be migrated ASAP, so we communicate
// the priority of a table's migration
func (p *CQLProxy) updatePriority(table *migration.Table, newPriority int) error {
	table.SetPriority(newPriority)

	marshaledTable, err := json.Marshal(table)
	if err != nil {
		return err
	}

	update := updates.New(updates.TableUpdate, marshaledTable)
	return updates.Send(update, p.migrationSession)
}

func (p *CQLProxy) setKeyspace(clientIP string, keyspace string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.Keyspaces[clientIP] = keyspace
	p.astraKeyspace[clientIP] = keyspace
}

func (p *CQLProxy) getOutstandingQuery(clientIP string, streamID uint16) *frame.Frame {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.outstandingQueries[clientIP][streamID]
}

func (p *CQLProxy) getAstraSession(client string) net.Conn {
	p.lock.Lock()
	defer p.lock.Unlock()

	return p.astraSessions[client]
}

func (p *CQLProxy) CheckStart(keyspace string, tableName string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	status := p.tableStatus(keyspace, tableName)
	if p.tablePaused[keyspace][tableName] && status == migration.LoadingDataComplete {
		p.startTable(keyspace, tableName)
	}
}

func (p *CQLProxy) checkStop(keyspace string, tableName string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	status := p.tableStatus(keyspace, tableName)
	if !p.tablePaused[keyspace][tableName] && status >= migration.WaitingToUnload && status < migration.LoadingDataComplete {
		p.stopTable(keyspace, tableName)
	}
}

func (p *CQLProxy) getTable(keyspace string, tableName string) *migration.Table {
	p.migrationStatus.Lock.Lock()
	defer p.migrationStatus.Lock.Unlock()

	return p.migrationStatus.Tables[keyspace][tableName]
}

func (p *CQLProxy) tableStatus(keyspace string, tableName string) migration.Step {
	table := p.getTable(keyspace, tableName)
	table.Lock.Lock()
	defer table.Lock.Unlock()

	return table.Step
}

// stopTable grabs the queueLock for a table so that the corresponding consumeQueue
// function for the table cannot continue processing queries.
// Assumes caller has p.lock acquired.
func (p *CQLProxy) stopTable(keyspace string, tableName string) {
	log.Debugf("Stopping query consumption on %s.%s", keyspace, tableName)

	p.tablePaused[keyspace][tableName] = true
	p.queueLocks[keyspace][tableName].Lock()
}

// startTable releases the queueLock for a table so that the consumeQueue function
// can resume processing queries for the table.
// Assumes caller has p.lock acquired.
func (p *CQLProxy) startTable(keyspace string, tableName string) {
	log.Debugf("Starting query consumption on %s.%s", keyspace, tableName)

	p.tablePaused[keyspace][tableName] = false
	p.queueLocks[keyspace][tableName].Unlock()
}

// checkRedirect communicates over the ReadyForRedirect channel when migration is complete
// and there are no direct connections to the client's source DB any longer
// (envoy can point directly to the Astra DB now, skipping over proxy)
func (p *CQLProxy) checkRedirect() {
	if p.migrationComplete && p.Metrics.SourceConnections() == 0 {
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
	p.outstandingUses = make(map[string]map[uint16]chan bool)
	p.migrationComplete = p.Conf.MigrationComplete
	p.listeners = []net.Listener{}
	p.ReadyForRedirect = make(chan struct{})
	p.lock = &sync.Mutex{}
	p.Metrics = metrics.New(p.Conf.ProxyMetricsPort)
	p.Metrics.Expose()

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
	p.astraKeyspace = make(map[string]string)
	p.astraSessions = make(map[string]net.Conn)
	p.sessionLocks = make(map[string]*sync.Mutex)
	p.queuesCompleteCond = sync.NewCond(&sync.Mutex{})
	p.preparedIDs = make(map[uint16]string)
	p.mappedPreparedIDs = make(map[string]string)
	p.outstandingPrepares = make(map[uint16][]byte)
	p.preparedQueryBytes = make(map[string][]byte)
	p.prepareIDToKeyspace = make(map[string]string)
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

	return keyspace, tableName
}
