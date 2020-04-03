package proxy

import (
	"cloud-gate/utils"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)


const (
	cqlHeaderLength = 9
	cqlOpcodeByte   = 4
	cqlQueryOpcode  = 7
	cqlVersionByte  = 0

	WAITING   = TableStatus("WAITING")
	MIGRATING = TableStatus("MIGRATING")
	MIGRATED  = TableStatus("MIGRATED")
	ERROR     = TableStatus("ERROR")

	// TODO: Finalize queue size to use
	queueSize = 1000
)

// These three migration/table structs will be transferred over to the migrator package
type TableStatus string

type Table struct {
	Name     string
	Keyspace string
	Status   TableStatus
	Error    error

	Priority int
}

type MigrationStatus struct {
	Tables map[string]Table

	PercentComplete int
	Speed           int

	Lock sync.Mutex
}

type CQLProxy struct {
	SourceHostname   string
	SourceUsername   string
	SourcePassword   string
	SourcePort       int
	sourceHostString string

	AstraHostname   string
	AstraUsername   string
	AstraPassword   string
	AstraPort       int
	astraHostString string

	Port   int
	listener net.Listener
	astraSession *gocql.Session

	// TODO: Make maps support multiple keyspaces (ex: map[string]map[string]chan string)
	tableQueues map[string]chan string
	queueSizes  map[string]int

	// Should we wait on this table (do we need to wait for migration to finish before we run anymore queries)
	tableWaiting map[string]bool

	// Signals to restart consumption of queries for a particular table
	tableStarts map[string]chan struct{}

	// Lock for maps/metrics
	// TODO: (maybe) create more locks to improve performance
	lock sync.Mutex

	migrationComplete bool

	// Channel that signals that the migrator has finished the migration process.
	MigrationCompleteChan chan struct{}

	// Channel that signals that the migrator has begun the unloading/loading process
	MigrationStartChan chan *MigrationStatus
	migrationStatus *MigrationStatus

	// Is the proxy ready to process queries from user?
	ready bool

	// Channel signalling that the proxy is now ready to process queries
	ReadyChan chan struct{}

	// Number of open connections to the Client's Database
	connectionsToSource int

	// Channel to signal when the Proxy should stop all forwarding and close all connections
	ShutdownChan chan struct{}
	shutdown chan struct{}

	// Channel to signal to coordinator that there are no more open connections to the Client's Database
	// and that the coordinator can redirect Envoy to point directly to Astra without any negative side effects
	ReadyForRedirect chan struct{}

	// Metrics
	PacketCount int
	Reads       int
	Writes      int

	WriteFails int
	ReadFails  int
}

func (p *CQLProxy) migrationCheck() {
	envVar := os.Getenv("migration_complete")
	status, err := strconv.ParseBool(envVar)
	p.migrationComplete = status && err == nil

	log.Debugf("Migration Complete: %s", strconv.FormatBool(p.migrationComplete))

	if !p.migrationComplete {
		log.Info("Proxy waiting for migration start signal.")
		for {
			select {
			case status := <-p.MigrationStartChan:
				log.Info("Proxy received migration info.")
				p.migrationStatus = status
				p.initQueues()
				p.ready = true
				p.ReadyChan <- struct{}{}
				log.Info("Proxy sent ready signal.")

			case <-p.MigrationCompleteChan:
				log.Info("Migration Complete. Directing all new connections to Astra Database.")
				p.migrationComplete = true

			case <-p.ShutdownChan:
				log.Info("Proxy shutting down...")
				p.Shutdown()
				return
			}
		}
	}
}

func (p *CQLProxy) initQueues() {
	for tableName := range p.migrationStatus.Tables {
			p.tableQueues[tableName] = make(chan string, queueSize)
			p.tableStarts[tableName] = make(chan struct{})
	}
	log.Debug("Proxy queues initialized.")
}

func (p *CQLProxy) Start() {
	p.clear()

	// Attempt to connect to astra database using given credentials
	err := p.connect()
	if err != nil {
		panic(err)
	}

	go p.migrationCheck()
}

func (p *CQLProxy) Listen() {
	// Let's operating system assign a random port to listen on if Port isn't set (value == 0)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", p.Port))
	if err != nil {
		panic(err)
	}
	p.listener = l

	defer l.Close()
	port := l.Addr().(*net.TCPAddr).Port
	log.Infof("Proxy listening for packets on port %d", port)

	for {
		conn, err := l.Accept()
		if err != nil {
			// TODO: Is there a better way to do this?
			select {
			case <-p.shutdown:
				return
			default:
				log.Error(err)
				continue
			}
		}
		go p.handleRequest(conn)
	}
}

func (p *CQLProxy) handleRequest(conn net.Conn) {
	var hostname string
	if p.migrationComplete {
		hostname = p.astraHostString
	} else {
		hostname = p.sourceHostString
	}

	dst, err := net.Dial("tcp", hostname)
	if err != nil {
		log.Error(err)
		return
	}

	if hostname == p.sourceHostString {
		p.incrementSources()
	}

	// Begin two way packet forwarding
	go p.forward(conn, dst)
	go p.forward(dst, conn)

	log.Debugf("Connection established with %s", conn.RemoteAddr())
}

func (p *CQLProxy) forward(src, dst net.Conn) {
	defer src.Close()
	defer dst.Close()

	if dst.RemoteAddr().String() == p.sourceHostString {
		defer p.decrementSources()
	}

	// TODO: Finalize buffer size
	// 	Right now just using 0xffff as a placeholder, but the maximum request
	// 	that could be sent through the CQL wire protocol is 256mb, so we should accommodate that, unless there's
	// 	an issue with that
	buf := make([]byte, 0xffff)
	for {
		bytesRead, err := src.Read(buf)
		if err != nil {
			if err != io.EOF {
				log.Debugf("%s disconnected", src.RemoteAddr())
			}
			return
		}
		log.Debugf("Read %d bytes", bytesRead)

		b := buf[:bytesRead]
		bytesWritten, err := dst.Write(b)
		if err != nil {
			log.Error(err)
			continue
		}
		log.Debugf("Wrote %d bytes", bytesWritten)

		// We only want to mirror writes if the migration is not complete,
		// OR if the migration is complete, but this connection is still directly connected to the
		// user's DB (ex: migration is finished while the user is actively connected to the proxy)
		// TODO: Can possibly get rid of the !p.migrationComplete part of the condition
		if !p.migrationComplete || dst.RemoteAddr().String() == p.sourceHostString {
			// Parse only if it's a request from the client:
			// 		First bit of version field is a 0 (< 0x80)
			// AND
			// Parse only if it's a query:
			// 		OPCode is 0x07
			if b[cqlVersionByte] < 0x80 {
				if b[cqlOpcodeByte] == cqlQueryOpcode {
					p.parseQuery(b)
				}
			}
		}

		p.lock.Lock()
		p.PacketCount++
		p.lock.Unlock()
	}
}

// TODO: Deal with more cases
func (p *CQLProxy) parseQuery(b []byte) {
	// Trim off header portion of the query
	trimmed := b[cqlHeaderLength:]

	// Find length of query body
	queryLen := binary.BigEndian.Uint32(trimmed[:4])

	// Splice out query body
	query := string(trimmed[4 : 4+queryLen])

	// Get keyword of the query
	// Currently only supports queries of the type "KEYWORD -------------"
	keyword := strings.ToUpper(query[0:strings.IndexRune(query, ' ')])

	// Check if keyword is a write (add more later)
	// TODO: Figure out what to do with responses from writes
	// 	Add delete keyword + others
	var err error
	switch keyword {
	case "USE":
		// TODO: DEAL with the USE case
		//  We cannot change the keyspace for a session dynamically, must change it before session
		//  creation, thus we might have to do some weird stuff here, not exactly sure.
		//  Maybe we can keep track of the current keyspace we're in?
		//  --- But then there would be issues with all of the queuing and stuff if there's changes in keyspaces
		//  Need to talk about this more.
		break
	case "INSERT":
		err = p.handleInsertQuery(query)
	case "UPDATE":
		err = p.handleUpdateQuery(query)
	case "DELETE":
		err = p.handleDeleteQuery(query)
	case "TRUNCATE":
		err = p.handleTruncateQuery(query)
	}

	// TODO: Maybe add errors if needed, and don't just panic
	if err != nil {
		panic(err)
	}
}

func (p *CQLProxy) executeQuery(query string) error {
	err := p.astraSession.Query(query).Exec()
	if err != nil {
		return err
	}
	return nil
}

func (p *CQLProxy) handleTruncateQuery(query string) error {
	split := strings.Split(strings.ToUpper(query), " ")
	// Invalid query, don't run
	// TODO: maybe return an error or something, if necessary
	if len(split) < 2 {
		return nil
	}

	// Two possibilities for a TRUNCATE query:
	//    TRUNCATE keyspace.table;
	// OR
	//	  TRUNCATE TABLE keyspace.table;
	var tableName string
	if split[1] == "TABLE" {
		tableName = extractTableName(split[2])
	} else {
		tableName = extractTableName(split[1])
	}

	if p.tableStatus(tableName) != MIGRATED {
		p.stopTable(tableName)
	}

	p.addQueryToTableQueue(tableName, query)
	return nil
}

func (p *CQLProxy) handleDeleteQuery(query string) error {
	split := strings.Split(query, " ")

	// Query must be invalid, don't run
	if len(split) < 5 {
		return nil
	}

	var tableName string
	for i, v := range split {
		if v == "FROM" {
			tableName = extractTableName(split[i+1])
			break
		}
	}

	// Invalid query, don't run
	if tableName == "" {
		return nil
	}

	// Wait for migration of table to be finished before processing anymore queries
	if p.tableStatus(tableName) != MIGRATED {
		p.stopTable(tableName)
	}

	p.addQueryToTableQueue(tableName, query)
	return nil
}

// Extract table name from insert query & add query to proper queue
func (p *CQLProxy) handleInsertQuery(query string) error {
	// TODO: Clean this up
	split := strings.Split(query, " ")

	// Query must be invalid, ignore
	if len(split) < 5 {
		return nil
	}

	tableName := extractTableName(split[2])

	p.addQueryToTableQueue(tableName, query)
	return nil
}

// Extract table name from update query & add query to proper queue
func (p *CQLProxy) handleUpdateQuery(query string) error {
	// TODO: See if there's a better way to go about doing this
	split := strings.Split(query, " ")

	// Query must be invalid, ignore
	if len(split) < 6 {
		return nil
	}

	tableName := extractTableName(split[1])


	// Wait for migration of table to be finished before processing anymore queries
	if p.tableStatus(tableName) != MIGRATED {
		p.stopTable(tableName)
	}

	p.addQueryToTableQueue(tableName, query)
	return nil
}

func (p *CQLProxy) addQueryToTableQueue(table string, query string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	queue, ok := p.tableQueues[table]
	if !ok {
		// TODO: Remove once we verify that the queue creation works w/ the migration service
		// 	aka the queue should never be nil and ok should always be true
		queue = make(chan string, queueSize)
		p.tableQueues[table] = queue
		p.tableStarts[table] = make(chan struct{})
		go p.consumeQueue(table)
	}

	queue <- query
	p.queueSizes[table]++
}

func (p *CQLProxy) consumeQueue(table string) {
	// TODO: Get rid of this locking once we add queue creation at startup
	p.lock.Lock()
	queue := p.tableQueues[table]
	p.lock.Unlock()

	for {
		select {
		case query := <-queue:
			p.lock.Lock()
			waiting := p.tableWaiting[table]
			p.lock.Unlock()

			if waiting {
				<-p.tableStarts[table]
			}

			// Driver is async, so we don't need a lock around query execution
			err := p.executeQuery(query)
			if err != nil {
				log.Debugf("Query %s failed, retrying", query)
				err = p.retry(query, 5)
				// TODO: Figure out exactly what to do if we're unable to write
				// 	If it's a bad query, no issue, but if it's a good query that isn't working for some reason
				// 	we need to figure out what to do
				if err != nil {
					log.Error(err)

					p.lock.Lock()
					p.WriteFails++
					p.lock.Unlock()
				}
			}

			p.lock.Lock()
			p.queueSizes[table]--
			p.Writes++
			p.lock.Unlock()
		}
	}
}

// TODO: Make better
func (p *CQLProxy) retry(query string, attempts int) error {
	var err error
	for i := 1; i <= attempts; i++ {
		log.Debugf("Retrying %s attempt #%d", query, i)

		err = p.executeQuery(query)
		if err == nil {
			break
		}
		// This is an arbitrary duration for now, probably want to change in future
		time.Sleep(500 * time.Millisecond)
	}

	return err
}

func (p *CQLProxy) tableStatus(tableName string) TableStatus {
	table := p.migrationStatus.Tables[tableName]
	p.migrationStatus.Lock.Lock()
	status := table.Status
	p.migrationStatus.Lock.Unlock()
	return status
}

// Stop consuming queries for a given table
func (p *CQLProxy) stopTable(table string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.tableWaiting[table] = true
}

// Restart consuming queries for a given table
func (p *CQLProxy) startTable(table string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.tableWaiting[table] = false
	p.tableStarts[table] <- struct{}{}
}

func (p *CQLProxy) incrementSources() {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.connectionsToSource++
}

func (p *CQLProxy) decrementSources() {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.connectionsToSource--

	if p.migrationComplete && p.connectionsToSource == 0 {
		log.Debug("No more connections to client database; ready for redirect.")
		p.ReadyForRedirect <- struct{}{}
	}
}

// TODO: Maybe add a couple retries, or let the caller deal with that?
func (p *CQLProxy) connect() error {
	session, err := utils.ConnectToCluster(p.AstraHostname, p.AstraUsername, p.AstraPassword, p.AstraPort)
	if err != nil {
		return err
	}
	p.astraSession = session

	p.sourceHostString = fmt.Sprintf("%s:%d", p.SourceHostname, p.SourcePort)
	p.astraHostString = fmt.Sprintf("%s:%d", p.AstraHostname, p.AstraPort)
	return nil
}

func (p *CQLProxy) Shutdown() {
	p.shutdown <- struct{}{}
	p.listener.Close()
	p.astraSession.Close()
}

func (p *CQLProxy) clear() {
	p.tableQueues = make(map[string]chan string)
	p.queueSizes = make(map[string]int)
	p.tableWaiting = make(map[string]bool)
	p.tableStarts = make(map[string]chan struct{})
	p.connectionsToSource = 0
	p.PacketCount = 0
	p.Reads = 0
	p.Writes = 0
	p.ready = false
	p.ReadyChan = make(chan struct{})
	p.ShutdownChan = make(chan struct{})
	p.shutdown = make(chan struct{})
	p.ReadyForRedirect = make(chan struct{})
	p.lock = sync.Mutex{}
}

// Given a FROM argument, extract the table name
// ex: table, keyspace.table, keyspace.table;, keyspace.table(, etc..
func extractTableName(s string) string {
	// Remove semicolon if it is attached to the table name from the query
	if i := strings.IndexRune(s, ';'); i != -1 {
		s = s[:i]
	}

	// Remove keyspace if table in format keyspace.table
	if i := strings.IndexRune(s, '.'); i != -1 {
		s = s[i+1:]
	}

	// Remove column names if part of an INSERT query: ex: TABLE(col, col)
	if i := strings.IndexRune(s, '('); i != -1 {
		s = s[:i]
	}

	return s
}
