package proxy

import (
	"cloud-gate/utils"
	"encoding/binary"
	"errors"
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
	cqlBufferSize   = 0xffffffff

	WAITING   = TableStatus("WAITING")
	MIGRATING = TableStatus("MIGRATING")
	MIGRATED  = TableStatus("MIGRATED")
	ERROR     = TableStatus("ERROR")

	USE      = QueryType("USE")
	INSERT   = QueryType("INSERT")
	UPDATE   = QueryType("UPDATE")
	DELETE   = QueryType("DELETE")
	TRUNCATE = QueryType("TRUNCATE")

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
	Lock     *sync.Mutex
}

type MigrationStatus struct {
	Tables map[string]map[string]*Table

	PercentComplete int
	Speed           int

	Lock *sync.Mutex
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

	Port     int
	listener net.Listener
	Sessions map[string]*gocql.Session

	tableQueues map[string]map[string]chan *Query
	queueSizes  map[string]map[string]int

	// Should we wait on this table (do we need to wait for migration to finish before we run anymore queries)
	tableWaiting map[string]map[string]bool

	// Signals to restart consumption of queries for a particular table
	tableStarts map[string]map[string]chan struct{}

	// TODO: (maybe) create more locks to improve performance
	lock *sync.Mutex

	migrationComplete bool

	// Channel that signals that the migrator has finished the migration process.
	MigrationCompleteChan chan struct{}

	// Channel that signals that the migrator has begun the unloading/loading process
	MigrationStartChan chan *MigrationStatus
	migrationStatus    *MigrationStatus

	// Channel that the migration service will send us tables that have finished migrating
	// so that we can restart their queue consumption if it was paused
	TableMigratedChan chan *Table

	// Is the proxy ready to process queries from user?
	ready bool

	// Channel signalling that the proxy is now ready to process queries
	ReadyChan chan struct{}

	// Number of open connections to the Client's Database
	connectionsToSource int

	// Channel to signal when the Proxy should stop all forwarding and close all connections
	ShutdownChan chan struct{}
	shutdown     chan struct{}

	// Channel to signal to coordinator that there are no more open connections to the Client's Database
	// and that the coordinator can redirect Envoy to point directly to Astra without any negative side effects
	ReadyForRedirect chan struct{}

	// Keeps track of the current keyspace queries are being ran in
	Keyspace string

	// Metrics
	Metrics Metrics
}

type QueryType string

type Query struct {
	Table *Table

	Type  QueryType
	Query string
}

// TODO: Maybe change migration_complete to migration_in_progress, so that we can turn on proxy before migration
// 	starts (if it ever starts), and it will just redirect to Astra normally.
func (p *CQLProxy) migrationLoop() {
	envVar := os.Getenv("migration_complete")
	status, err := strconv.ParseBool(envVar)
	if err != nil {
		log.Error(err)
	}
	p.migrationComplete = status && err == nil

	log.Debugf("Migration Complete: %t", p.migrationComplete)

	if !p.migrationComplete {
		log.Info("Proxy waiting for migration start signal.")
		for {
			select {
			case status := <-p.MigrationStartChan:
				p.start(status)
				log.Info("Proxy ready to consume queries.")

			case table := <-p.TableMigratedChan:
				p.startTable(table.Keyspace, table.Name)
				log.Debugf("Restarted query consumption on table %s.%s", table.Keyspace, table.Name)

			case <-p.MigrationCompleteChan:
				p.migrationComplete = true
				log.Info("Migration Complete. Directing all new connections to Astra Database.")

			case <-p.ShutdownChan:
				log.Info("Proxy shutting down...")
				p.Shutdown()
				return
			}
		}
	}
}

func (p *CQLProxy) start(status *MigrationStatus) {
	for keyspace, tables := range status.Tables {
		p.tableQueues[keyspace] = make(map[string]chan *Query)
		p.tableStarts[keyspace] = make(map[string]chan struct{})
		p.tableWaiting[keyspace] = make(map[string]bool)
		p.queueSizes[keyspace] = make(map[string]int)
		for tableName := range tables {
			p.tableQueues[keyspace][tableName] = make(chan *Query, queueSize)
			p.tableStarts[keyspace][tableName] = make(chan struct{})

			go p.consumeQueue(keyspace, tableName)
		}
	}

	p.migrationStatus = status
	p.ReadyChan <- struct{}{}
	p.ready = true

	log.Info("Proxy ready to execute queries.")
}

func (p *CQLProxy) Start() error {
	p.reset()

	// Attempt to connect to astra database using given credentials
	session, err := p.connect(p.Keyspace)
	if err != nil {
		return err
	}
	p.Sessions[p.Keyspace] = session

	go p.migrationLoop()

	return nil
}

func (p *CQLProxy) Listen() error {
	// Let's operating system assign a random port to listen on if Port isn't set (value == 0)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", p.Port))
	if err != nil {
		return err
	}
	p.listener = l

	defer l.Close()
	port := l.Addr().(*net.TCPAddr).Port
	log.Infof("Proxy listening for packets on port %d", port)

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-p.shutdown:
				return nil
			default:
				log.Error(err)
				continue
			}
		}
		go p.handleRequest(conn)
	}
}

func (p *CQLProxy) handleRequest(conn net.Conn) {
	hostname := p.sourceHostString
	if p.migrationComplete {
		hostname = p.astraHostString
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

}

func (p *CQLProxy) forward(src, dst net.Conn) {
	defer src.Close()
	defer dst.Close()

	if dst.RemoteAddr().String() == p.sourceHostString {
		defer p.decrementSources()
	}

	buf := make([]byte, cqlBufferSize)
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

		b := buf[:bytesRead]
		_, err = dst.Write(b)
		if err != nil {
			log.Error(err)
			continue
		}

		// We only want to mirror writes if this connection is still directly connected to the
		// user's DB
		if !p.migrationComplete || dst.RemoteAddr().String() == p.sourceHostString {
			// Parse only if it's a request from the client:
			// 		First bit of version field is a 0 (< 0x80)
			// AND
			// Parse only if it's a query:
			// 		OPCode is 0x07
			if b[cqlVersionByte] < 0x80 && b[cqlOpcodeByte] == cqlQueryOpcode {
				err := p.parseQuery(b)
				if err != nil {
					// TODO: Handle error
					log.Error(err)
				}
			}
		}

		p.Metrics.incrementPackets()
	}
}

// TODO: FIX BUG: When the user uses a function such as now(), since it is calculated on the databases side,
// 	the client DB and astra will have different values.
func (p *CQLProxy) parseQuery(b []byte) error {
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
	// TODO: When we're extracting keyspaces, if the keyspace doesn't have a gocql session attributed to it,
	// 	we need to start one up (this is for the idea of having a mapping from keyspace to gocql session)
	switch keyword {
	case "USE":
		return p.handleUseQuery(query)
	case "INSERT":
		return p.handleInsertQuery(query)
	case "UPDATE":
		return p.handleUpdateQuery(query)
	case "DELETE":
		return p.handleDeleteQuery(query)
	case "TRUNCATE":
		return p.handleTruncateQuery(query)
	}

	return nil
}

func (p *CQLProxy) executeQuery(q *Query) error {
	session, ok := p.Sessions[q.Table.Keyspace]
	if !ok {
		return errors.New("ran query on nonexistent session")
	}

	err := session.Query(q.Query).Exec()
	if err != nil {
		return err
	}

	// For testing
	log.Debugf("Executing %v on keyspace %s", *q, q.Table.Keyspace)
	return nil
}

func (p *CQLProxy) handleUseQuery(query string) error {
	split := strings.Split(query, " ")

	// Invalid query, don't run
	if len(split) < 2 {
		return nil
	}

	// Remove trailing semicolon if it's attached
	keyspace := strings.TrimSuffix(split[1], ";")

	// If surrounded by quotation marks, case sensitive, else not case sensitive
	if strings.HasPrefix(keyspace, "\"") && strings.HasSuffix(keyspace, "\"") {
		keyspace = keyspace[1 : len(keyspace)-1]
	} else {
		keyspace = strings.ToLower(keyspace)
	}

	log.Debugf("Attempting to connect to keyspace %s", keyspace)
	// Open new session if this is the first time we're using this keyspace
	// Will error out on an invalid keyspace, so we know after this if statement that the keyspace is valid
	if _, ok := p.Sessions[keyspace]; !ok {
		newSession, err := p.connect(keyspace)
		if err != nil {
			return err
		}
		p.Sessions[keyspace] = newSession
	}

	p.Keyspace = keyspace

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
	var keyspace string
	var tableName string
	if split[1] == "TABLE" {
		keyspace, tableName = extractTableInfo(split[2])
	} else {
		keyspace, tableName = extractTableInfo(split[1])
	}
	if keyspace == "" {
		keyspace = p.Keyspace
	}

	table, ok := p.migrationStatus.Tables[keyspace][tableName]
	if !ok {
		return fmt.Errorf("table %s.%s does not exist", keyspace, tableName)
	}

	if p.tableStatus(keyspace, tableName) != MIGRATED {
		p.stopTable(keyspace, tableName)
	}

	q := &Query{
		Table: table,
		Type:  TRUNCATE,
		Query: query}

	p.queueQuery(q)
	return nil
}

func (p *CQLProxy) handleDeleteQuery(query string) error {
	split := strings.Split(query, " ")

	// Query must be invalid, don't run
	if len(split) < 5 {
		return nil
	}

	// TODO: must make query all upper to do if v == "FROM"
	var keyspace string
	var tableName string
	for i, v := range split {
		if v == "FROM" {
			keyspace, tableName = extractTableInfo(split[i+1])
			break
		}
	}

	if keyspace == "" {
		keyspace = p.Keyspace
	}

	table, ok := p.migrationStatus.Tables[keyspace][tableName]
	if !ok {
		return fmt.Errorf("table %s.%s does not exist", keyspace, tableName)
	}

	// Wait for migration of table to be finished before processing anymore queries
	if p.tableStatus(keyspace, tableName) != MIGRATED {
		p.stopTable(keyspace, tableName)
	}

	q := &Query{
		Table: table,
		Type:  DELETE,
		Query: query}

	p.queueQuery(q)
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

	keyspace, tableName := extractTableInfo(split[2])
	if keyspace == "" {
		keyspace = p.Keyspace
	}

	table, ok := p.migrationStatus.Tables[keyspace][tableName]
	if !ok {
		return fmt.Errorf("table %s.%s does not exist", keyspace, tableName)
	}

	q := &Query{
		Table: table,
		Type:  INSERT,
		Query: query}

	p.queueQuery(q)
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

	keyspace, tableName := extractTableInfo(split[1])
	if keyspace == "" {
		keyspace = p.Keyspace
	}

	table, ok := p.migrationStatus.Tables[keyspace][tableName]
	if !ok {
		return fmt.Errorf("table %s.%s does not exist", keyspace, tableName)
	}

	// Wait for migration of table to be finished before processing anymore queries
	if p.tableStatus(keyspace, tableName) != MIGRATED {
		p.stopTable(keyspace, tableName)
	}

	q := &Query{
		Table: table,
		Type:  UPDATE,
		Query: query}

	p.queueQuery(q)
	return nil
}

func (p *CQLProxy) queueQuery(query *Query) {
	p.tableQueues[query.Table.Keyspace][query.Table.Name] <- query

	p.lock.Lock()
	defer p.lock.Unlock()
	p.queueSizes[query.Table.Keyspace][query.Table.Name]++
}

func (p *CQLProxy) consumeQueue(keyspace string, table string) {
	log.Debugf("Beginning consumption of queries for %s.%s", keyspace, table)

	for {
		select {
		case query := <-p.tableQueues[keyspace][table]:
			p.lock.Lock()
			waiting := p.tableWaiting[keyspace][table]
			p.lock.Unlock()

			if waiting {
				<-p.tableStarts[keyspace][table]
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

					p.Metrics.incrementWriteFails()
				}
			}

			p.lock.Lock()
			p.queueSizes[keyspace][table]--
			p.lock.Unlock()

			p.Metrics.incrementWrites()
		}
	}
}

// TODO: Make better
func (p *CQLProxy) retry(query *Query, attempts int) error {
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

func (p *CQLProxy) tableStatus(keyspace string, tableName string) TableStatus {
	table := p.migrationStatus.Tables[keyspace][tableName]
	table.Lock.Lock()
	defer table.Lock.Unlock()

	status := table.Status
	return status
}

// Stop consuming queries for a given table
func (p *CQLProxy) stopTable(keyspace string, table string) {
	log.Debugf("Stopping query consumption on %s.%s", keyspace, table)
	p.lock.Lock()
	defer p.lock.Unlock()

	p.tableWaiting[keyspace][table] = true
}

// Restart consuming queries for a given table
func (p *CQLProxy) startTable(keyspace string, table string) {
	log.Debugf("Restarting query consumption on %s.%s", keyspace, table)
	p.lock.Lock()
	defer p.lock.Unlock()

	p.tableWaiting[keyspace][table] = false
	p.tableStarts[keyspace][table] <- struct{}{}
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
func (p *CQLProxy) connect(keyspace string) (*gocql.Session, error) {
	session, err := utils.ConnectToCluster(p.AstraHostname, p.AstraUsername, p.AstraPassword, p.AstraPort, keyspace)
	if err != nil {
		return nil, err
	}

	return session, err
}

func (p *CQLProxy) Shutdown() {
	p.shutdown <- struct{}{}
	p.listener.Close()
	for _, session := range p.Sessions {
		session.Close()
	}
}

func (p *CQLProxy) reset() {
	p.Sessions = make(map[string]*gocql.Session)
	p.tableQueues = make(map[string]map[string]chan *Query)
	p.queueSizes = make(map[string]map[string]int)
	p.tableWaiting = make(map[string]map[string]bool)
	p.tableStarts = make(map[string]map[string]chan struct{})
	p.ready = false
	p.ReadyChan = make(chan struct{})
	p.ShutdownChan = make(chan struct{})
	p.shutdown = make(chan struct{})
	p.ReadyForRedirect = make(chan struct{})
	p.connectionsToSource = 0
	p.lock = &sync.Mutex{}
	p.Metrics = Metrics{}
	p.Metrics.lock = &sync.Mutex{}
	p.sourceHostString = fmt.Sprintf("%s:%d", p.SourceHostname, p.SourcePort)
	p.astraHostString = fmt.Sprintf("%s:%d", p.AstraHostname, p.AstraPort)
}

// Given a FROM argument, extract the table name
// ex: table, keyspace.table, keyspace.table;, keyspace.table(, etc..
func extractTableInfo(fromClause string) (string, string) {
	var keyspace string

	// Remove keyspace if table in format keyspace.table
	if i := strings.IndexRune(fromClause, '.'); i != -1 {
		keyspace = fromClause[:i]
	}

	tableName := fromClause

	// Remove semicolon if it is attached to the table name from the query
	if i := strings.IndexRune(tableName, ';'); i != -1 {
		tableName = tableName[:i]
	}

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

type Metrics struct {
	PacketCount int
	Reads       int
	Writes      int

	WriteFails int
	ReadFails  int

	lock *sync.Mutex
}

func (m *Metrics) incrementPackets() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.PacketCount++
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
