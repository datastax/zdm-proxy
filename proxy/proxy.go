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
	// TODO: Maybe change to error channel so that the migrator can signal whether there were errors during the
	// 	migration process?
	MigrationCompleteChan chan struct{}

	// Channel that signals that the migrator has begun the unloading/loading process
	MigrationStartChan chan *MigrationStatus
	migrationStatus *MigrationStatus

	// Is the proxy ready to process queries from user?
	ready bool

	// Channel signalling that the proxy is now ready to process queries
	// TODO: Change ready channel to signal to coordinator when it can redirect envoy to point to this proxy
	// 	so that we can guarantee there won't be any queries sent to this until it is fully ready to accept them
	ReadyChan chan struct{}

	// Metrics
	PacketCount int
	Reads       int
	Writes      int

	WriteFails int
	ReadFails  int
}

// Temporary map & method until proper methods are created by the migration team
var tableStatuses map[string]TableStatus

func checkTable(table string)TableStatus {
	return tableStatuses[table]
}

func (p *CQLProxy) migrationCheck() {
	p.loadMigrationStatus()
	defer close(p.MigrationCompleteChan)

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
				// Maybe close MigrationStartChan here?
			case <-p.MigrationCompleteChan:
				log.Info("Migration Complete. Directing all new connections to Astra Database.")
				p.migrationComplete = true
				return
			}
		}
	}
}

func (p *CQLProxy) loadMigrationStatus() {
	envVar := os.Getenv("migration_complete")
	status, err := strconv.ParseBool(envVar)
	p.migrationComplete = status && err == nil

	log.Debugf("Migration Complete: %s", strconv.FormatBool(p.migrationComplete))
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
	defer p.Shutdown()

	// Begin migration completion loop
	go p.migrationCheck()
}

func (p *CQLProxy) Listen() {
	// Let's operating system assign a random port to listen on if Port isn't set (value == 0)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", p.Port))
	if err != nil {
		panic(err)
	}

	defer l.Close()
	port := l.Addr().(*net.TCPAddr).Port
	log.Infof("Proxy listening for packets on port %d", port)

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Error(err)
			continue
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
		panic(err)
	}

	// Begin two way packet forwarding
	go p.forward(conn, dst)
	go p.forward(dst, conn)

	log.Debugf("Connection established with %s", p.sourceHostString)
}

func (p *CQLProxy) forward(src, dst net.Conn) {
	defer src.Close()
	defer dst.Close()

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
			panic(err)
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

		p.PacketCount++
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
	}

	if err != nil {
		panic(err)
	}
}

// TODO: Error handling (possibly retry query / retry connect if connection closed)
func (p *CQLProxy) executeQuery(query string) error {
	err := p.astraSession.Query(query).Exec()
	if err != nil {
		return err
	}
	return nil
}

// Extract table name from insert query & add query to proper queue
func (p *CQLProxy) handleInsertQuery(query string) error {
	// TODO: Clean this up
	split := strings.Split(query, " ")
	tableName := split[2]
	tableName = tableName[:strings.IndexRune(tableName, '(')]

	if strings.Contains(tableName, ".") {
		sepIndex := strings.IndexRune(tableName, '.')
		tableName = tableName[sepIndex+1:]
	}

	p.addQueryToTableQueue(tableName, query)
	return nil
}

// Extract table name from update query & add query to proper queue
func (p *CQLProxy) handleUpdateQuery(query string) error {
	// TODO: See if there's a better way to go about doing this
	split := strings.Split(query, " ")
	tableName := split[1]

	if strings.Contains(tableName, ".") {
		sepIndex := strings.IndexRune(tableName, '.')
		tableName = tableName[sepIndex+1:]
	}

	// If we're doing an UPDATE and if the table isn't fully migrated, then the UPDATE query
	// may not be correct on the Astra DB as all the rows may not be fully imported. Thus,
	// if we reach an UPDATE query and the table isn't fully migrated, we stop running
	// queries on this table, keeping a queue of all the queries that come in, and then running them
	// in order once the table is finished being migrated
	if checkTable(tableName) != MIGRATED {
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

		time.Sleep(500 * time.Millisecond)
	}

	return err
}

func (p *CQLProxy) stopTable(table string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.tableWaiting[table] = true
}

// Start Table query consumption once migration of a table has completed
func (p *CQLProxy) startTable(table string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.tableWaiting[table] = false
	p.tableStarts[table] <- struct{}{}
}

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
	p.astraSession.Close()
}

func (p *CQLProxy) clear() {
	p.tableQueues = make(map[string]chan string)
	p.queueSizes = make(map[string]int)
	p.tableWaiting = make(map[string]bool)
	p.tableStarts = make(map[string]chan struct{})
	p.PacketCount = 0
	p.Reads = 0
	p.Writes = 0
	p.ready = false
	p.ReadyChan = make(chan struct{})
	p.lock = sync.Mutex{}
}

// function for testing purposes. Can be called from main to toggle table status for CODEBASE
func (p *CQLProxy) DoTestToggle() {
	if !p.tableWaiting["codebase"] {
		fmt.Println("------ stopping codebase queue!")
		p.stopTable("codebase")
	} else {
		fmt.Println("------ starting codebase queue!")
		p.startTable("codebase")
	}
}
