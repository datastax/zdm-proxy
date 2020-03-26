package proxy

import (
	"cloud-gate/utils"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

type TableStatus string

const (
	CQLHeaderLength = 9
	CQLOpcodeByte   = 4
	CQLQueryOpcode  = 7
	CQLVersionByte  = 0

	WAITING   = TableStatus("waiting")
	MIGRATING = TableStatus("migrating")
	MIGRATED  = TableStatus("migrated")
)

type CQLProxy struct {
	SourceHostname   string
	SourceUsername   string
	SourcePassword   string
	SourcePort       int
	sourceHostString string

	AstraHostname string
	AstraUsername string
	AstraPassword string
	AstraPort     int
	astraHostString string

	ListenPort   int
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

	// TODO: Find a way to set this variable on startup
	//  Maybe save it as an environment variable that is read when the proxy starts up
	//  Or check if there are still credentials for a user DB as environment variables and
	//  if there isn't then we can assume migration has already happened / doesn't need to happen
	migrationComplete bool

	// When signal received on this channel, sets the migrationComplete variable to true
	MigrationCompleteChan chan struct{}

	// Metrics
	PacketCount int
	Reads       int
	Writes      int
}

// Temporary map & method until proper methods are created by the migration team
var tableStatuses map[string]TableStatus
func checkTable(table string) TableStatus {
	return tableStatuses[table]
}

// TODO: Maybe also save migration complete as an environment variable so it can be loaded
//  if the proxy crashes/restarts
func (p *CQLProxy) migrationCheck()  {
	if !p.migrationComplete {
		for {
			select {
			case <-p.MigrationCompleteChan:
				log.Infof("Migration Complete. Directing all new connections to Astra Database.")
				p.migrationComplete = true
				return
			}
		}
	}
}

// TODO: Handle case where connection closes, instead of panicking
func (p *CQLProxy) Listen() {
	p.clear()

	// Attempt to connect to astra database using given credentials
	// TODO: Maybe move where this happens?
	err := p.connect()
	if err != nil {
		panic(err)
	}
	defer p.Shutdown()

	// Begin migration completion loop
	go p.migrationCheck()

	// Let's operating system assign a random port to listen on if ListenPort isn't set (value == 0)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", p.ListenPort))
	if err != nil {
		panic(err)
	}

	defer l.Close()
	port := l.Addr().(*net.TCPAddr).Port
	log.Infof("Listening on port %d", port)

	for {
		conn, err := l.Accept()
		if err != nil {
			panic(err)
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

// TODO: Handle case where the migration is completed when there is an active connection
//  Close connection and reopen new one with the Astra DB as dst?
//  We'd have to get out of the blocking that happens on src.Read then
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
			panic(err)
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
			if b[CQLVersionByte] < 0x80 {
				if b[CQLOpcodeByte] == CQLQueryOpcode {
					go p.parseQuery(b)
				}
			}
		}

		p.PacketCount++
	}
}

// TODO: Deal with more cases
func (p *CQLProxy) parseQuery(b []byte) {
	// Trim off header portion of the query
	trimmed := b[CQLHeaderLength:]

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

func (p *CQLProxy) executeQuery(query string) {
	err := p.astraSession.Query(query).Exec()
	if err != nil {
		panic(err)
	}
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
	queue, ok := p.tableQueues[table]
	if !ok {
		// TODO: Move queue creation to startup so that we never need a lock for the map
		//  Multiple readers & no writers for map doesn't need a lock
		//  Finalize queue size to use
		queue = make(chan string, 1000)
		p.tableQueues[table] = queue
		go p.consumeQueue(table)
	}

	queue <- query
	p.queueSizes[table]++
	p.lock.Unlock()
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

			p.lock.Lock()
			p.executeQuery(query)
			p.queueSizes[table]--
			p.Writes++
			p.lock.Unlock()
		}
	}
}

func (p *CQLProxy) stopTable(table string) {
	p.lock.Lock()
	p.tableWaiting[table] = true
	p.lock.Unlock()
}

// Start Table query consumption once migration of a table has completed
func (p *CQLProxy) startTable(table string) {
	p.lock.Lock()
	p.tableWaiting[table] = false
	p.tableStarts[table] <- struct{}{}
	p.lock.Unlock()
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
	//TODO: create a loop to initialize all the channels in the beginning
	// once the migration service sends over the list of tables
	p.tableStarts["codebase"] = make(chan struct{})
	p.PacketCount = 0
	p.Reads = 0
	p.Writes = 0
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