package proxy

import (
	"cloud-gate/utils"
	"encoding/binary"
	"fmt"
	"github.com/gocql/gocql"
	"net"
	"strings"
	"sync"
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

	ListenPort   int
	astraSession *gocql.Session

	// Channel is closed once the migration status is MIGRATED
	tableQueues map[string]chan string
	queueSizes  map[string]int

	// Currently consuming from table?
	tableWaiting map[string]bool

	// Signals to restart consumption of queries for a table
	tableStarts map[string]chan struct{}

	// Lock for maps/metrics
	// TODO: create more locks to improve performance
	lock sync.Mutex

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

// TODO: Handle case where connection closes, instead of panicking
func (p *CQLProxy) Listen() {
	// Attempt to connect to astra database using given credentials
	// TODO: Maybe move where this happens?
	err := p.connect()
	if err != nil {
		panic(err)
	}

	p.clear()

	// Let's operating system assign a random port to listen on if ListenPort isn't set (value == 0)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", p.ListenPort))
	if err != nil {
		panic(err)
	}

	defer l.Close()
	port := l.Addr().(*net.TCPAddr).Port
	fmt.Println("Listening on port ", port)

	for {
		conn, err := l.Accept()
		if err != nil {
			panic(err)
		}
		go p.handleRequest(conn)
	}

}

func (p *CQLProxy) handleRequest(conn net.Conn) {
	dst, err := net.Dial("tcp", p.sourceHostString)
	if err != nil {
		panic(err)
	}

	// Begin two way packet forwarding
	go p.forward(conn, dst)
	go p.forward(dst, conn)

	fmt.Println("Connection established with ", p.sourceHostString)
}

func (p *CQLProxy) forward(src, dst net.Conn) {
	defer src.Close()
	defer dst.Close()

	// What buffer size should we use?
	// Right now just using 0xffff as a placeholder, but the maximum request
	// that could be sent through the CQL wire protocol is 256mb
	buf := make([]byte, 0xffff)
	for {
		bytesRead, err := src.Read(buf)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Read %d bytes\n", bytesRead)

		b := buf[:bytesRead]
		bytesWritten, err := dst.Write(b)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Wrote %d bytes\n", bytesWritten)

		// Parse only if it's a request from the client:
		// 		First bit of version field is a 0 (< 0x80) (Big Endian)
		// AND
		// Parse only if it's a query:
		// 		OPCode is 0x07
		if b[CQLVersionByte] < 0x80 {
			if b[CQLOpcodeByte] == CQLQueryOpcode {
				go p.parseQuery(b)
			}
		}

		p.PacketCount++
	}
}

// Not close to being done (I don't think)
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
	// Figure out what to do with responses from writes
	// Add delete keyword + others
	var err error
	switch keyword {
	case "USE":
		// TODO: DEAL with the USE case
		// We cannot change the keyspace for a session dynamically, must change it before session
		// creation, thus we might have to do some weird stuff here, not exactly sure.
		// Maybe we can keep track of the current keyspace we're in?
		// --- But then there would be issues with all of the queuing and stuff if there's changes in keyspaces
		// Need to talk about this more.
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

func (p *CQLProxy) handleUpdateQuery(query string) error {
	// TODO: See if there's a better way to go about doing this
	split := strings.Split(query, " ")
	tableName := split[1]

	if strings.Contains(tableName, ".") {
		sepIndex := strings.IndexRune(tableName, '.')
		tableName = tableName[sepIndex+1:]
	}

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
		// TODO: Maybe move queue creation to startup so that we never need a lock for the map
		// Multiple readers & no writers for map doesn't need a lock
		queue = make(chan string, 1000)
		p.tableQueues[table] = queue
		go p.consumeQueue(table)
	}

	queue <- query
	p.queueSizes[table]++
	p.lock.Unlock()
}

func (p *CQLProxy) consumeQueue(table string) {
	// TODO: fix lock logic as not everything needs to be within lock
	p.lock.Lock()
	queue := p.tableQueues[table]
	p.lock.Unlock()
	for {
		select {
		case query := <-queue:

			if p.tableWaiting[table] {
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
	//TODO: initialization must take in SCHEME to initialize channels for each table
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