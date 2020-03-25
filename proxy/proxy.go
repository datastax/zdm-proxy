package proxy

import (
	"cloud-gate/utils"
	"encoding/binary"
	"fmt"
	"github.com/gocql/gocql"
	"net"
	"strings"
)

type TableStatus string

const (
	CQLHeaderLength = 9
	CQLOpcodeByte   = 4
	CQLQueryOpcode  = 7
	CQLVersionByte  = 0

	WAITING = TableStatus("waiting")
	MIGRATING  = TableStatus("migrating")
	MIGRATED = TableStatus("migrated")
	)

type CQLProxy struct {
	SourceHostname    string
	SourceUsername    string
	SourcePassword    string
	SourcePort        int
	sourceHostString  string

	AstraHostname     string
	AstraUsername     string
	AstraPassword     string
	AstraPort         int

	ListenPort        int
	astraSession      *gocql.Session

	// Channel is closed once the migration status is MIGRATED
	tableQueues map[string]chan string

	// Able to consume from table?
	tableWaiting map[string]bool

	// Signals to restart consumption of queries for a table\
	tableStarts map[string]chan struct{}
}

// Temporary map & method until proper methods are created by the migration team
var tableStatuses map[string]TableStatus
func checkTable(table string) TableStatus {
	return tableStatuses[table]
}

func (p *CQLProxy) connect() error {
	session, err := utils.ConnectToCluster(p.AstraHostname, p.AstraUsername, p.AstraPassword, p.AstraPort)
	if err != nil {
		return err
	}
	p.astraSession = session

	return nil
}

func (p *CQLProxy) Listen() {
	// Attempt to connect to astra database using given credentials, if not connected
	// TODO: Maybe move where this happens?
	if p.astraSession == nil {
		err := p.connect()
		if err != nil {
			panic(err)
		}
	}

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

func (p *CQLProxy) runQuery(query string) {
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

	p.addToQueue(tableName, query)
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

	p.addToQueue(tableName, query)
	return nil
}

func (p *CQLProxy) addToQueue(table string, query string) {
	queue, ok := p.tableQueues[table]
	if !ok {
		queue = make(chan string)
		p.tableQueues[table] = queue
		go p.runQueue(table)
	}

	queue <- query
}

func (p *CQLProxy) runQueue(table string) {
	for {
		select {
		case query := <-p.tableQueues[table]:
			if p.tableWaiting[table] {
				<-p.tableStarts[table]
			}

			p.runQuery(query)
		}
	}
}

func (p *CQLProxy) stopTable(table string) {
	p.tableWaiting[table] = true
}

// Start Table query consumption once migration of a table has completed
func (p *CQLProxy) startTable(table string) {
	p.tableWaiting[table] = false
	p.tableStarts[table] <- struct{}{}
}

