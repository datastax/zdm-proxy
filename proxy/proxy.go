package main

import (
	"cloud-gate/utils"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"net"
	"strings"
)

var (
	source_hostname    string
	source_username    string
	source_password    string
	source_port        int
	source_host_string string
	astra_hostname     string
	astra_username     string
	astra_password     string
	astra_port         int
	listen_port        int

	astraSession       *gocql.Session
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

	QUEUE_SIZE = 0xffff
)

func main() {
	parseFlags()

	var err error
	astraSession, err = utils.ConnectToCluster(astra_hostname, astra_username, astra_password, astra_port)
	if err != nil {
		log.Panicf("Unable to connect to Astra cluster (%s:%d, %s, %s)",
			astra_hostname, astra_port, astra_username, astra_password)
	}
	defer astraSession.Close()

	listen()
}

var tableStatuses map[string]TableStatus
func checkTable(table string) TableStatus {
	return tableStatuses[table]
}

// Channel is closed once the migration status is MIGRATED
var tableQueues map[string]chan string

// Able to consume from table?
var tableWaiting map[string]bool

// Signals to restart consumption of queries for a table
var tableStarts map[string]chan struct{}


func parseFlags() {
	flag.StringVar(&source_hostname, "source_hostname", "127.0.0.1", "Source Hostname")
	flag.StringVar(&source_username, "source_username", "", "Source Username")
	flag.StringVar(&source_password, "source_password", "", "Source Password")
	flag.IntVar(&source_port, "source_port", 9042, "Source Port")
	flag.StringVar(&astra_hostname, "astra_hostname", "127.0.0.1", "Astra Hostname")
	flag.StringVar(&astra_username, "astra_username", "", "Aster Username")
	flag.StringVar(&astra_password, "astra_password", "", "Astra Password")
	flag.IntVar(&astra_port, "astra_port", 9042, "Astra Port")
	flag.IntVar(&listen_port, "listen_port", 0, "Listening Port")
	flag.Parse()

	source_host_string = fmt.Sprintf("%s:%d", source_hostname, source_port)
}

func listen() {
	// Let the operating system assign us a random unused port
	// Probably change this in the future to take in a port as
	// a command line argument
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", listen_port))
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
		go handleRequest(conn)
	}

}

func handleRequest(conn net.Conn) {
	dst, err := net.Dial("tcp", source_host_string)
	if err != nil {
		panic(err)
	}

	// Begin two way packet forwarding
	go forward(conn, dst)
	go forward(dst, conn)

	fmt.Println("Connection established with ", source_host_string)
}

func forward(src, dst net.Conn) {
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
				go parseQuery(b)
			}
		}

	}
}

// Not close to being done (I don't think)
func parseQuery(b []byte) {
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
	var err error
	switch keyword {
	case "USE":
		// TODO: Make sure this is the correct approach for USE
		runQuery(query)
	case "INSERT":
		err = handleInsertQuery(query)
	case "UPDATE":
		err = handleUpdateQuery(query)
	}

	if err != nil {
		panic(err)
	}
}

func runQuery(query string) {
	err := astraSession.Query(query).Exec()
	if err != nil {
		panic(err)
	}
}

func handleInsertQuery(query string) error {
	// TODO: Clean this up
	split := strings.Split(query, " ")
	tableName := split[2]
	tableName = tableName[:strings.IndexRune(tableName, '(')]

	if strings.Contains(tableName, ".") {
		sepIndex := strings.IndexRune(tableName, '.')
		tableName = tableName[sepIndex+1:]
	}

	addToQueue(tableName, query)
	return nil
}

func handleUpdateQuery(query string) error {
	// TODO: See if there's a better way to about doing this
	split := strings.Split(query, " ")
	tableName := split[1]

	if strings.Contains(tableName, ".") {
		sepIndex := strings.IndexRune(tableName, '.')
		tableName = tableName[sepIndex+1:]
	}

	if checkTable(tableName) != MIGRATED {
		stopTable(tableName)
	}

	addToQueue(tableName, query)
	return nil
}

func addToQueue(table string, query string) {
	queue, ok := tableQueues[table]
	if !ok {
		queue = make(chan string)
		tableQueues[table] = queue
		go runQueue(table)
	}

	queue <- query
}

func runQueue(table string) {
	for {
		select {
		case query := <-tableQueues[table]:
			if tableWaiting[table] {
				<-tableStarts[table]
			}

			runQuery(query)
		}
	}
}

func stopTable(table string) {
	tableWaiting[table] = true
}

// Start Table query consumption once migration of a table has completed
func startTable(table string) {
	tableWaiting[table] = false
	tableStarts[table] <- struct{}{}
}

// Always add queries to queue for a table, take off queue to run
// After we've hit an UPDATE WHERE if the table isn't migrated, then stop running the queue until it is

