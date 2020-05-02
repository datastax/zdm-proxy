package main

import (
	"cloud-gate/integration-tests/test"
	"cloud-gate/migration/migration"
	"cloud-gate/updates"
	"cloud-gate/utils"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/jpillora/backoff"

	log "github.com/sirupsen/logrus"
)

// TestKeyspace is the dedicated keyspace for testing
// no other keyspaces will be modified
const TestKeyspace = "cloudgate_test"
const TestTable = "tasks"

func dropExistingKeyspaces(session *gocql.Session) {
	session.Query(fmt.Sprintf("DROP KEYSPACE %s;", TestKeyspace)).Exec()
}

func seedKeyspace(source *gocql.Session, dest *gocql.Session) {
	log.Info("Seeding keyspace...")
	source.Query(fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};", TestKeyspace)).Exec()
	dest.Query(fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};", TestKeyspace)).Exec()
}

func seedData(source *gocql.Session, dest *gocql.Session) {
	log.Info("Seeding tables...")
	// Create the table in source
	err := source.Query(fmt.Sprintf("CREATE TABLE %s.%s(id UUID, task text, PRIMARY KEY(id));", TestKeyspace, TestTable)).Exec()
	if err != nil {
		log.WithError(err).Error(err)
	}

	err = dest.Query(fmt.Sprintf("CREATE TABLE %s.%s(id UUID, task text, PRIMARY KEY(id));", TestKeyspace, TestTable)).Exec()
	if err != nil {
		log.WithError(err).Error(err)
	}

	// Seed data in source only
	ids := []string{
		"d1b05da0-8c20-11ea-9fc6-6d2c86545d91",
		"eed574b0-8c20-11ea-9fc6-6d2c86545d91",
		"cf0f4cf0-8c20-11ea-9fc6-6d2c86545d91",
		"d4b2ef40-8c20-11ea-9fc6-6d2c86545d91",
		"da9e7000-8c20-11ea-9fc6-6d2c86545d91",
		"cac98cf0-8c20-11ea-9fc6-6d2c86545d91",
		"c617ebc0-8c20-11ea-9fc6-6d2c86545d91",
		"d0b69450-8c20-11ea-9fc6-6d2c86545d91",
		"c3539ba0-8c20-11ea-9fc6-6d2c86545d91",
		"d2293720-8c20-11ea-9fc6-6d2c86545d91"}

	tasks := []string{
		"MSzZMTWA9hw6tkYWPTxT0XfGL9nGQUpy",
		"IH0FC3aWM4ynriOFvtr5TfiKxziR5aB1",
		"FgQfJesbNcxAebzFPRRcW2p1bBtoz1P1",
		"pzRxykuPkQ13oXv8cFzlOGsz51Zy68lS",
		"R3xkC90pKxGzkAQbmGGIYdI24Bo82RaL",
		"lsgVQ912cUYiL6TxR6ykiH7qRej6Lkfe",
		"So5O5ai4snEwD3aq1EXgzuoFeoUplLhD",
		"WW00H9IOC9JOslNwUVBKhCi3M2W9SfSD",
		"CFgmMRUzLcZdKuwdIKHPO5ohKJrThm5R",
		"g5qVsck8edpGfVA7NaHiSJOAmIGqo3dl",
	}

	// Seed 10 rows
	for i := 0; i < len(ids); i++ {
		id, task := ids[i], tasks[i]
		err = source.Query(fmt.Sprintf("INSERT INTO %s.%s(id, task) VALUES (%s, '%s');", TestKeyspace, TestTable, id, task)).Exec()
		if err != nil {
			log.WithError(err).Error(err)
		}
	}
}

func establishConnection(hostname string) net.Conn {
	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: false,
	}

	log.Info(fmt.Sprintf("Attempting to connect to Proxy Service at %s...", hostname))
	for {
		conn, err := net.Dial("tcp", hostname)
		if err != nil {
			nextDuration := b.Duration()
			log.Info(fmt.Sprintf("Couldn't connect to Proxy Service, retrying in %s...", nextDuration.String()))
			time.Sleep(nextDuration)
			continue
		}
		log.Info("Successfully established connection with Proxy Service")
		return conn
	}
}

func listenProxy() error {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", 15000))
	if err != nil {
		return err
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			default:
				log.Error(err)
				continue
			}
		}

		updates.CommunicationHandler(conn, conn, func(req *updates.Update) error {
			log.Info("RECEIVED UPDATE: ", req)
			return nil
		})
	}
}

func main() {
	// Connect to source and dest sessions
	var err error
	sourceSession, err := utils.ConnectToCluster("127.0.0.1", "", "", 9042)
	if err != nil {
		log.WithError(err).Error(err)
	}

	destSession, err := utils.ConnectToCluster("127.0.0.1", "", "", 9043)
	if err != nil {
		log.WithError(err).Error(err)
	}

	// Drop all existing data
	dropExistingKeyspaces(sourceSession)
	dropExistingKeyspaces(destSession)

	// Seed source and dest with keyspace
	seedKeyspace(sourceSession, destSession)

	// Seed source and dest w/ schema and data
	seedData(sourceSession, destSession)

	proxyCommand := exec.Command("go", "run", "./proxy/main.go")
	proxyCommand.Env = os.Environ()
	// log.Info(proxyCommand.Env)

	proxyOut, _ := proxyCommand.StdoutPipe()
	go printProxyOutput(proxyOut)
	go listenProxy()

	proxyCommand.Start()

	log.Info("PROXY STARTED")

	// Establish connection w/ proxy
	conn := establishConnection(fmt.Sprintf("127.0.0.1:14000"))

	// Make channel for listening to send complete request
	c := make(chan int)

	// Run test package here
	go test.Test1(c)

	// Listen for complete command from test
	go waitSendComplete(c, conn)

	for {
	}
}

func waitSendComplete(c chan int, conn net.Conn) {
	for {
		select {
		case <-c:
			// Send complete signal received
			sendComplete(conn)
		}
	}
}

func printProxyOutput(proxyOut io.ReadCloser) {
	for {
		var b []byte
		n, _ := proxyOut.Read(b)
		if n > 0 {
			log.Info(string(b))
		}
	}
}

func sendComplete(conn net.Conn) {
	status := migration.Status{
		Timestamp:  time.Now(),
		Tables:     make(map[string]map[string]*migration.Table),
		Steps:      1,
		TotalSteps: 1,
		Speed:      0,
		Lock:       new(sync.Mutex),
	}

	status.Tables[TestKeyspace] = make(map[string]*migration.Table)
	status.Tables[TestKeyspace][TestTable] = &migration.Table{
		Keyspace: TestKeyspace,
		Name:     TestTable,
		Step:     migration.LoadingDataComplete,
		Error:    nil,
		Priority: 0,
		Lock:     new(sync.Mutex),
	}

	bytes, err := json.Marshal(status)
	if err != nil {
		log.WithError(err).Fatal("Error marshalling status for complete signal")
	}

	req := updates.New(updates.Complete, bytes)
	err = updates.Send(req, conn)
	if err != nil {
		log.WithError(err).Errorf("Error sending request %s", req.ID)
	}
}
