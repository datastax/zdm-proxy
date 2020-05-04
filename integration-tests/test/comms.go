package test

import (
	"cloud-gate/migration/migration"
	"cloud-gate/updates"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"
)

// EstablishConnection connects to Proxy Service
func EstablishConnection(hostname string) net.Conn {
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

// ListenProxy listens on port 15000
func ListenProxy() error {
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

// PrintProxyOutput logs prox
func PrintProxyOutput(proxyOut io.ReadCloser) {
	for {
		var b []byte
		n, _ := proxyOut.Read(b)
		if n > 0 {
			log.Info("PROXY: " + string(b))
		}
	}
}

// CreateStatusObject creates Status for testing
func CreateStatusObject(tableStep migration.Step) migration.Status {
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
		Step:     tableStep,
		Error:    nil,
		Priority: 0,
		Lock:     new(sync.Mutex),
	}

	return status
}

// SendStart sends a Start update
func SendStart(conn net.Conn) {
	bytes, err := json.Marshal(CreateStatusObject(migration.MigratingSchemaComplete))
	if err != nil {
		log.WithError(err).Fatal("Error marshalling status for start signal")
	}

	SendRequest(updates.New(updates.Start, bytes), conn)
}

// SendTableUpdate sends a TableUpdate update
func SendTableUpdate(step migration.Step, conn net.Conn) {
	status := CreateStatusObject(step)
	bytes, err := json.Marshal(status.Tables[TestKeyspace][TestTable])
	if err != nil {
		log.WithError(err).Fatal("Error marshalling table for update")
	}

	SendRequest(updates.New(updates.TableUpdate, bytes), conn)
}

// SendMigrationComplete sends a Complete update
func SendMigrationComplete(conn net.Conn) {
	bytes, err := json.Marshal(CreateStatusObject(migration.LoadingDataComplete))
	if err != nil {
		log.WithError(err).Fatal("Error marshalling status for complete signal")
	}

	SendRequest(updates.New(updates.Complete, bytes), conn)
}

// SendRequest will notify the proxy service about the migration progress
func SendRequest(req *updates.Update, conn net.Conn) {
	err := updates.Send(req, conn)
	if err != nil {
		log.WithError(err).Errorf("Error sending request %s", req.ID)
	}
	log.Info(fmt.Sprintf("SEND UPDATE: Type %d; ID %s", req.Type, req.ID))

	log.Info("Sleeping...")
	time.Sleep(time.Second * 2)
}
