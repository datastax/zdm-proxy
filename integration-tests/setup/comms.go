package setup

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/riptano/cloud-gate/updates"

	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"
)

var reqMap = make(map[string]chan bool)

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
			log.WithError(err).Info(fmt.Sprintf("Couldn't connect to Proxy Service, retrying in %s...", nextDuration.String()))
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
			reqMap[req.ID] <- req.Type == updates.Success
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

//// CreateStatusObject creates Status for testing
//func CreateStatusObject() migration.Status {
//	status := migration.Status{
//		Timestamp: time.Now(),
//		Tables:    make(map[string]map[string]*migration.Table),
//		Lock:      new(sync.Mutex),
//	}
//
//	status.Tables[TestKeyspace] = make(map[string]*migration.Table)
//	for _, tableName := range TestTables {
//		status.Tables[TestKeyspace][tableName] = &migration.Table{
//			Keyspace: TestKeyspace,
//			Name:     tableName,
//			Step:     migration.MigratingSchemaComplete,
//			Error:    nil,
//			Lock:     new(sync.Mutex),
//		}
//	}
//
//	return status
//}

//// SendStart sends a Start update
//func SendStart(conn net.Conn, status migration.Status) {
//	bytes, err := json.Marshal(status)
//	if err != nil {
//		log.WithError(err).Fatal("Error marshalling status for start signal")
//	}
//
//	SendRequest(updates.New(updates.Start, bytes), conn)
//}
//
//// SendTableUpdate sends a TableUpdate update
//func SendTableUpdate(conn net.Conn, table *migration.Table) {
//	bytes, err := json.Marshal(table)
//	if err != nil {
//		log.WithError(err).Fatal("Error marshalling table for update")
//	}
//
//	SendRequest(updates.New(updates.TableUpdate, bytes), conn)
//}
//
//// SendMigrationComplete sends a Complete update
//func SendMigrationComplete(conn net.Conn, status migration.Status) {
//	bytes, err := json.Marshal(status)
//	if err != nil {
//		log.WithError(err).Fatal("Error marshalling status for complete signal")
//	}
//
//	SendRequest(updates.New(updates.Complete, bytes), conn)
//}

// SendRequest will notify the proxy service about the migration progress
func SendRequest(req *updates.Update, conn net.Conn) {
	reqMap[req.ID] = make(chan bool)
	err := updates.Send(req, conn)
	if err != nil {
		log.WithError(err).Errorf("Error sending request %s", req.ID)
	}
	log.Info(fmt.Sprintf("SEND UPDATE: Type %d; ID %s", req.Type, req.ID))

	<-reqMap[req.ID]
}
