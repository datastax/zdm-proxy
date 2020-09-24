package cloudgateproxy

import (
	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"
	"net"
	"time"
)

// Establishes a TCP connection with the passed in IP. Retries using exponential backoff.
func establishConnection(ip string) net.Conn {
	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: false,
	}

	log.Debugf("Attempting to connect to %s...", ip)
	for {
		conn, err := net.Dial("tcp", ip)
		if err != nil {
			nextDuration := b.Duration()
			log.Errorf("Couldn't connect to %s, retrying in %s...", ip, nextDuration.String())
			time.Sleep(nextDuration)
			continue
		}
		log.Infof("Successfully established connection with %s", conn.RemoteAddr())
		return conn
	}
}

func writeToConnection(connection net.Conn, message []byte) error {
	_, err := connection.Write(message)
	if err != nil {
		return err
	}
	return nil
}

// TODO: Is there a better way to check that we can connect to both databases?
func checkConnection(ip string) {
	// Wait until the source database is up and ready to accept TCP connections.
	originCassandra := establishConnection(ip)
	originCassandra.Close()

}