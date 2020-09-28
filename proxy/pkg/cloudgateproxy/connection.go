package cloudgateproxy

import (
	"context"
	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"
	"net"
	"time"
)

// Establishes a TCP connection with the passed in IP. Retries using exponential backoff.
func establishConnection(ip string, shutdownContext context.Context) (net.Conn, error) {
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
			select {
			case <-shutdownContext.Done():
				return nil, ShutdownErr
			default:
				nextDuration := b.Duration()
				log.Errorf("Couldn't connect to %s, retrying in %s...", ip, nextDuration.String())
				time.Sleep(nextDuration)
				continue

			}
		}
		log.Infof("Successfully established connection with %s", conn.RemoteAddr())
		return conn, nil
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
func checkConnection(ip string, shutdownContext context.Context) error {
	// Wait until the source database is up and ready to accept TCP connections.
	originCassandra, err := establishConnection(ip, shutdownContext)

	if err != nil {
		return err
	}

	originCassandra.Close()
	return nil
}