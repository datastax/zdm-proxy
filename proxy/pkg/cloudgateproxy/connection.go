package cloudgateproxy

import (
	"context"
	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"
	"net"
	"time"
)

// Establishes a TCP connection with the passed in IP. Retries using exponential backoff.
func establishConnection(ip string, ctx context.Context) (net.Conn, error) {
	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: false,
	}

	log.Infof("Attempting to connect to %v...", ip)
	dialer := net.Dialer{}
	for {
		conn, err := dialer.DialContext(ctx, "tcp", ip)
		if err != nil {
			select {
			case <-ctx.Done():
				return nil, ShutdownErr
			default:
				nextDuration := b.Duration()
				log.Errorf("Couldn't connect to %v, retrying in %v...", ip, nextDuration)
				time.Sleep(nextDuration)
				continue

			}
		}
		log.Infof("Successfully established connection with %v", conn.RemoteAddr())
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
func checkConnection(ip string, ctx context.Context) error {
	log.Infof("Opening test connection to %v", ip)

	// Wait until the source database is up and ready to accept TCP connections.
	dialer := net.Dialer{}
	originCassandra, err := dialer.DialContext(ctx, "tcp", ip)
	if err != nil {
		select {
		case <-ctx.Done():
			return ShutdownErr
		default:
			return err
		}
	}

	log.Infof("Closing test connection to %v", ip)
	originCassandra.Close()
	return nil
}
