package cloudgateproxy

import (
	"context"
	"fmt"
	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"
	"io"
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
			if ctx.Err() != nil {
				return nil, ShutdownErr
			}
			nextDuration := b.Duration()
			log.Errorf("Couldn't connect to %v, retrying in %v...", ip, nextDuration)
			time.Sleep(nextDuration)
			continue
		}
		log.Infof("Successfully established connection with %v", conn.RemoteAddr())
		return conn, nil
	}
}

func writeToConnection(writer io.Writer, message []byte) error {
	_, err := writer.Write(message)
	if err != nil {
		return err
	}
	return nil
}

func openConnection(addr string, ctx context.Context) (net.Conn, error) {
	log.Infof("Opening connection to %v", addr)

	// Wait until the source database is up and ready to accept TCP connections.
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		if ctx.Err() == context.Canceled {
			return nil, fmt.Errorf("connection error (%v) but context was canceled (%v): %w", err, ctx.Err(), ShutdownErr)
		}
		return nil, err
	}

	return conn, nil
}
