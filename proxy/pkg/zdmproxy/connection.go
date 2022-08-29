package zdmproxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"
	"net"
	"time"
)

func openConnection(cc ConnectionConfig, ec Endpoint, ctx context.Context, useBackoff bool) (net.Conn, context.Context, error) {
	var connection net.Conn
	var err error

	timeout := time.Duration(cc.GetConnectionTimeoutMs()) * time.Millisecond
	openConnectionTimeoutCtx, _ := context.WithTimeout(ctx, timeout)

	if cc.GetTlsConfig() != nil {
		// open connection using TLS
		connection, err = openTLSConnection(ec, openConnectionTimeoutCtx, useBackoff)
		if err != nil {
			return nil, openConnectionTimeoutCtx, err
		}
		return connection, openConnectionTimeoutCtx, nil
	}

	// open plain TCP connection using contact points
	if useBackoff {
		connection, err = openTCPConnectionWithBackoff(ec.GetSocketEndpoint(), openConnectionTimeoutCtx)
	} else {
		connection, err = openTCPConnection(ec.GetSocketEndpoint(), openConnectionTimeoutCtx)
	}

	return connection, openConnectionTimeoutCtx, err
}

func openTCPConnectionWithBackoff(addr string, ctx context.Context) (net.Conn, error) {
	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: false,
	}

	log.Debugf("[openTCPConnectionWithBackoff] Attempting to connect to %v...", addr)
	dialer := net.Dialer{}
	for {
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ShutdownErr
			}
			nextDuration := b.Duration()
			log.Errorf("[openTCPConnectionWithBackoff] Couldn't connect to %v, retrying in %v...", addr, nextDuration)
			time.Sleep(nextDuration)
			continue
		}
		log.Debugf("[openTCPConnectionWithBackoff] Successfully established connection with %v", conn.RemoteAddr())
		return conn, nil
	}
}

func openTCPConnection(addr string, ctx context.Context) (net.Conn, error) {
	log.Infof("[openTCPConnection] Opening connection to %v", addr)

	// Wait until the source database is up and ready to accept TCP connections.
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		if ctx.Err() == context.Canceled {
			return nil, fmt.Errorf("[openTCPConnection] Connection error (%v) but context was canceled (%v): %w", err, ctx.Err(), ShutdownErr)
		}
		return nil, err
	}
	log.Infof("[openTCPConnection] Successfully established connection with %v", conn.RemoteAddr())

	return conn, nil
}

func openTLSConnection(endpoint Endpoint, ctx context.Context, useBackoff bool) (*tls.Conn, error) {

	var tcpConn net.Conn
	var err error
	if useBackoff {
		tcpConn, err = openTCPConnectionWithBackoff(endpoint.GetSocketEndpoint(), ctx)
	} else {
		tcpConn, err = openTCPConnection(endpoint.GetSocketEndpoint(), ctx)
	}
	if err != nil {
		return nil, err
	}

	log.Infof("[openTLSConnection] Opening TLS connection to %v using underlying TCP connection", endpoint.GetEndpointIdentifier())
	tlsConn := tls.Client(tcpConn, endpoint.GetTlsConfig())
	if err := tlsConn.Handshake(); err != nil {
		tlsConn.Close()
		return nil, err
	}
	log.Infof("[openTLSConnection] Successfully established connection with %v", endpoint.GetEndpointIdentifier())

	return tlsConn, nil
}
