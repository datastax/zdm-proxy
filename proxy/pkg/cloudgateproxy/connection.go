package cloudgateproxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"
	"net"
	"time"
)

type ConnectionConfig struct {
	tlsConfig           *tls.Config
	connectionTimeoutMs int
	sniProxyEndpoint    string
	sniProxyAddr        string
	clusterType         ClusterType
	endpointFactory     func(*Host) Endpoint
}

func NewConnectionConfig(tlsConfig *tls.Config, connectionTimeoutMs int, sniProxyEndpoint string, sniProxyAddr string,
	clusterType ClusterType, endpointFactory func(*Host) Endpoint) *ConnectionConfig {
	return &ConnectionConfig{
		tlsConfig:           tlsConfig,
		connectionTimeoutMs: connectionTimeoutMs,
		sniProxyEndpoint:    sniProxyEndpoint,
		sniProxyAddr:        sniProxyAddr,
		clusterType:         clusterType,
		endpointFactory:     endpointFactory,
	}
}

// version from zipped bundle
func initializeConnectionConfig(secureConnectBundlePath string, contactPoints []string, port int, connTimeoutInMs int, clusterType ClusterType) (*ConnectionConfig, []Endpoint, error){
	var connConfig *ConnectionConfig
	controlConnEndpointConfigs := make([]Endpoint, 0)

	if secureConnectBundlePath != "" {

		fileMap, err := extractFilesFromZipArchive(secureConnectBundlePath)
		if err != nil {
			return nil, nil, err
		}

		metadataServiceHostName, metadataServicePort, err := parseHostAndPortFromSCBConfig(fileMap["config.json"])
		if err != nil {
			return nil, nil, err
		}

		if metadataServiceHostName == "" || metadataServicePort == "" {
			return nil, nil, fmt.Errorf("incomplete metadata service contact information. hostname: %v, port: %v", metadataServiceHostName, metadataServicePort)
		}

		tlsConfig, err := initializeTLSConfiguration(fileMap["ca.crt"], fileMap["cert"], fileMap["key"], metadataServiceHostName)
		if err != nil {
			return nil, nil, err
		}

		metadata, err := retrieveAstraMetadata(metadataServiceHostName, metadataServicePort, tlsConfig)
		if err != nil {
			return nil, nil, err
		}
		log.Debugf("Astra metadata parsed to: %v", metadata)

		sniProxyHostname, _, err := net.SplitHostPort(metadata.ContactInfo.SniProxyAddress)
		if err != nil {
			return nil, nil, fmt.Errorf("could not split sni proxy hostname and port: %w", err)
		}

		endpointFactory := func(h *Host) Endpoint {
			return NewAstraEndpoint(metadata.ContactInfo.SniProxyAddress, sniProxyHostname, h.HostId.String(), tlsConfig)
		}

		connConfig = NewConnectionConfig(tlsConfig, connTimeoutInMs, metadata.ContactInfo.SniProxyAddress, sniProxyHostname, clusterType, endpointFactory)

		// save all contact points as potential control connection endpoints so that if connecting to one fails it is possible to retry with the next one
		for _, hostIdContactPoint := range metadata.ContactInfo.ContactPoints {
			controlConnEndpointConfigs = append(controlConnEndpointConfigs, NewAstraEndpoint(connConfig.sniProxyEndpoint, connConfig.sniProxyAddr, hostIdContactPoint, connConfig.tlsConfig))
		}

	} else {
		endpointFactory := func(h *Host) Endpoint {
			return NewDefaultEndpoint(h.Address.String(), h.Port)
		}

		connConfig = NewConnectionConfig(nil, connTimeoutInMs, "", "", clusterType, endpointFactory)
		for _, contactPoint := range contactPoints {
			controlConnEndpointConfigs = append(controlConnEndpointConfigs, NewDefaultEndpoint(contactPoint, port))
		}
	}
	return connConfig, controlConnEndpointConfigs, nil
}

func (cc *ConnectionConfig) usesSNI() bool {
	return cc.sniProxyEndpoint != ""
}

func openConnection(cc *ConnectionConfig, ec Endpoint, ctx context.Context, useBackoff bool) (net.Conn, context.Context, error){
	var connection net.Conn
	var err error

	timeout := time.Duration(cc.connectionTimeoutMs) * time.Millisecond
	openConnectionTimeoutCtx, _ := context.WithTimeout(ctx, timeout)

	if cc.tlsConfig != nil {
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

