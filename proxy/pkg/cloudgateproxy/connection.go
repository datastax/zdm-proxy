package cloudgateproxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"
	"net"
	"time"
)

type ConnectionConfig struct {
	metadataServiceHostName string
	metadataServicePort     string
	tlsConfig               *tls.Config
	connectionTimeoutMs     int
	sniProxyAddress         string
	cqlPort                 int
}

func NewConnectionConfig(tlsConfig *tls.Config, connectionTimeoutMs int, metadataServiceHostname string,
	metadataServicePort string, sniProxyAddress string, roundRobinPort int) *ConnectionConfig {
	return &ConnectionConfig{
		metadataServiceHostName: metadataServiceHostname,
		metadataServicePort:     metadataServicePort,
		tlsConfig:               tlsConfig,
		connectionTimeoutMs:     connectionTimeoutMs,
		sniProxyAddress:         sniProxyAddress,
		cqlPort:                 roundRobinPort,
	}
}

// version from zipped bundle
func initializeConnectionAndControlEndpointConfig(secureConnectBundlePath string, hostName string, port int, connTimeoutInMs int) (*ConnectionConfig, []EndpointConfig, error){
	var connConfig *ConnectionConfig
	controlConnEndpointConfigs := make([]EndpointConfig, 0)

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

		connConfig = NewConnectionConfig(tlsConfig, connTimeoutInMs, metadataServiceHostName, metadataServicePort,
										metadata.ContactInfo.SniProxyAddress, metadata.ContactInfo.RoundRobinPort)
		if err != nil {
			return nil, nil, err
		}

		// save all contact points as potential control connection endpoints so that if connecting to one fails it is possible to retry with the next one
		for _, contactPoint := range metadata.ContactInfo.ContactPoints {
			controlConnEndpointConfigs = append(controlConnEndpointConfigs, NewEndpointConfig(contactPoint, true, connConfig.cqlPort))
		}

	} else {
		connConfig = NewConnectionConfig(nil, connTimeoutInMs, "", "", "", port)
		controlConnEndpointConfigs = append(controlConnEndpointConfigs, NewEndpointConfig(hostName, false, port))
	}
	return connConfig, controlConnEndpointConfigs, nil
}

func (cc *ConnectionConfig) usesSNI() bool {
	return cc.sniProxyAddress != ""
}

func (cc *ConnectionConfig) getSNIConfig() *SNIConfig{
	if cc.tlsConfig == nil || cc.sniProxyAddress == "" {
		return nil
	}

	return &SNIConfig{
		SNIProxyAddress: cc.sniProxyAddress,
		tlsConfig:       cc.tlsConfig,
	}
}

func connectToFirstAvailableEndpoint(connectionConfig *ConnectionConfig, endpointConfigs []EndpointConfig, ctx context.Context, useBackoff bool) (net.Conn, EndpointConfig, error) {
	var conn net.Conn
	var connectedEndpointConfig EndpointConfig
	var err error
	connectionEstablished := false
	for _, endpointConfig := range endpointConfigs {
		err = nil
		conn, _, err = openConnection(connectionConfig, endpointConfig, ctx, useBackoff)
		if err != nil || conn == nil {
			// could not establish connection using this endpoint, try the next one
			log.Warnf("Could not establish a connection to endpoint %v due to %v, trying the next endpoint if available", endpointConfig.getEndpoint(), err)
			continue
		} else {
			// connection established, no need to try any remaining endpoints
			connectionEstablished = true
			connectedEndpointConfig = endpointConfig
			break
		}
	}

	if !connectionEstablished {
		return nil, nil, fmt.Errorf("could not connect to any of the endpoints provided")
	}

	return conn, connectedEndpointConfig, nil

}

func openConnection(cc *ConnectionConfig, ec EndpointConfig, ctx context.Context, useBackoff bool) (net.Conn, context.Context, error){
	var connection net.Conn
	var err error

	timeout := time.Duration(cc.connectionTimeoutMs) * time.Millisecond
	openConnectionTimeoutCtx, _ := context.WithTimeout(ctx, timeout)

	if cc.getSNIConfig() != nil {
		if ! ec.isSNI() {
			return nil, openConnectionTimeoutCtx, fmt.Errorf("Non-SNI endpoint specified with SNI connection configuration")
		}
		// open connection using SNI
		connection, err = openTLSConnectionWithSNI(ec.getEndpoint(), cc.getSNIConfig(), openConnectionTimeoutCtx, useBackoff)
		if err != nil {
			return nil, openConnectionTimeoutCtx, err
		}
		return connection, openConnectionTimeoutCtx, nil
	}

	if cc.tlsConfig != nil {
		// open connection using TLS
		connection, err = openTLSConnection(ec.getEndpoint(), cc.tlsConfig, openConnectionTimeoutCtx, useBackoff)
		if err != nil {
			return nil, openConnectionTimeoutCtx, err
		}
		return connection, openConnectionTimeoutCtx, nil

	}

	// open plain TCP connection using contact points
	if useBackoff {
		connection, err = openTCPConnectionWithBackoff(ec.getEndpoint(), openConnectionTimeoutCtx)
	} else {
		connection, err = openTCPConnection(ec.getEndpoint(), openConnectionTimeoutCtx)
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

	log.Infof("[openTCPConnectionWithBackoff] Attempting to connect to %v...", addr)
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
		log.Infof("[openTCPConnectionWithBackoff] Successfully established connection with %v", conn.RemoteAddr())
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

	return conn, nil
}

func openTLSConnection(addr string, tlsConfig *tls.Config, ctx context.Context, useBackoff bool) (*tls.Conn, error) {

	var tcpConn net.Conn
	var err error
	if useBackoff {
		tcpConn, err = openTCPConnectionWithBackoff(addr, ctx)
	} else {
		tcpConn, err = openTCPConnection(addr, ctx)
	}
	if err != nil {
		return nil, err
	}

	log.Infof("[openTLSConnection] Opening TLS connection to %v using underlying TCP connection", addr)
	tlsConn := tls.Client(tcpConn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		tlsConn.Close()
		return nil, err
	}
	return tlsConn, nil
}

func openTLSConnectionWithSNI(hostId string, sniConfig *SNIConfig, ctx context.Context, useBackoff bool) (*tls.Conn, error) {

	sniProxyHostname, _, err := net.SplitHostPort(sniConfig.SNIProxyAddress)
	if err != nil {
		return nil, err
	}

	tlsConfig := sniConfig.tlsConfig.Clone()

	//customVerify := func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
	//
	//	opts := x509.VerifyOptions{
	//		Roots:         tlsConfig.RootCAs,
	//		DNSName:       sniProxyHostname,
	//		Intermediates: x509.NewCertPool(),
	//	}
	//
	//	roots := x509.NewCertPool()
	//	for _, rawCert := range rawCerts {
	//		c, _ := x509.ParseCertificate(rawCert)
	//		certItem, _ := x509.ParseCertificate(rawCert)
	//		opts.Intermediates.AddCert(certItem)
	//		_, err := certItem.Verify(opts)
	//		if err != nil {
	//			return err
	//		}
	//		roots.AddCert(c)
	//	}
	//	return nil
	//}

	customVerify := func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
		certs := make([]*x509.Certificate, len(rawCerts))
		for i, asn1Data := range rawCerts {
			cert, err := x509.ParseCertificate(asn1Data)
			if err != nil {
				//c.sendAlert(alertBadCertificate)
				return errors.New("tls: failed to parse certificate from server: " + err.Error())
			}
			certs[i] = cert
		}

		opts := x509.VerifyOptions{
			Roots:         tlsConfig.RootCAs,
			CurrentTime:   time.Now(),
			DNSName:       sniProxyHostname,
			Intermediates: x509.NewCertPool(),
		}
		for _, cert := range certs[1:] {
			opts.Intermediates.AddCert(cert)
		}
		var err error
		verifiedChains, err = certs[0].Verify(opts)
		//if err != nil {
		//	c.sendAlert(alertBadCertificate)
			return err
		//}
	}

	tlsConfig.ServerName = hostId
	tlsConfig.InsecureSkipVerify = true	//This is required to evaluate our custom cert validation logic, otherwise it will not consider VerifyPeerCertificate and just run the standard cert logic, which fails as expected
	tlsConfig.VerifyPeerCertificate = customVerify
	log.Infof("[openTLSConnectionWithSNI] Opening TLS connection using SNI to %v using underlying TCP connection", hostId)
	tlsConn, err := openTLSConnection(sniConfig.SNIProxyAddress, tlsConfig, ctx, useBackoff)
	if err != nil {
		return nil, err
	}

	return tlsConn, nil
}

