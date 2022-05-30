package cloudgateproxy

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"runtime"
)

func loadTlsFile(filePath string) ([]byte, error) {
	var file []byte
	var err error
	if filePath != "" {
		file, err = ioutil.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("could not load file with path %s due to: %v", filePath, err)
		}
	}
	return file, err
}

func getClientSideTlsConfigFromProxyClusterTlsConfig(clusterTlsConfig *config.ClusterTlsConfig, clusterType ClusterType) (*tls.Config, error) {
	// create tls config object using the values provided in the cluster security config
	serverCAFile, err := loadTlsFile(clusterTlsConfig.ServerCaPath)
	if err != nil {
		return nil, err
	}
	clientCertFile, err := loadTlsFile(clusterTlsConfig.ClientCertPath)
	if err != nil {
		return nil, err
	}
	clientKeyFile, err := loadTlsFile(clusterTlsConfig.ClientKeyPath)
	if err != nil {
		return nil, err
	}
	// currently not supporting server hostname verification for non-Astra clusters
	return getClientSideTlsConfig(serverCAFile, clientCertFile, clientKeyFile, "", "", clusterType)
}

func getClientSideTlsConfig(
	caCert []byte, cert []byte, key []byte, serverName string, dnsName string, clusterType ClusterType) (*tls.Config, error) {

	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		if runtime.GOOS == "windows" {
			rootCAs = x509.NewCertPool()
			err = nil
		} else {
			return nil, err
		}
	}

	// if TLS is used, server CA must always be specified
	if caCert != nil {
		ok := rootCAs.AppendCertsFromPEM(caCert)
		if !ok {
			return nil, fmt.Errorf("the provided CA cert could not be added to the rootCAs")
		}
	} else {
		// this should be caught when validating the configuration
		log.Warnf("Using TLS for %s, but CA cert was not specified", clusterType)
	}

	var clientCerts []tls.Certificate
	if cert == nil && key == nil {
		log.Debugf("Using one-way TLS for %s.", clusterType)
	} else {
		log.Debugf("Using mutual TLS for %s.", clusterType)
		// if using mTLS, both client cert and client key have to be specified
		if cert == nil {
			return nil, fmt.Errorf("using mutual TLS for %s, but Client certificate was not specified", clusterType)
		}
		if key == nil {
			return nil, fmt.Errorf("using mutual TLS for %s, but Client key was not specified", clusterType)
		}
		clientCert, err := tls.X509KeyPair(cert, key)
		if err != nil {
			return nil, err
		}
		clientCerts = []tls.Certificate{clientCert}
	}

	return getClientSideTlsConfigFromParsedCerts(rootCAs, clientCerts, serverName, dnsName), nil
}

func getClientSideTlsConfigFromParsedCerts(rootCAs *x509.CertPool, clientCerts []tls.Certificate, serverName string, dnsName string) *tls.Config {
	var verifyConnectionCallback func(cs tls.ConnectionState) error
	if serverName != dnsName || serverName == "" {
		verifyConnectionCallback = getClientSideVerifyConnectionCallback(dnsName, rootCAs)
	}

	tlsConfig := &tls.Config{
		RootCAs:            rootCAs,
		Certificates:       clientCerts,
		ServerName:         serverName,
		InsecureSkipVerify: verifyConnectionCallback != nil,
		VerifyConnection:   verifyConnectionCallback,
	}

	return tlsConfig
}

func getClientSideVerifyConnectionCallback(certificateDnsName string, rootCAs *x509.CertPool) func(cs tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		dnsName := cs.ServerName
		if certificateDnsName != "" {
			dnsName = certificateDnsName
		}
		opts := x509.VerifyOptions{
			DNSName:       dnsName,
			Roots:         rootCAs,
		}
		if len(cs.PeerCertificates) > 0 {
			opts.Intermediates = x509.NewCertPool()
			for _, cert := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(cert)
			}
		}
		_, err := cs.PeerCertificates[0].Verify(opts)
		return err
	}
}

func getServerSideTlsConfigFromProxyClusterTlsConfig(proxyTlsConfig *config.ProxyTlsConfig) (*tls.Config, error) {
	// create tls config object using the values provided in the cluster security config
	proxyCaFile, err := loadTlsFile(proxyTlsConfig.ProxyCaPath)
	if err != nil {
		return nil, err
	}
	proxyCertFile, err := loadTlsFile(proxyTlsConfig.ProxyCertPath)
	if err != nil {
		return nil, err
	}
	proxyCertKey, err := loadTlsFile(proxyTlsConfig.ProxyKeyPath)
	if err != nil {
		return nil, err
	}
	// currently not supporting server hostname verification for client connections
	return getServerSideTlsConfig(proxyCaFile, proxyCertFile, proxyCertKey, proxyTlsConfig.ClientAuth)
}

func getServerSideTlsConfig(
	caCert []byte, cert []byte, key []byte, clientAuth bool) (*tls.Config, error) {

	// if proxy TLS is used, proxy CA must always be specified
	rootCAs := x509.NewCertPool()
	ok := rootCAs.AppendCertsFromPEM(caCert)
	if !ok {
		return nil, fmt.Errorf("the provided proxy CA cert could not be added to the rootCAs")
	}

	serverCert, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}
	serverCerts := []tls.Certificate{serverCert}

	if clientAuth {
		log.Debug("Using mutual TLS for client connections.")
	} else {
		log.Debug("Using one-way TLS for client connections.")
	}

	return getServerSideTlsConfigFromParsedCerts(rootCAs, serverCerts, clientAuth), nil
}

func getServerSideTlsConfigFromParsedCerts(
	rootCAs *x509.CertPool, serverCerts []tls.Certificate, clientAuth bool) *tls.Config {

	var verifyConnectionCallback func(cs tls.ConnectionState) error
	var clientAuthType tls.ClientAuthType
	if clientAuth {
		verifyConnectionCallback = getServerSideVerifyConnectionCallback(rootCAs)
		clientAuthType = tls.RequireAnyClientCert
	} else {
		verifyConnectionCallback = nil
		clientAuthType = tls.NoClientCert
	}

	tlsConfig := &tls.Config{
		ClientAuth:       clientAuthType,
		VerifyConnection: verifyConnectionCallback,
		RootCAs:          rootCAs,
		Certificates:     serverCerts,
	}

	return tlsConfig
}

func getServerSideVerifyConnectionCallback(rootCAs *x509.CertPool) func(cs tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		opts := x509.VerifyOptions{
			DNSName:       "",
			Intermediates: x509.NewCertPool(),
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			Roots:         rootCAs,
		}
		for _, cert := range cs.PeerCertificates[1:] {
			opts.Intermediates.AddCert(cert)
		}
		_, err := cs.PeerCertificates[0].Verify(opts)
		return err
	}
}