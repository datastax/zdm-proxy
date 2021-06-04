package cloudgateproxy

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	log "github.com/sirupsen/logrus"
)

func initializeTLSConfiguration(caCert []byte, cert []byte, key []byte, hostName string) (*tls.Config, error) {

	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}

	if caCert != nil {
		ok := rootCAs.AppendCertsFromPEM(caCert)
		if !ok {
			return nil, fmt.Errorf("the provided CA cert could not be added to the rootCAs")
		}
	} else {
		log.Warnf("CA cert was not specified")
	}

	var clientCert tls.Certificate
	if cert != nil && key != nil {
		clientCert, err = tls.X509KeyPair(cert, key)
		if err != nil {
			return nil, err
		}
	} else {
		if cert == nil {
			log.Warnf("Client certificate was not specified.")
		}
		if key == nil {
			log.Warnf("Client key was not specified.")
		}
	}

	tlsConfig := tls.Config{RootCAs: rootCAs,
							Certificates: []tls.Certificate{clientCert},
							ServerName: hostName}

	return &tlsConfig, nil
}
