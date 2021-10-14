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

func initializeTlsConfigFromProxyClusterTlsConfig(clusterTlsConfig *config.ClusterTlsConfig, clusterType ClusterType) (*tls.Config, error) {
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
	return initializeTlsConfiguration(serverCAFile, clientCertFile, clientKeyFile, "", clusterType)
}

func initializeTlsConfiguration(caCert []byte, cert []byte, key []byte, hostName string, clusterType ClusterType) (*tls.Config, error) {

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

	if cert == nil && key == nil {
		log.Debugf("Using one-way TLS for %s.", clusterType)
		return &tls.Config{RootCAs: rootCAs,
			ServerName:         hostName,
			InsecureSkipVerify: hostName == "",
		}, nil
	}

	// if using mTLS, both client cert and client key have to be specified
	var clientCert tls.Certificate
	if cert != nil && key != nil {
		clientCert, err = tls.X509KeyPair(cert, key)
		if err != nil {
			return nil, err
		}
	} else {
		if cert == nil {
			return nil, fmt.Errorf("Using mutual TLS for %s, but Client certificate was not specified.", clusterType)
		}
		if key == nil {
			return nil, fmt.Errorf("Using mutual TLS for %s, but Client key was not specified.", clusterType)
		}
	}

	log.Debugf("Using mutual TLS for %s.", clusterType)
	tlsConfig := tls.Config{RootCAs: rootCAs,
		Certificates:       []tls.Certificate{clientCert},
		ServerName:         hostName,
		InsecureSkipVerify: hostName == "",
	}

	return &tlsConfig, nil
}
