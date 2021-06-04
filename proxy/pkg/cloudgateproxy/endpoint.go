package cloudgateproxy

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"time"
)

type Endpoint interface {
	GetSocketEndpoint() string
	GetTlsConfig() *tls.Config
	GetEndpointIdentifier()   string
}

type DefaultEndpoint struct {
	socketEndpoint string
}

func NewDefaultEndpoint(addr string, port int) *DefaultEndpoint {
	return &DefaultEndpoint{
		socketEndpoint: fmt.Sprintf("%s:%d", addr, port),
	}
}

func (recv *DefaultEndpoint) GetSocketEndpoint() string {
	 return recv.socketEndpoint
}

func (recv *DefaultEndpoint) GetTlsConfig() *tls.Config {
	// tls not supported for non sni/astra yet
	return nil
}

func (recv *DefaultEndpoint) GetEndpointIdentifier() string {
	return recv.socketEndpoint
}

func (recv *DefaultEndpoint) String() string {
	return recv.GetEndpointIdentifier()
}

type AstraEndpoint struct {
	proxyEndpoint string
	proxyAddr     string
	baseTlsConfig *tls.Config
	hostId        string
	endpointId    string
}

func NewAstraEndpoint(proxyEndpoint string, proxyAddr string, hostId string, baseTlsConfig *tls.Config) *AstraEndpoint {
	return &AstraEndpoint{
		proxyEndpoint: proxyEndpoint,
		proxyAddr:     proxyAddr,
		baseTlsConfig: baseTlsConfig,
		hostId:        hostId,
		endpointId:    fmt.Sprintf("%s-%s", proxyEndpoint, hostId),
	}
}

func (recv *AstraEndpoint) GetSocketEndpoint() string {
	return recv.proxyEndpoint
}

func (recv *AstraEndpoint) GetTlsConfig() *tls.Config {
	return &tls.Config{
		RootCAs:               recv.baseTlsConfig.RootCAs,
		Certificates:          recv.baseTlsConfig.Certificates,
		ServerName:            recv.hostId,
		InsecureSkipVerify:    true,
		VerifyPeerCertificate: recv.verifyCerts,
	}
}

func (recv *AstraEndpoint) GetEndpointIdentifier() string {
	return recv.endpointId
}

func (recv *AstraEndpoint) String() string {
	return recv.GetEndpointIdentifier()
}

func (recv *AstraEndpoint) verifyCerts(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
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
		Roots:         recv.baseTlsConfig.RootCAs,
		CurrentTime:   time.Now(),
		DNSName:       recv.proxyAddr,
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