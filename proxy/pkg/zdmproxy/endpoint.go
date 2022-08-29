package zdmproxy

import (
	"crypto/tls"
	"fmt"
)

type Endpoint interface {
	GetSocketEndpoint() string
	GetTlsConfig() *tls.Config
	GetEndpointIdentifier() string
	String() string
}

type DefaultEndpoint struct {
	socketEndpoint 	string
	tlsConfig		*tls.Config
}

func NewDefaultEndpoint(addr string, port int, tlsConfig *tls.Config) *DefaultEndpoint {
	return &DefaultEndpoint{
		socketEndpoint: fmt.Sprintf("%s:%d", addr, port),
		tlsConfig: tlsConfig,
	}
}

func (recv *DefaultEndpoint) GetSocketEndpoint() string {
	 return recv.socketEndpoint
}

func (recv *DefaultEndpoint) GetTlsConfig() *tls.Config {
	return recv.tlsConfig
}

func (recv *DefaultEndpoint) GetEndpointIdentifier() string {
	return recv.socketEndpoint
}

func (recv *DefaultEndpoint) String() string {
	return recv.socketEndpoint
}

type AstraEndpoint struct {
	astraConnConfig AstraConnectionConfig
	baseTlsConfig   *tls.Config
	hostId          string
}

func NewAstraEndpoint(astraConnConfig AstraConnectionConfig, hostId string, baseTlsConfig *tls.Config) *AstraEndpoint {
	return &AstraEndpoint{
		astraConnConfig: astraConnConfig,
		baseTlsConfig:   baseTlsConfig,
		hostId:          hostId,
	}
}

func (recv *AstraEndpoint) GetSocketEndpoint() string {
	return recv.astraConnConfig.GetSniProxyEndpoint()
}

func (recv *AstraEndpoint) GetTlsConfig() *tls.Config {
	return getClientSideTlsConfigFromParsedCerts(
		recv.baseTlsConfig.RootCAs, recv.baseTlsConfig.Certificates, recv.hostId, recv.astraConnConfig.GetSniProxyAddr())
}

func (recv *AstraEndpoint) GetEndpointIdentifier() string {
	return recv.hostId
}

func (recv *AstraEndpoint) String() string {
	return fmt.Sprintf("%s-%s", recv.astraConnConfig.GetSniProxyEndpoint(), recv.hostId)
}