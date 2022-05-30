package cloudgateproxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	log "github.com/sirupsen/logrus"
	"net"
	"sync"
)

type ConnectionConfig interface {
	GetClusterType() ClusterType
	GetLocalDatacenter() string
	GetTlsConfig() *tls.Config
	UsesSNI() bool
	GetConnectionTimeoutMs() int
	GetContactPoints() []Endpoint
	RefreshContactPoints(ctx context.Context) ([]Endpoint, error)
	CreateEndpoint(h *Host) Endpoint
}

func InitializeConnectionConfig(clusterTlsConfig *config.ClusterTlsConfig, contactPointsFromConfig []string, port int,
	connTimeoutInMs int, clusterType ClusterType, datacenterFromConfig string, ctx context.Context) (ConnectionConfig, error) {

	var tlsConfig *tls.Config
	var err error
	if clusterTlsConfig.TlsEnabled {
		if clusterTlsConfig.SecureConnectBundlePath != "" {
			return initializeAstraConnectionConfig(connTimeoutInMs, clusterType, clusterTlsConfig.SecureConnectBundlePath, ctx)
		} else {
			tlsConfig, err = getClientSideTlsConfigFromProxyClusterTlsConfig(clusterTlsConfig, clusterType)
			if err != nil {
				return nil, err
			}
		}
	}

	contactPoints := make([]Endpoint, 0)
	for _, contactPoint := range contactPointsFromConfig {
		contactPoints = append(contactPoints, NewDefaultEndpoint(contactPoint, port, tlsConfig))
	}
	return newGenericConnectionConfig(tlsConfig, connTimeoutInMs, clusterType, datacenterFromConfig, contactPoints), nil

}

type baseConnectionConfig struct {
	tlsConfig           *tls.Config
	connectionTimeoutMs int
	clusterType         ClusterType
}

func newBaseConnectionConfig(
	tlsConfig *tls.Config, connectionTimeoutMs int, clusterType ClusterType) *baseConnectionConfig {
	return &baseConnectionConfig{
		tlsConfig:           tlsConfig,
		connectionTimeoutMs: connectionTimeoutMs,
		clusterType:         clusterType,
	}
}

func (cc *baseConnectionConfig) GetConnectionTimeoutMs() int {
	return cc.connectionTimeoutMs
}

func (cc *baseConnectionConfig) GetTlsConfig() *tls.Config {
	return cc.tlsConfig
}

func (cc *baseConnectionConfig) GetClusterType() ClusterType {
	return cc.clusterType
}

type genericConnectionConfig struct {
	*baseConnectionConfig
	datacenter    string
	contactPoints []Endpoint
}

func newGenericConnectionConfig(
	tlsConfig *tls.Config, connectionTimeoutMs int, clusterType ClusterType, datacenter string, contactPoints []Endpoint) *genericConnectionConfig {
	return &genericConnectionConfig{
		baseConnectionConfig: newBaseConnectionConfig(tlsConfig, connectionTimeoutMs, clusterType),
		datacenter:           datacenter,
		contactPoints:        contactPoints,
	}
}

func (cc *genericConnectionConfig) GetLocalDatacenter() string {
	return cc.datacenter
}

func (cc *genericConnectionConfig) UsesSNI() bool {
	// SNI is currently only supported for Astra
	return false
}

func (cc *genericConnectionConfig) GetContactPoints() []Endpoint {
	return cc.contactPoints
}

func (cc *genericConnectionConfig) RefreshContactPoints(ctx context.Context) ([]Endpoint, error) {
	return cc.contactPoints, nil
}

func (cc *genericConnectionConfig) CreateEndpoint(h *Host) Endpoint {
	return NewDefaultEndpoint(h.Address.String(), h.Port, cc.tlsConfig)
}

type AstraConnectionConfig interface {
	ConnectionConfig
	GetSniProxyAddr() string
	GetSniProxyEndpoint() string
}

type astraConnectionConfigImpl struct {
	*baseConnectionConfig
	datacenter          string
	metadataServiceName string
	metadataServicePort string

	contactPoints    []Endpoint
	sniProxyEndpoint string
	sniProxyAddr     string
	contactInfoLock  *sync.RWMutex
}

func initializeAstraConnectionConfig(
	connectionTimeoutMs int, clusterType ClusterType, secureConnectBundlePath string, ctx context.Context) (*astraConnectionConfigImpl, error) {
	fileMap, err := extractFilesFromZipArchive(secureConnectBundlePath)
	if err != nil {
		return nil, err
	}

	metadataServiceHostName, metadataServicePort, err := parseHostAndPortFromSCBConfig(fileMap["config.json"])
	if err != nil {
		return nil, err
	}

	if metadataServiceHostName == "" || metadataServicePort == "" {
		return nil, fmt.Errorf("incomplete metadata service contact information. hostname: %v, port: %v", metadataServiceHostName, metadataServicePort)
	}

	tlsConfig, err := initializeTlsConfigurationFromSecureConnectBundle(fileMap, metadataServiceHostName, clusterType)
	if err != nil {
		return nil, err
	}

	connConfig := &astraConnectionConfigImpl{
		baseConnectionConfig: newBaseConnectionConfig(tlsConfig, connectionTimeoutMs, clusterType),
		datacenter:           "",
		metadataServiceName:  metadataServiceHostName,
		metadataServicePort:  metadataServicePort,
		contactPoints:        nil,
		sniProxyEndpoint:     "",
		sniProxyAddr:         "",
		contactInfoLock:      &sync.RWMutex{},
	}

	metadata, _, err := connConfig.refreshMetadata(ctx)
	if err != nil {
		return nil, err
	}

	connConfig.datacenter = metadata.ContactInfo.LocalDc // set it once only, never refresh
	return connConfig, nil
}

func (cc *astraConnectionConfigImpl) GetLocalDatacenter() string {
	return cc.datacenter
}

func (cc *astraConnectionConfigImpl) UsesSNI() bool {
	return true
}

func (cc *astraConnectionConfigImpl) GetSniProxyAddr() string {
	cc.contactInfoLock.RLock()
	defer cc.contactInfoLock.RUnlock()
	return cc.sniProxyAddr
}

func (cc *astraConnectionConfigImpl) GetSniProxyEndpoint() string {
	cc.contactInfoLock.RLock()
	defer cc.contactInfoLock.RUnlock()
	return cc.sniProxyEndpoint
}

func (cc *astraConnectionConfigImpl) GetContactPoints() []Endpoint {
	cc.contactInfoLock.RLock()
	defer cc.contactInfoLock.RUnlock()
	return cc.contactPoints
}

func (cc *astraConnectionConfigImpl) RefreshContactPoints(ctx context.Context) ([]Endpoint, error) {
	_, contactPoints, err := cc.refreshMetadata(ctx)
	if err != nil {
		return nil, err
	}

	return contactPoints, nil
}

func (cc *astraConnectionConfigImpl) CreateEndpoint(h *Host) Endpoint {
	return cc.createEndpointFromString(h.HostId.String())
}

func (cc *astraConnectionConfigImpl) createEndpointFromString(hostId string) Endpoint {
	return NewAstraEndpoint(cc, hostId, cc.GetTlsConfig())
}

func (cc *astraConnectionConfigImpl) refreshMetadata(ctx context.Context) (*AstraMetadata, []Endpoint, error) {
	metadata, err := retrieveAstraMetadata(cc.metadataServiceName, cc.metadataServicePort, cc.GetTlsConfig(), ctx)
	if err != nil {
		return nil, nil, err
	}
	log.Debugf("Astra metadata parsed to: %v", metadata)

	sniProxyHostname, _, err := net.SplitHostPort(metadata.ContactInfo.SniProxyAddress)
	if err != nil {
		return nil, nil, fmt.Errorf("could not split sni proxy hostname and port: %w", err)
	}

	endpoints := make([]Endpoint, 0)
	for _, hostIdContactPoint := range metadata.ContactInfo.ContactPoints {
		endpoints = append(endpoints, cc.createEndpointFromString(hostIdContactPoint))
	}

	cc.contactInfoLock.Lock()
	defer cc.contactInfoLock.Unlock()
	cc.sniProxyAddr = sniProxyHostname
	cc.sniProxyEndpoint = metadata.ContactInfo.SniProxyAddress
	cc.contactPoints = endpoints

	return metadata, endpoints, nil
}
