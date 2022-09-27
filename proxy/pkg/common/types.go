package common

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"net"
)

// TopologyConfig contains configuration parameters for 2 features related to multi zdm-proxy instance deployment:
//   - Virtualization of system.peers
//   - Assignment of C* hosts per proxy instance for request connections
type TopologyConfig struct {
	VirtualizationEnabled bool     // enabled if PROXY_ADDRESSES is not empty
	Addresses             []net.IP // comes from PROXY_ADDRESSES
	Count                 int      // comes from PROXY_INSTANCE_COUNT unless PROXY_ADDRESSES is set
	Index                 int      // comes from PROXY_INDEX
	NumTokens             int      // comes from PROXY_NUM_TOKENS
}

func (recv *TopologyConfig) String() string {
	return fmt.Sprintf("TopologyConfig{VirtualizationEnabled=%v, Addresses=%v, Count=%v, Index=%v, NumTokens=%v}",
		recv.VirtualizationEnabled, recv.Addresses, recv.Count, recv.Index, recv.NumTokens)
}

// ClusterTlsConfig contains all TLS configuration parameters to connect to a cluster
//   - TLS enabled is an internal flag that is automatically set based on the configuration provided
//   - SCB and all other parameters are mutually exclusive: if SCB is provided, no other parameters must be specified. Doing so will result in a validation errExpected
//   - When using a non-SCB configuration, all other three parameters must be specified (ServerCaPath, ClientCertPath, ClientKeyPath).
type ClusterTlsConfig struct {
	TlsEnabled              bool
	ServerCaPath            string
	ClientCertPath          string
	ClientKeyPath           string
	SecureConnectBundlePath string
}

func (recv *ClusterTlsConfig) String() string {
	return fmt.Sprintf("ClusterTlsConfig{TlsEnabled=%v, ProxyCaPath=%v, ClientCertPath=%v, ClientKeyPath=%v}",
		recv.TlsEnabled, recv.ServerCaPath, recv.ClientCertPath, recv.ClientKeyPath)
}

// ProxyTlsConfig contains all TLS configuration parameters to enable TLS at proxy level
//   - TLS enabled is an internal flag that is automatically set based on the configuration provided
//   - All three properties (ProxyCaPath, ProxyCertPath and ProxyKeyPath) are required for proxy TLS to be enabled
type ProxyTlsConfig struct {
	TlsEnabled    bool
	ProxyCaPath   string
	ProxyCertPath string
	ProxyKeyPath  string
	ClientAuth    bool
}

func (recv *ProxyTlsConfig) String() string {
	return fmt.Sprintf("ProxyTlsConfig{TlsEnabled=%v, ProxyCaPath=%v, ProxyCertPath=%v, ProxyKeyPath=%v, ClientAuth=%v}",
		recv.TlsEnabled, recv.ProxyCaPath, recv.ProxyCertPath, recv.ProxyKeyPath, recv.ClientAuth)

}

type ReadMode struct {
	slug string
}

func (r ReadMode) String() string {
	return r.slug
}

var (
	ReadModeUndefined            = ReadMode{""}
	ReadModePrimaryOnly          = ReadMode{"PRIMARY_ONLY"}
	ReadModeDualAsyncOnSecondary = ReadMode{"DUAL_ASYNC_ON_SECONDARY"}
)

type SystemQueriesMode struct {
	slug string
}

func (r SystemQueriesMode) String() string {
	return r.slug
}

func (r SystemQueriesMode) IsForwardToTarget(primaryCluster ClusterType) bool {
	switch r.slug {
	case SystemQueriesModeOrigin.slug:
		return false
	case SystemQueriesModeTarget.slug:
		return true
	case SystemQueriesModePrimary.slug:
		switch primaryCluster {
		case ClusterTypeOrigin:
			return false
		case ClusterTypeTarget:
			return true
		}
	}
	log.Errorf("Unexpected IsForwardToTarget call on SystemQueriesMode with an invalid configuration. " +
		"This is most likely a bug, please report. Falling back to ORIGIN for SystemQueriesMode. " +
		"SystemQueriesMode = %v, PrimaryCluster = %v", r.slug, primaryCluster)
	return false
}

var (
	SystemQueriesModeUndefined = SystemQueriesMode{""}
	SystemQueriesModeOrigin    = SystemQueriesMode{"ORIGIN"}
	SystemQueriesModeTarget    = SystemQueriesMode{"TARGET"}
	SystemQueriesModePrimary   = SystemQueriesMode{"PRIMARY"}
)

type ClusterType string

const (
	ClusterTypeNone   = ClusterType("")
	ClusterTypeOrigin = ClusterType("ORIGIN")
	ClusterTypeTarget = ClusterType("TARGET")
)