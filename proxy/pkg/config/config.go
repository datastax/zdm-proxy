package config

import (
	"encoding/json"
	"fmt"
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
	"net"
	"strconv"
	"strings"
)

// Config holds the values of environment variables necessary for proper Proxy function.
type Config struct {
	TopologyIndex         int    `default:"0" split_words:"true"`
	TopologyInstanceCount int    `default:"-1" split_words:"true"` // Overridden by length of TopologyAddresses (after split) if set
	TopologyAddresses string `split_words:"true"`
	TopologyNumTokens int    `default:"8" split_words:"true"`

	OriginDatacenter string `split_words:"true"`
	TargetDatacenter string `split_words:"true"`

	OriginUsername string `required:"true" split_words:"true"`
	OriginPassword string `required:"true" split_words:"true" json:"-"`

	OriginContactPoints                    string `split_words:"true"`
	OriginPort                    int    `default:"9042" split_words:"true"`
	OriginSecureConnectBundlePath string `split_words:"true"`

	TargetUsername string `required:"true" split_words:"true"`
	TargetPassword string `required:"true" split_words:"true" json:"-"`

	TargetContactPoints                    string `split_words:"true"`
	TargetPort                    int    `default:"9042" split_words:"true"`
	TargetSecureConnectBundlePath string `split_words:"true"`

	OriginTlsServerCaPath   string `split_words:"true"`
	OriginTlsClientCertPath string `split_words:"true"`
	OriginTlsClientKeyPath  string `split_words:"true"`

	TargetTlsServerCaPath   string `split_words:"true"`
	TargetTlsClientCertPath string `split_words:"true"`
	TargetTlsClientKeyPath  string `split_words:"true"`

	ProxyTlsCaPath            string `split_words:"true"`
	ProxyTlsCertPath          string `split_words:"true"`
	ProxyTlsKeyPath           string `split_words:"true"`
	ProxyTlsRequireClientAuth bool   `split_words:"true"`

	NetMetricsAddress string `default:"localhost" split_words:"true"`
	NetMetricsPort    int    `default:"14001" split_words:"true"`
	NetQueryPort    int    `default:"14002" split_words:"true"`
	NetQueryAddress string `default:"localhost" split_words:"true"`

	ClusterConnectionTimeoutMs int `default:"30000" split_words:"true"`
	HeartbeatIntervalMs        int `default:"30000" split_words:"true"`

	HeartbeatRetryIntervalMinMs int     `default:"250" split_words:"true"`
	HeartbeatRetryIntervalMaxMs int     `default:"30000" split_words:"true"`
	HeartbeatRetryBackoffFactor float64 `default:"2" split_words:"true"`
	HeartbeatFailureThreshold   int     `default:"1" split_words:"true"`

	MonitoringEnableMetrics bool `default:"true" split_words:"true"`

	ForwardReadsToTarget         bool `default:"false" split_words:"true"`

	ReplaceServerSideFunctions	bool `default:"false" split_words:"true"`

	DualReadsEnabled        bool `default:"false" split_words:"true"`
	AsyncReadsOnSecondary   bool `default:"false" split_words:"true"`
	AsyncHandshakeTimeoutMs int  `default:"4000" split_words:"true"`

	RequestTimeoutMs int `default:"10000" split_words:"true"`

	ProxyLogLevel string `default:"INFO" split_words:"true"`

	MaxClients int `default:"500" split_words:"true"`

	MonitoringOriginBucketsMs string `default:"1, 4, 7, 10, 25, 40, 60, 80, 100, 150, 250, 500, 1000, 2500, 5000, 10000, 15000" split_words:"true"`
	MonitoringTargetBucketsMs string `default:"1, 4, 7, 10, 25, 40, 60, 80, 100, 150, 250, 500, 1000, 2500, 5000, 10000, 15000" split_words:"true"`
	MonitoringAsyncBucketsMs  string `default:"1, 4, 7, 10, 25, 40, 60, 80, 100, 150, 250, 500, 1000, 2500, 5000, 10000, 15000" split_words:"true"`


	//////////////////////////////////////////////////////////////////////
	/// THE SETTINGS BELOW AREN'T SUPPORTED AND MAY CHANGE AT ANY TIME ///
	//////////////////////////////////////////////////////////////////////

	ForwardClientCredentialsToOrigin bool `default:"false" split_words:"true"` // only takes effect if both clusters have auth enabled

	OriginEnableHostAssignment bool `default:"true" split_words:"true"`
	TargetEnableHostAssignment bool `default:"true" split_words:"true"`

	// PERFORMANCE TUNING CONFIG SETTINGS (shouldn't be changed by users)

	RequestWriteQueueSizeFrames int `default:"128" split_words:"true"`
	RequestWriteBufferSizeBytes int `default:"4096" split_words:"true"`
	RequestReadBufferSizeBytes  int `default:"32768" split_words:"true"`

	ResponseWriteQueueSizeFrames int `default:"128" split_words:"true"`
	ResponseWriteBufferSizeBytes int `default:"8192" split_words:"true"`
	ResponseReadBufferSizeBytes  int `default:"32768" split_words:"true"`

	RequestResponseMaxWorkers int `default:"-1" split_words:"true"`
	WriteMaxWorkers           int `default:"-1" split_words:"true"`
	ReadMaxWorkers            int `default:"-1" split_words:"true"`
	ListenerMaxWorkers        int `default:"-1" split_words:"true"`

	EventQueueSizeFrames int `default:"12" split_words:"true"`

	AsyncConnectorWriteQueueSizeFrames int `default:"2048" split_words:"true"`
	AsyncConnectorWriteBufferSizeBytes int `default:"4096" split_words:"true"`

}

func (c *Config) String() string {
	serializedConfig, _ := json.Marshal(c)
	return string(serializedConfig)
}

// New returns an empty Config struct
func New() *Config {
	return &Config{}
}

// ParseEnvVars fills out the fields of the Config struct according to envconfig rules
// See: Usage @ https://github.com/kelseyhightower/envconfig
func (c *Config) ParseEnvVars() (*Config, error) {
	err := envconfig.Process("ZDM", c)
	if err != nil {
		return nil, fmt.Errorf("could not load environment variables: %w", err)
	}

	err = c.Validate()
	if err != nil {
		return nil, err
	}

	log.Infof("Parsed configuration: %v", c)

	return c, nil
}

func (c *Config) ParseTopologyConfig() (*TopologyConfig, error) {
	virtualizationEnabled := true
	proxyInstanceCount := c.TopologyInstanceCount
	proxyAddressesTyped := []net.IP{net.ParseIP("127.0.0.1")}
	if isNotDefined(c.TopologyAddresses) {
		virtualizationEnabled = false
		if proxyInstanceCount == -1 {
			proxyInstanceCount = 1
		}
	} else {
		proxyAddresses := strings.Split(strings.ReplaceAll(c.TopologyAddresses, " ", ""), ",")
		if len(proxyAddresses) <= 0 {
			return nil, fmt.Errorf("invalid TopologyAddresses: %v", c.TopologyAddresses)
		}

		proxyAddressesTyped = make([]net.IP, 0, len(proxyAddresses))
		for i := 0; i < len(proxyAddresses); i++ {
			proxyAddr := proxyAddresses[i]
			parsedIp := net.ParseIP(proxyAddr)
			if parsedIp == nil {
				return nil, fmt.Errorf("invalid proxy address in TopologyAddresses env var: %v", proxyAddr)
			}
			proxyAddressesTyped = append(proxyAddressesTyped, parsedIp)
		}
		proxyInstanceCount = len(proxyAddressesTyped)
	}

	if proxyInstanceCount <= 0 {
		return nil, fmt.Errorf("invalid TopologyInstanceCount: %v", proxyInstanceCount)
	}

	proxyIndex := c.TopologyIndex
	if proxyIndex < 0 || proxyIndex >= proxyInstanceCount {
		return nil, fmt.Errorf("invalid TopologyIndex and TopologyInstanceCount values; "+
			"proxy index (%d) must be less than instance count (%d) and non negative", proxyIndex, proxyInstanceCount)
	}

	if c.TopologyNumTokens <= 0 || c.TopologyNumTokens > 256 {
		return nil, fmt.Errorf("invalid TopologyNumTokens (%v), it must be positive and equal or less than 256", c.TopologyNumTokens)
	}

	return &TopologyConfig{
		VirtualizationEnabled: virtualizationEnabled,
		Addresses:             proxyAddressesTyped,
		Index:                 proxyIndex,
		Count:                 proxyInstanceCount,
		NumTokens:             c.TopologyNumTokens,
	}, nil
}

func (c *Config) Validate() error {
	_, err := c.ParseLogLevel()
	if err != nil {
		return fmt.Errorf("invalid log level: %w", err)
	}

	_, err = c.ParseTargetContactPoints()
	if err != nil {
		return fmt.Errorf("invalid target configuration: %w", err)
	}

	_, err = c.ParseOriginContactPoints()
	if err != nil {
		return fmt.Errorf("invalid origin configuration: %w", err)
	}

	_, err = c.ParseOriginBuckets()
	if err != nil {
		return fmt.Errorf("could not parse origin buckets: %v", err)
	}

	_, err = c.ParseTargetBuckets()
	if err != nil {
		return fmt.Errorf("could not parse target buckets: %v", err)
	}

	_, err = c.ParseTopologyConfig()
	if err != nil {
		return err
	}

	_, err = c.ParseOriginTlsConfig(false)
	if err != nil {
		return err
	}

	_, err = c.ParseTargetTlsConfig(false)
	if err != nil {
		return err
	}

	_, err = c.ParseProxyTlsConfig(false)
	if err != nil {
		return err
	}

	_, err = c.GetReadMode()
	if err != nil {
		return err
	}

	return nil
}

func (c *Config) ParseLogLevel() (log.Level, error) {
	level, err := log.ParseLevel(strings.TrimSpace(c.ProxyLogLevel))
	if err != nil {
		var lvl log.Level
		return lvl, fmt.Errorf("invalid log level, valid log levels are "+
			"PANIC, FATAL, ERROR, WARN or WARNING, INFO, DEBUG and TRACE; original err: %w", err)
	}

	return level, nil
}

func (c *Config) ParseOriginBuckets() ([]float64, error) {
	return c.parseBuckets(c.MonitoringOriginBucketsMs)
}

func (c *Config) ParseTargetBuckets() ([]float64, error) {
	return c.parseBuckets(c.MonitoringTargetBucketsMs)
}

func (c *Config) ParseAsyncBuckets() ([]float64, error) {
	return c.parseBuckets(c.MonitoringAsyncBucketsMs)
}

func (c *Config) parseBuckets(bucketsConfigStr string) ([]float64, error) {
	var bucketsArr []float64
	bucketsStrArr := strings.Split(bucketsConfigStr, ",")
	for _, bucketStr := range bucketsStrArr {
		bucket, err := strconv.ParseFloat(strings.TrimSpace(bucketStr), 64)
		if err != nil {
			return nil, fmt.Errorf(
				"unable to parse buckets from %v: could not convert %v to float",
				bucketsConfigStr,
				bucketStr)
		}
		bucketsArr = append(bucketsArr, bucket/1000) // convert ms to seconds
	}

	return bucketsArr, nil
}

func (c *Config) ParseOriginContactPoints() ([]string, error) {
	if isDefined(c.OriginSecureConnectBundlePath) && isDefined(c.OriginContactPoints) {
		return nil, fmt.Errorf("OriginSecureConnectBundlePath and OriginContactPoints are mutually exclusive. Please specify only one of them.")
	}

	if isDefined(c.OriginSecureConnectBundlePath) && isDefined(c.OriginDatacenter) {
		return nil, fmt.Errorf("OriginSecureConnectBundlePath and OriginDatacenter are mutually exclusive. Please specify only one of them.")
	}

	if isNotDefined(c.OriginSecureConnectBundlePath) && isNotDefined(c.OriginContactPoints) {
		return nil, fmt.Errorf("Both OriginSecureConnectBundlePath and OriginContactPoints are empty. Please specify either one of them.")
	}

	if isDefined(c.OriginContactPoints) && (c.OriginPort == 0) {
		return nil, fmt.Errorf("OriginContactPoints was specified but the port is missing. Please provide OriginPort")
	}

	if (c.OriginEnableHostAssignment == false) && (isDefined(c.OriginDatacenter)) {
		return nil, fmt.Errorf("OriginDatacenter was specified but OriginEnableHostAssignment is false. Please enable host assignment or don't set the datacenter.")
	}

	if isNotDefined(c.OriginSecureConnectBundlePath) {
		contactPoints := parseContactPoints(c.OriginContactPoints)
		if len(contactPoints) <= 0 {
			return nil, fmt.Errorf("could not parse origin contact points: %v", c.OriginContactPoints)
		}

		return contactPoints, nil
	}

	return nil, nil
}

func (c *Config) ParseTargetContactPoints() ([]string, error) {
	if isDefined(c.TargetSecureConnectBundlePath) && isDefined(c.TargetContactPoints) {
		return nil, fmt.Errorf("TargetSecureConnectBundlePath and TargetContactPoints are mutually exclusive. Please specify only one of them.")
	}

	if isDefined(c.TargetSecureConnectBundlePath) && isDefined(c.TargetDatacenter) {
		return nil, fmt.Errorf("TargetSecureConnectBundlePath and TargetDatacenter are mutually exclusive. Please specify only one of them.")
	}

	if isNotDefined(c.TargetSecureConnectBundlePath) && isNotDefined(c.TargetContactPoints) {
		return nil, fmt.Errorf("Both TargetSecureConnectBundlePath and TargetContactPoints are empty. Please specify either one of them.")
	}

	if (isDefined(c.TargetContactPoints)) && (c.TargetPort == 0) {
		return nil, fmt.Errorf("TargetContactPoints was specified but the port is missing. Please provide TargetPort")
	}

	if (c.TargetEnableHostAssignment == false) && (isDefined(c.TargetDatacenter)) {
		return nil, fmt.Errorf("TargetDatacenter was specified but TargetEnableHostAssignment is false. Please enable host assignment or don't set the datacenter.")
	}

	if isNotDefined(c.TargetSecureConnectBundlePath) {
		contactPoints := parseContactPoints(c.TargetContactPoints)
		if len(contactPoints) <= 0 {
			return nil, fmt.Errorf("could not parse target contact points: %v", c.TargetContactPoints)
		}

		return contactPoints, nil
	}

	return nil, nil
}

func parseContactPoints(setting string) []string {
	return strings.Split(strings.ReplaceAll(setting, " ", ""), ",")
}

func (c *Config) ParseOriginTlsConfig(displayLogMessages bool) (*ClusterTlsConfig, error) {

	// No TLS defined

	if isNotDefined(c.OriginSecureConnectBundlePath) &&
		isNotDefined(c.OriginTlsServerCaPath) &&
		isNotDefined(c.OriginTlsClientCertPath) &&
		isNotDefined(c.OriginTlsClientKeyPath) {
		if displayLogMessages {
			log.Infof("TLS was not configured for Origin")
		}
		return &ClusterTlsConfig{
			TlsEnabled: false,
		}, nil
	}

	//SCB specified

	if isDefined(c.OriginSecureConnectBundlePath) {
		if isDefined(c.OriginTlsServerCaPath) || isDefined(c.OriginTlsClientCertPath) || isDefined(c.OriginTlsClientKeyPath) {
			return &ClusterTlsConfig{}, fmt.Errorf("Incorrect TLS configuration for Origin: Secure Connect Bundle and custom TLS parameters cannot be specified at the same time.")
		}

		if displayLogMessages {
			log.Infof("Mutual TLS configured for Origin using an Astra secure connect bundle")
		}
		return &ClusterTlsConfig{
			TlsEnabled:              true,
			SecureConnectBundlePath: c.OriginSecureConnectBundlePath,
		}, nil
	}

	// Custom TLS params specified

	if isDefined(c.OriginTlsServerCaPath) && (isNotDefined(c.OriginTlsClientCertPath) && isNotDefined(c.OriginTlsClientKeyPath)) {
		if displayLogMessages {
			log.Infof("One-way TLS configured for Origin. Please note that hostname verification is not currently supported.")
		}
		return &ClusterTlsConfig{
			TlsEnabled:   true,
			ServerCaPath: c.OriginTlsServerCaPath,
		}, nil
	}

	if isDefined(c.OriginTlsServerCaPath) && isDefined(c.OriginTlsClientCertPath) && isDefined(c.OriginTlsClientKeyPath) {
		if displayLogMessages {
			log.Infof("Mutual TLS configured for Origin. Please note that hostname verification is not currently supported.")
		}
		return &ClusterTlsConfig{
			TlsEnabled:     true,
			ServerCaPath:   c.OriginTlsServerCaPath,
			ClientCertPath: c.OriginTlsClientCertPath,
			ClientKeyPath:  c.OriginTlsClientKeyPath,
		}, nil
	}

	return &ClusterTlsConfig{}, fmt.Errorf("incomplete TLS configuration for Origin: when using mutual TLS, please specify Server CA path, Client Cert path and Client Key path")

}

func (c *Config) ParseTargetTlsConfig(displayLogMessages bool) (*ClusterTlsConfig, error) {

	// No TLS defined

	if isNotDefined(c.TargetSecureConnectBundlePath) &&
		isNotDefined(c.TargetTlsServerCaPath) &&
		isNotDefined(c.TargetTlsClientCertPath) &&
		isNotDefined(c.TargetTlsClientKeyPath) {
		if displayLogMessages {
			log.Infof("TLS was not configured for Target")
		}
		return &ClusterTlsConfig{
			TlsEnabled: false,
		}, nil
	}

	//SCB specified

	if isDefined(c.TargetSecureConnectBundlePath) {
		if isDefined(c.TargetTlsServerCaPath) || isDefined(c.TargetTlsClientCertPath) || isDefined(c.TargetTlsClientKeyPath) {
			return &ClusterTlsConfig{}, fmt.Errorf("Incorrect TLS configuration for Target: Secure Connect Bundle and custom TLS parameters cannot be specified at the same time.")
		}

		return &ClusterTlsConfig{
			TlsEnabled:              true,
			SecureConnectBundlePath: c.TargetSecureConnectBundlePath,
		}, nil
	}

	// Custom TLS params specified

	if isDefined(c.TargetTlsServerCaPath) && (isNotDefined(c.TargetTlsClientCertPath) && isNotDefined(c.TargetTlsClientKeyPath)) {
		if displayLogMessages {
			log.Infof("One-way TLS configured for Target. Please note that hostname verification is not currently supported.")
		}
		return &ClusterTlsConfig{
			TlsEnabled:   true,
			ServerCaPath: c.TargetTlsServerCaPath,
		}, nil
	}

	if isDefined(c.TargetTlsServerCaPath) && isDefined(c.TargetTlsClientCertPath) && isDefined(c.TargetTlsClientKeyPath) {
		if displayLogMessages {
			log.Infof("Mutual TLS configured for Target. Please note that hostname verification is not currently supported.")
		}
		return &ClusterTlsConfig{
			TlsEnabled:     true,
			ServerCaPath:   c.TargetTlsServerCaPath,
			ClientCertPath: c.TargetTlsClientCertPath,
			ClientKeyPath:  c.TargetTlsClientKeyPath,
		}, nil
	}

	return &ClusterTlsConfig{}, fmt.Errorf("incomplete TLS configuration for Target: when using mutual TLS, please specify Server CA path, Client Cert path and Client Key path")
}

func (c *Config) ParseProxyTlsConfig(displayLogMessages bool) (*ProxyTlsConfig, error) {

	if isNotDefined(c.ProxyTlsCaPath) &&
		isNotDefined(c.ProxyTlsCertPath) &&
		isNotDefined(c.ProxyTlsKeyPath) {
		if displayLogMessages {
			log.Info("Proxy TLS was not configured.")
		}
		return &ProxyTlsConfig{
			TlsEnabled: false,
		}, nil
	}

	if isDefined(c.ProxyTlsCaPath) && isDefined(c.ProxyTlsCertPath) && isDefined(c.ProxyTlsKeyPath) {
		if displayLogMessages {
			log.Info("Proxy TLS configured. Please note that hostname verification is not currently supported.")
		}
		return &ProxyTlsConfig{
			TlsEnabled:    true,
			ProxyCaPath:   c.ProxyTlsCaPath,
			ProxyCertPath: c.ProxyTlsCertPath,
			ProxyKeyPath:  c.ProxyTlsKeyPath,
			ClientAuth:    c.ProxyTlsRequireClientAuth,
		}, nil
	}

	return &ProxyTlsConfig{}, fmt.Errorf("incomplete Proxy TLS configuration: when enabling proxy TLS, please specify CA path, Cert path and Key path")
}

func (c *Config) GetReadMode() (ReadMode, error) {
	if c.DualReadsEnabled {
		if c.AsyncReadsOnSecondary {
			return ReadModeSecondaryAsync, nil
		} else {
			return ReadModeUndefined, fmt.Errorf("combination of DUAL_READS_ENABLED (%v) and ASYNC_READS_ON_SECONDARY (%v) not yet implemented",
				c.DualReadsEnabled, c.AsyncReadsOnSecondary)
		}
	} else {
		if c.AsyncReadsOnSecondary {
			return ReadModeUndefined, fmt.Errorf("invalid combination of DUAL_READS_ENABLED (%v) and ASYNC_READS_ON_SECONDARY (%v)",
				c.DualReadsEnabled, c.AsyncReadsOnSecondary)
		} else {
			return ReadModePrimaryOnly, nil
		}
	}
}

func isDefined(propertyValue string) bool {
	return propertyValue != ""
}

func isNotDefined(propertyValue string) bool {
	return !isDefined(propertyValue)
}

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
	ReadModeUndefined      = ReadMode{""}
	ReadModePrimaryOnly    = ReadMode{"primary_only"}
	ReadModeSecondaryAsync = ReadMode{"secondary_async"}
)
