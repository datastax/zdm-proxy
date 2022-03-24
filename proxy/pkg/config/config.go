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
	ProxyIndex         int    `default:"0" split_words:"true"`
	ProxyInstanceCount int    `default:"-1" split_words:"true"` // Overridden by length of ProxyAddresses (after split) if set
	ProxyAddresses     string `split_words:"true"`
	ProxyNumTokens     int    `default:"8" split_words:"true"`

	OriginEnableHostAssignment bool `default:"true" split_words:"true"`
	TargetEnableHostAssignment bool `default:"true" split_words:"true"`

	OriginDatacenter string `split_words:"true"`
	TargetDatacenter string `split_words:"true"`

	OriginCassandraUsername string `required:"true" split_words:"true"`
	OriginCassandraPassword string `required:"true" split_words:"true" json:"-"`

	OriginCassandraContactPoints           string `split_words:"true"`
	OriginCassandraPort                    int    `default:"9042" split_words:"true"`
	OriginCassandraSecureConnectBundlePath string `split_words:"true"`

	TargetCassandraUsername string `required:"true" split_words:"true"`
	TargetCassandraPassword string `required:"true" split_words:"true" json:"-"`

	TargetCassandraContactPoints           string `split_words:"true"`
	TargetCassandraPort                    int    `default:"9042" split_words:"true"`
	TargetCassandraSecureConnectBundlePath string `split_words:"true"`

	ForwardClientCredentialsToOrigin bool `default:"false" split_words:"true"` // only takes effect if both clusters have auth enabled

	OriginTlsServerCaPath   string `split_words:"true"`
	OriginTlsClientCertPath string `split_words:"true"`
	OriginTlsClientKeyPath  string `split_words:"true"`

	TargetTlsServerCaPath   string `split_words:"true"`
	TargetTlsClientCertPath string `split_words:"true"`
	TargetTlsClientKeyPath  string `split_words:"true"`

	ProxyMetricsAddress string `default:"localhost" split_words:"true"`
	ProxyMetricsPort    int    `default:"14001" split_words:"true"`
	ProxyQueryPort      int    `default:"14002" split_words:"true"`
	ProxyQueryAddress   string `default:"localhost" split_words:"true"`

	ClusterConnectionTimeoutMs int `default:"30000" split_words:"true"`
	HeartbeatIntervalMs        int `default:"30000" split_words:"true"`

	HeartbeatRetryIntervalMinMs int     `default:"250" split_words:"true"`
	HeartbeatRetryIntervalMaxMs int     `default:"30000" split_words:"true"`
	HeartbeatRetryBackoffFactor float64 `default:"2" split_words:"true"`
	HeartbeatFailureThreshold   int     `default:"1" split_words:"true"`

	EnableMetrics bool `default:"true" split_words:"true"`

	ForwardReadsToTarget         bool `default:"false" split_words:"true"`
	ForwardSystemQueriesToTarget bool `default:"false" split_words:"true"`

	DualReadsEnabled        bool `default:"false" split_words:"true"`
	AsyncReadsOnSecondary   bool `default:"false" split_words:"true"`
	AsyncHandshakeTimeoutMs int  `default:"4000" split_words:"true"`

	RequestTimeoutMs int `default:"10000" split_words:"true"`

	LogLevel string `default:"INFO" split_words:"true"`

	MaxClientsThreshold int `default:"500" split_words:"true"`

	OriginBucketsMs string `default:"1, 4, 7, 10, 25, 40, 60, 80, 100, 150, 250, 500, 1000, 2500, 5000, 10000, 15000" split_words:"true"`
	TargetBucketsMs string `default:"1, 4, 7, 10, 25, 40, 60, 80, 100, 150, 250, 500, 1000, 2500, 5000, 10000, 15000" split_words:"true"`

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
	err := envconfig.Process("", c)
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
	proxyInstanceCount := c.ProxyInstanceCount
	proxyAddressesTyped := []net.IP{net.ParseIP("127.0.0.1")}
	if isNotDefined(c.ProxyAddresses) {
		virtualizationEnabled = false
		if proxyInstanceCount == -1 {
			proxyInstanceCount = 1
		}
	} else {
		proxyAddresses := strings.Split(strings.ReplaceAll(c.ProxyAddresses, " ", ""), ",")
		if len(proxyAddresses) <= 0 {
			return nil, fmt.Errorf("invalid ProxyAddresses: %v", c.ProxyAddresses)
		}

		proxyAddressesTyped = make([]net.IP, 0, len(proxyAddresses))
		for i := 0; i < len(proxyAddresses); i++ {
			proxyAddr := proxyAddresses[i]
			parsedIp := net.ParseIP(proxyAddr)
			if parsedIp == nil {
				return nil, fmt.Errorf("invalid proxy address in ProxyAddresses env var: %v", proxyAddr)
			}
			proxyAddressesTyped = append(proxyAddressesTyped, parsedIp)
		}
		proxyInstanceCount = len(proxyAddressesTyped)
	}

	if proxyInstanceCount <= 0 {
		return nil, fmt.Errorf("invalid ProxyInstanceCount: %v", proxyInstanceCount)
	}

	proxyIndex := c.ProxyIndex
	if proxyIndex < 0 || proxyIndex >= proxyInstanceCount {
		return nil, fmt.Errorf("invalid ProxyIndex and ProxyInstanceCount values; "+
			"proxy index (%d) must be less than instance count (%d) and non negative", proxyIndex, proxyInstanceCount)
	}

	if c.ProxyNumTokens <= 0 || c.ProxyNumTokens > 256 {
		return nil, fmt.Errorf("invalid ProxyNumTokens (%v), it must be positive and equal or less than 256", c.ProxyNumTokens)
	}

	return &TopologyConfig{
		VirtualizationEnabled: virtualizationEnabled,
		Addresses:             proxyAddressesTyped,
		Index:                 proxyIndex,
		Count:                 proxyInstanceCount,
		NumTokens:             c.ProxyNumTokens,
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

	_, err = c.ParseReadMode()
	if err != nil {
		return err
	}

	return nil
}

func (c *Config) ParseLogLevel() (log.Level, error) {
	level, err := log.ParseLevel(strings.TrimSpace(c.LogLevel))
	if err != nil {
		var lvl log.Level
		return lvl, fmt.Errorf("invalid log level, valid log levels are "+
			"PANIC, FATAL, ERROR, WARN or WARNING, INFO, DEBUG and TRACE; original err: %w", err)
	}

	return level, nil
}

func (c *Config) ParseOriginBuckets() ([]float64, error) {
	return c.parseBuckets(c.OriginBucketsMs)
}

func (c *Config) ParseTargetBuckets() ([]float64, error) {
	return c.parseBuckets(c.TargetBucketsMs)
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
	if isDefined(c.OriginCassandraSecureConnectBundlePath) && isDefined(c.OriginCassandraContactPoints) {
		return nil, fmt.Errorf("OriginCassandraSecureConnectBundlePath and OriginCassandraContactPoints are mutually exclusive. Please specify only one of them.")
	}

	if isDefined(c.OriginCassandraSecureConnectBundlePath) && isDefined(c.OriginDatacenter) {
		return nil, fmt.Errorf("OriginCassandraSecureConnectBundlePath and OriginDatacenter are mutually exclusive. Please specify only one of them.")
	}

	if isNotDefined(c.OriginCassandraSecureConnectBundlePath) && isNotDefined(c.OriginCassandraContactPoints) {
		return nil, fmt.Errorf("Both OriginCassandraSecureConnectBundlePath and OriginCassandraContactPoints are empty. Please specify either one of them.")
	}

	if isDefined(c.OriginCassandraContactPoints) && (c.OriginCassandraPort == 0) {
		return nil, fmt.Errorf("OriginCassandraContactPoints was specified but the port is missing. Please provide OriginCassandraPort")
	}

	if (c.OriginEnableHostAssignment == false) && (isDefined(c.OriginDatacenter)) {
		return nil, fmt.Errorf("OriginDatacenter was specified but OriginEnableHostAssignment is false. Please enable host assignment or don't set the datacenter.")
	}

	if isNotDefined(c.OriginCassandraSecureConnectBundlePath) {
		contactPoints := parseContactPoints(c.OriginCassandraContactPoints)
		if len(contactPoints) <= 0 {
			return nil, fmt.Errorf("could not parse origin contact points: %v", c.OriginCassandraContactPoints)
		}

		return contactPoints, nil
	}

	return nil, nil
}

func (c *Config) ParseTargetContactPoints() ([]string, error) {
	if isDefined(c.TargetCassandraSecureConnectBundlePath) && isDefined(c.TargetCassandraContactPoints) {
		return nil, fmt.Errorf("TargetCassandraSecureConnectBundlePath and TargetCassandraContactPoints are mutually exclusive. Please specify only one of them.")
	}

	if isDefined(c.TargetCassandraSecureConnectBundlePath) && isDefined(c.TargetDatacenter) {
		return nil, fmt.Errorf("TargetCassandraSecureConnectBundlePath and TargetDatacenter are mutually exclusive. Please specify only one of them.")
	}

	if isNotDefined(c.TargetCassandraSecureConnectBundlePath) && isNotDefined(c.TargetCassandraContactPoints) {
		return nil, fmt.Errorf("Both TargetCassandraSecureConnectBundlePath and TargetCassandraContactPoints are empty. Please specify either one of them.")
	}

	if (isDefined(c.TargetCassandraContactPoints)) && (c.TargetCassandraPort == 0) {
		return nil, fmt.Errorf("TargetCassandraContactPoints was specified but the port is missing. Please provide TargetCassandraPort")
	}

	if (c.TargetEnableHostAssignment == false) && (isDefined(c.TargetDatacenter)) {
		return nil, fmt.Errorf("TargetDatacenter was specified but TargetEnableHostAssignment is false. Please enable host assignment or don't set the datacenter.")
	}

	if isNotDefined(c.TargetCassandraSecureConnectBundlePath) {
		contactPoints := parseContactPoints(c.TargetCassandraContactPoints)
		if len(contactPoints) <= 0 {
			return nil, fmt.Errorf("could not parse target contact points: %v", c.TargetCassandraContactPoints)
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

	if isNotDefined(c.OriginCassandraSecureConnectBundlePath) &&
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

	if isDefined(c.OriginCassandraSecureConnectBundlePath) {
		if isDefined(c.OriginTlsServerCaPath) || isDefined(c.OriginTlsClientCertPath) || isDefined(c.OriginTlsClientKeyPath) {
			return &ClusterTlsConfig{}, fmt.Errorf("Incorrect TLS configuration for Origin: Secure Connect Bundle and custom TLS parameters cannot be specified at the same time.")
		}

		if displayLogMessages {
			log.Infof("Mutual TLS configured for Origin using an Astra secure connect bundle")
		}
		return &ClusterTlsConfig{
			TlsEnabled:              true,
			SecureConnectBundlePath: c.OriginCassandraSecureConnectBundlePath,
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

	return &ClusterTlsConfig{}, fmt.Errorf("Incomplete TLS configuration for Origin: when using mutual TLS, please specify Server CA path, Client Cert path and Client Key path.")

}

func (c *Config) ParseTargetTlsConfig(displayLogMessages bool) (*ClusterTlsConfig, error) {

	// No TLS defined

	if isNotDefined(c.TargetCassandraSecureConnectBundlePath) &&
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

	if isDefined(c.TargetCassandraSecureConnectBundlePath) {
		if isDefined(c.TargetTlsServerCaPath) || isDefined(c.TargetTlsClientCertPath) || isDefined(c.TargetTlsClientKeyPath) {
			return &ClusterTlsConfig{}, fmt.Errorf("Incorrect TLS configuration for Target: Secure Connect Bundle and custom TLS parameters cannot be specified at the same time.")
		}

		return &ClusterTlsConfig{
			TlsEnabled:              true,
			SecureConnectBundlePath: c.TargetCassandraSecureConnectBundlePath,
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

	return &ClusterTlsConfig{}, fmt.Errorf("Incomplete TLS configuration for Target: when using mutual TLS, please specify Server CA path, Client Cert path and Client Key path.")
}

func (c *Config) ParseReadMode() (ReadMode, error) {
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

// TopologyConfig contains configuration parameters for 2 features related to multi cloudgate-proxy instance deployment:
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
	return fmt.Sprintf("ClusterTlsConfig{TlsEnabled=%v, ServerCaPath=%v, ClientCertPath=%v, ClientKeyPath=%v}",
		recv.TlsEnabled, recv.ServerCaPath, recv.ClientCertPath, recv.ClientKeyPath)

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