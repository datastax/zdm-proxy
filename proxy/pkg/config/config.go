package config

import (
	"encoding/json"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/proxy/pkg/common"
	"github.com/kelseyhightower/envconfig"
	def "github.com/mcuadros/go-defaults"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
	"net"
	"os"
	"strconv"
	"strings"
)

// Config holds the values of environment variables necessary for proper Proxy function.
type Config struct {

	// Global bucket

	PrimaryCluster                string `default:"ORIGIN" split_words:"true" yaml:"primary_cluster"`
	ReadMode                      string `default:"PRIMARY_ONLY" split_words:"true" yaml:"read_mode"`
	ReplaceCqlFunctions           bool   `default:"false" split_words:"true" yaml:"replace_cql_functions"`
	AsyncHandshakeTimeoutMs       int    `default:"4000" split_words:"true" yaml:"async_handshake_timeout_ms"`
	LogLevel                      string `default:"INFO" split_words:"true" yaml:"log_level"`
	ControlConnMaxProtocolVersion string `default:"DseV2" split_words:"true" yaml:"control_conn_max_protocol_version"` // Numeric Cassandra OSS protocol version or DseV1 / DseV2

	// Proxy Topology (also known as system.peers "virtualization") bucket

	ProxyTopologyIndex     int    `default:"0" split_words:"true" yaml:"proxy_topology_index"`
	ProxyTopologyAddresses string `split_words:"true" yaml:"proxy_topology_addresses"`
	ProxyTopologyNumTokens int    `default:"8" split_words:"true" yaml:"proxy_topology_num_tokens"`

	// Origin bucket

	OriginContactPoints           string `split_words:"true" yaml:"origin_contact_points"`
	OriginPort                    int    `default:"9042" split_words:"true" yaml:"origin_port"`
	OriginSecureConnectBundlePath string `split_words:"true" yaml:"origin_secure_connect_bundle_path"`
	OriginLocalDatacenter         string `split_words:"true" yaml:"origin_local_datacenter"`
	OriginUsername                string `required:"true" split_words:"true" yaml:"origin_username"`
	OriginPassword                string `required:"true" split_words:"true" json:"-" yaml:"origin_password"`
	OriginConnectionTimeoutMs     int    `default:"30000" split_words:"true" yaml:"origin_connection_timeout_ms"`

	OriginTlsServerCaPath   string `split_words:"true" yaml:"origin_tls_server_ca_path"`
	OriginTlsClientCertPath string `split_words:"true" yaml:"origin_tls_client_cert_path"`
	OriginTlsClientKeyPath  string `split_words:"true" yaml:"origin_tls_client_key_path"`

	OriginPreferIpFromSystemLocal bool `default:"true" split_words:"true" yaml:"origin_prefer_ip_from_system_local"`

	// Target bucket

	TargetContactPoints           string `split_words:"true" yaml:"target_contact_points"`
	TargetPort                    int    `default:"9042" split_words:"true" yaml:"target_port"`
	TargetSecureConnectBundlePath string `split_words:"true" yaml:"target_secure_connect_bundle_path"`
	TargetLocalDatacenter         string `split_words:"true" yaml:"target_local_datacenter"`
	TargetUsername                string `required:"true" split_words:"true" yaml:"target_username"`
	TargetPassword                string `required:"true" split_words:"true" json:"-" yaml:"target_password"`
	TargetConnectionTimeoutMs     int    `default:"30000" split_words:"true" yaml:"target_connection_timeout_ms"`

	TargetTlsServerCaPath   string `split_words:"true" yaml:"target_tls_server_ca_path"`
	TargetTlsClientCertPath string `split_words:"true" yaml:"target_tls_client_cert_path"`
	TargetTlsClientKeyPath  string `split_words:"true" yaml:"target_tls_client_key_path"`

	TargetPreferIpFromSystemLocal bool `default:"true" split_words:"true" yaml:"target_prefer_ip_from_system_local"`

	// Proxy bucket

	ProxyListenAddress        string `default:"localhost" split_words:"true" yaml:"proxy_listen_address"`
	ProxyListenPort           int    `default:"14002" split_words:"true" yaml:"proxy_listen_port"`
	ProxyRequestTimeoutMs     int    `default:"10000" split_words:"true" yaml:"proxy_request_timeout_ms"`
	ProxyMaxClientConnections int    `default:"1000" split_words:"true" yaml:"proxy_max_client_connections"`
	ProxyMaxStreamIds         int    `default:"2048" split_words:"true" yaml:"proxy_max_stream_ids"`

	ProxyTlsCaPath            string `split_words:"true" yaml:"proxy_tls_ca_path"`
	ProxyTlsCertPath          string `split_words:"true" yaml:"proxy_tls_cert_path"`
	ProxyTlsKeyPath           string `split_words:"true" yaml:"proxy_tls_key_path"`
	ProxyTlsRequireClientAuth bool   `split_words:"true" yaml:"proxy_tls_require_client_auth"`

	// Metrics bucket

	MetricsEnabled bool   `default:"true" split_words:"true" yaml:"metrics_enabled"`
	MetricsAddress string `default:"localhost" split_words:"true" yaml:"metrics_address"`
	MetricsPort    int    `default:"14001" split_words:"true" yaml:"metrics_port"`
	MetricsPrefix  string `default:"zdm" split_words:"true" yaml:"metrics_prefix"`

	MetricsOriginLatencyBucketsMs    string `default:"1, 4, 7, 10, 25, 40, 60, 80, 100, 150, 250, 500, 1000, 2500, 5000, 10000, 15000" split_words:"true" yaml:"metrics_origin_latency_buckets_ms"`
	MetricsTargetLatencyBucketsMs    string `default:"1, 4, 7, 10, 25, 40, 60, 80, 100, 150, 250, 500, 1000, 2500, 5000, 10000, 15000" split_words:"true" yaml:"metrics_target_latency_buckets_ms"`
	MetricsAsyncReadLatencyBucketsMs string `default:"1, 4, 7, 10, 25, 40, 60, 80, 100, 150, 250, 500, 1000, 2500, 5000, 10000, 15000" split_words:"true" yaml:"metrics_async_read_latency_buckets_ms"`

	// Heartbeat bucket

	HeartbeatIntervalMs int `default:"30000" split_words:"true" yaml:"heartbeat_interval_ms"`

	HeartbeatRetryIntervalMinMs int     `default:"250" split_words:"true" yaml:"heartbeat_retry_interval_min_ms"`
	HeartbeatRetryIntervalMaxMs int     `default:"30000" split_words:"true" yaml:"heartbeat_retry_interval_max_ms"`
	HeartbeatRetryBackoffFactor float64 `default:"2" split_words:"true" yaml:"heartbeat_retry_backoff_factor"`
	HeartbeatFailureThreshold   int     `default:"1" split_words:"true" yaml:"heartbeat_failure_threshold"`

	//////////////////////////////////////////////////////////////////////
	/// THE SETTINGS BELOW AREN'T SUPPORTED AND MAY CHANGE AT ANY TIME ///
	//////////////////////////////////////////////////////////////////////

	SystemQueriesMode string `default:"ORIGIN" split_words:"true" yaml:"system_queries_mode"`

	ForwardClientCredentialsToOrigin bool `default:"false" split_words:"true" yaml:"forward_client_credentials_to_origin"` // only takes effect if both clusters have auth enabled

	OriginEnableHostAssignment bool `default:"true" split_words:"true" yaml:"origin_enable_host_assignment"`
	TargetEnableHostAssignment bool `default:"true" split_words:"true" yaml:"target_enable_host_assignment"`

	//////////////////////////////////////////////////////////////////////////////////////////////////////////
	/// THE SETTINGS BELOW ARE FOR PERFORMANCE TUNING; THEY AREN'T SUPPORTED AND MAY CHANGE AT ANY TIME //////
	//////////////////////////////////////////////////////////////////////////////////////////////////////////

	RequestWriteQueueSizeFrames int `default:"128" split_words:"true" yaml:"request_write_queue_size_frames"`
	RequestWriteBufferSizeBytes int `default:"4096" split_words:"true" yaml:"request_write_buffer_size_bytes"`
	RequestReadBufferSizeBytes  int `default:"32768" split_words:"true" yaml:"request_read_buffer_size_bytes"`

	ResponseWriteQueueSizeFrames int `default:"128" split_words:"true" yaml:"response_write_queue_size_frames"`
	ResponseWriteBufferSizeBytes int `default:"8192" split_words:"true" yaml:"response_write_buffer_size_bytes"`
	ResponseReadBufferSizeBytes  int `default:"32768" split_words:"true" yaml:"response_read_buffer_size_bytes"`

	RequestResponseMaxWorkers int `default:"-1" split_words:"true" yaml:"request_response_max_workers"`
	WriteMaxWorkers           int `default:"-1" split_words:"true" yaml:"write_max_workers"`
	ReadMaxWorkers            int `default:"-1" split_words:"true" yaml:"read_max_workers"`
	ListenerMaxWorkers        int `default:"-1" split_words:"true" yaml:"listener_max_workers"`

	EventQueueSizeFrames int `default:"12" split_words:"true" yaml:"event_queue_size_frames"`

	AsyncConnectorWriteQueueSizeFrames int `default:"2048" split_words:"true" yaml:"async_connector_write_queue_size_frames"`
	AsyncConnectorWriteBufferSizeBytes int `default:"4096" split_words:"true" yaml:"async_connector_write_buffer_size_bytes"`
}

func (c *Config) String() string {
	serializedConfig, _ := json.Marshal(c)
	return string(serializedConfig)
}

// New returns an empty Config struct
func New() *Config {
	return &Config{}
}

func (c *Config) loadFromFile(configFile string) error {
	file, err := os.Open(configFile)
	if err != nil {
		return fmt.Errorf("could not read configuration file %v: %w", configFile, err)
	}
	defer file.Close()

	def.SetDefaults(c) // apply default tag, it is not supported by YAML decoder
	dec := yaml.NewDecoder(file)
	if err = dec.Decode(c); err != nil {
		return fmt.Errorf("could not parse yaml file %v: %w", configFile, err)
	}
	return nil
}

// ParseEnvVars fills out the fields of the Config struct according to envconfig rules
// See: Usage @ https://github.com/kelseyhightower/envconfig
func (c *Config) parseEnvVars() error {
	err := envconfig.Process("ZDM", c)
	if err != nil {
		return fmt.Errorf("could not load environment variables: %w", err)
	}

	return nil
}

func (c *Config) LoadConfig(configFile string) (*Config, error) {
	var err error

	if configFile != "" {
		err = c.loadFromFile(configFile)
	} else {
		err = c.parseEnvVars()
	}
	if err != nil {
		return nil, err
	}

	err = c.Validate()
	if err != nil {
		return nil, err
	}

	log.Infof("Parsed configuration: %v", c)

	return c, nil
}

func lookupFirstIp4(host string) (net.IP, error) {
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	for _, ip := range ips {
		ip4 := ip.To4()
		if ip4 != nil {
			return ip4, nil
		}
	}
	return nil, fmt.Errorf("could not resolve %v to an ipv4 address", host)
}

func (c *Config) ParseTopologyConfig() (*common.TopologyConfig, error) {
	var proxyAddressesTyped []net.IP
	defaultLocalIp4Addr := net.IPv4(127, 0, 0, 1)
	if isNotDefined(c.ProxyTopologyAddresses) {
		log.Debugf("[TopologyConfig] Proxy Topology Addresses not defined, attempting to use proxy listen address for system.local: %v.", c.ProxyListenAddress)
		if isDefined(c.ProxyListenAddress) {
			parsedListenAddress, err := lookupFirstIp4(c.ProxyListenAddress)
			if err != nil {
				log.Debugf("[TopologyConfig] Could not resolve Proxy Listen Address to an IPv4 address: %v. Falling back to default: %v.", err, defaultLocalIp4Addr.String())
			} else {
				proxyAddressesTyped = []net.IP{parsedListenAddress}
			}
		} else {
			log.Debugf("[TopologyConfig] Proxy Listen Address not defined, falling back to default: %v.", defaultLocalIp4Addr.String())
		}
		if len(proxyAddressesTyped) == 0 {
			proxyAddressesTyped = []net.IP{defaultLocalIp4Addr}
		}
	} else {
		proxyAddresses := strings.Split(strings.ReplaceAll(c.ProxyTopologyAddresses, " ", ""), ",")
		if len(proxyAddresses) <= 0 {
			return nil, fmt.Errorf("invalid ZDM_PROXY_TOPOLOGY_ADDRESSES: %v", c.ProxyTopologyAddresses)
		}

		proxyAddressesTyped = make([]net.IP, 0, len(proxyAddresses))
		for i := 0; i < len(proxyAddresses); i++ {
			proxyAddr := proxyAddresses[i]
			parsedIp := net.ParseIP(proxyAddr)
			if parsedIp == nil {
				return nil, fmt.Errorf("invalid proxy address in ZDM_PROXY_TOPOLOGY_ADDRESSES env var: %v", proxyAddr)
			}
			proxyAddressesTyped = append(proxyAddressesTyped, parsedIp)
		}

	}

	proxyInstanceCount := len(proxyAddressesTyped)
	proxyIndex := c.ProxyTopologyIndex
	if proxyIndex < 0 || proxyIndex >= proxyInstanceCount {
		return nil, fmt.Errorf("invalid ZDM_PROXY_TOPOLOGY_INDEX and ZDM_PROXY_TOPOLOGY_ADDRESSES values; "+
			"proxy index (%d) must be less than length of addresses (%d) and non negative", proxyIndex, proxyInstanceCount)
	}

	if c.ProxyTopologyNumTokens <= 0 || c.ProxyTopologyNumTokens > 256 {
		return nil, fmt.Errorf("invalid ZDM_PROXY_TOPOLOGY_NUM_TOKENS (%v), it must be positive and equal or less than 256", c.ProxyTopologyNumTokens)
	}

	return &common.TopologyConfig{
		VirtualizationEnabled: true, // keep flag for now until we are absolutely certain we will never need it again
		Addresses:             proxyAddressesTyped,
		Index:                 proxyIndex,
		Count:                 proxyInstanceCount,
		NumTokens:             c.ProxyTopologyNumTokens,
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

	_, err = c.ParsePrimaryCluster()
	if err != nil {
		return err
	}

	_, err = c.ParseSystemQueriesMode()
	if err != nil {
		return err
	}

	_, err = c.ParseReadMode()
	if err != nil {
		return err
	}

	_, err = c.ParseControlConnMaxProtocolVersion()
	if err != nil {
		return err
	}

	return nil
}

const (
	SystemQueriesModeOrigin = "ORIGIN"
	SystemQueriesModeTarget = "TARGET"
)

func (c *Config) ParseSystemQueriesMode() (common.SystemQueriesMode, error) {
	switch strings.ToUpper(c.SystemQueriesMode) {
	case SystemQueriesModeTarget:
		return common.SystemQueriesModeTarget, nil
	case SystemQueriesModeOrigin:
		return common.SystemQueriesModeOrigin, nil
	default:
		return common.SystemQueriesModeUndefined, fmt.Errorf("invalid value for ZDM_SYSTEM_QUERIES_MODE; possible values are: %v and %v",
			SystemQueriesModeTarget, SystemQueriesModeOrigin)
	}
}

const (
	PrimaryClusterOrigin = "ORIGIN"
	PrimaryClusterTarget = "TARGET"
)

func (c *Config) ParsePrimaryCluster() (common.ClusterType, error) {
	switch strings.ToUpper(c.PrimaryCluster) {
	case PrimaryClusterOrigin:
		return common.ClusterTypeOrigin, nil
	case PrimaryClusterTarget:
		return common.ClusterTypeTarget, nil
	default:
		return common.ClusterTypeNone, fmt.Errorf("invalid value for ZDM_PRIMARY_CLUSTER; possible values are: %v and %v",
			PrimaryClusterOrigin, PrimaryClusterTarget)
	}
}

const (
	ReadModePrimaryOnly          = "PRIMARY_ONLY"
	ReadModeDualAsyncOnSecondary = "DUAL_ASYNC_ON_SECONDARY"
)

func (c *Config) ParseReadMode() (common.ReadMode, error) {
	switch strings.ToUpper(c.ReadMode) {
	case ReadModePrimaryOnly:
		return common.ReadModePrimaryOnly, nil
	case ReadModeDualAsyncOnSecondary:
		return common.ReadModeDualAsyncOnSecondary, nil
	default:
		return common.ReadModeUndefined, fmt.Errorf("invalid value for ZDM_READ_MODE; possible values are: %v and %v",
			ReadModePrimaryOnly, ReadModeDualAsyncOnSecondary)
	}
}

func (c *Config) ParseControlConnMaxProtocolVersion() (primitive.ProtocolVersion, error) {
	if strings.EqualFold(c.ControlConnMaxProtocolVersion, "DseV2") {
		return primitive.ProtocolVersionDse2, nil
	}
	if strings.EqualFold(c.ControlConnMaxProtocolVersion, "DseV1") {
		return primitive.ProtocolVersionDse1, nil
	}
	ver, err := strconv.ParseUint(c.ControlConnMaxProtocolVersion, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("could not parse control connection max protocol version, valid values are "+
			"2, 3, 4, DseV1, DseV2; original err: %w", err)
	}
	if ver < 2 || ver > 4 {
		return 0, fmt.Errorf("invalid control connection max protocol version, valid values are 2, 3, 4, DseV1, DseV2")
	}
	return primitive.ProtocolVersion(ver), nil
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
	return c.parseBuckets(c.MetricsOriginLatencyBucketsMs)
}

func (c *Config) ParseTargetBuckets() ([]float64, error) {
	return c.parseBuckets(c.MetricsTargetLatencyBucketsMs)
}

func (c *Config) ParseAsyncBuckets() ([]float64, error) {
	return c.parseBuckets(c.MetricsAsyncReadLatencyBucketsMs)
}

func (c *Config) parseBuckets(bucketsConfigStr string) ([]float64, error) {
	var bucketsArr []float64
	bucketsStrArr := strings.Split(bucketsConfigStr, ",")
	if len(bucketsStrArr) == 0 {
		return nil, fmt.Errorf("unable to parse buckets from %v: at least one bucket is required", bucketsConfigStr)
	}

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

	if isDefined(c.OriginSecureConnectBundlePath) && isDefined(c.OriginLocalDatacenter) {
		return nil, fmt.Errorf("OriginSecureConnectBundlePath and OriginLocalDatacenter are mutually exclusive. Please specify only one of them.")
	}

	if isNotDefined(c.OriginSecureConnectBundlePath) && isNotDefined(c.OriginContactPoints) {
		return nil, fmt.Errorf("Both OriginSecureConnectBundlePath and OriginContactPoints are empty. Please specify either one of them.")
	}

	if isDefined(c.OriginContactPoints) && (c.OriginPort == 0) {
		return nil, fmt.Errorf("OriginContactPoints was specified but the port is missing. Please provide OriginPort")
	}

	if (c.OriginEnableHostAssignment == false) && (isDefined(c.OriginLocalDatacenter)) {
		return nil, fmt.Errorf("OriginLocalDatacenter was specified but OriginEnableHostAssignment is false. Please enable host assignment or don't set the datacenter.")
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

	if isDefined(c.TargetSecureConnectBundlePath) && isDefined(c.TargetLocalDatacenter) {
		return nil, fmt.Errorf("TargetSecureConnectBundlePath and TargetLocalDatacenter are mutually exclusive. Please specify only one of them.")
	}

	if isNotDefined(c.TargetSecureConnectBundlePath) && isNotDefined(c.TargetContactPoints) {
		return nil, fmt.Errorf("Both TargetSecureConnectBundlePath and TargetContactPoints are empty. Please specify either one of them.")
	}

	if (isDefined(c.TargetContactPoints)) && (c.TargetPort == 0) {
		return nil, fmt.Errorf("TargetContactPoints was specified but the port is missing. Please provide TargetPort")
	}

	if (c.TargetEnableHostAssignment == false) && (isDefined(c.TargetLocalDatacenter)) {
		return nil, fmt.Errorf("TargetLocalDatacenter was specified but TargetEnableHostAssignment is false. Please enable host assignment or don't set the datacenter.")
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

func (c *Config) ParseOriginTlsConfig(displayLogMessages bool) (*common.ClusterTlsConfig, error) {

	// No TLS defined

	if isNotDefined(c.OriginSecureConnectBundlePath) &&
		isNotDefined(c.OriginTlsServerCaPath) &&
		isNotDefined(c.OriginTlsClientCertPath) &&
		isNotDefined(c.OriginTlsClientKeyPath) {
		if displayLogMessages {
			log.Infof("TLS was not configured for Origin")
		}
		return &common.ClusterTlsConfig{
			TlsEnabled: false,
		}, nil
	}

	//SCB specified

	if isDefined(c.OriginSecureConnectBundlePath) {
		if isDefined(c.OriginTlsServerCaPath) || isDefined(c.OriginTlsClientCertPath) || isDefined(c.OriginTlsClientKeyPath) {
			return &common.ClusterTlsConfig{}, fmt.Errorf("Incorrect TLS configuration for Origin: Secure Connect Bundle and custom TLS parameters cannot be specified at the same time.")
		}

		if displayLogMessages {
			log.Infof("Mutual TLS configured for Origin using an Astra secure connect bundle")
		}
		return &common.ClusterTlsConfig{
			TlsEnabled:              true,
			SecureConnectBundlePath: c.OriginSecureConnectBundlePath,
		}, nil
	}

	// Custom TLS params specified

	if isDefined(c.OriginTlsServerCaPath) && (isNotDefined(c.OriginTlsClientCertPath) && isNotDefined(c.OriginTlsClientKeyPath)) {
		if displayLogMessages {
			log.Infof("One-way TLS configured for Origin. Please note that hostname verification is not currently supported.")
		}
		return &common.ClusterTlsConfig{
			TlsEnabled:   true,
			ServerCaPath: c.OriginTlsServerCaPath,
		}, nil
	}

	if isDefined(c.OriginTlsServerCaPath) && isDefined(c.OriginTlsClientCertPath) && isDefined(c.OriginTlsClientKeyPath) {
		if displayLogMessages {
			log.Infof("Mutual TLS configured for Origin. Please note that hostname verification is not currently supported.")
		}
		return &common.ClusterTlsConfig{
			TlsEnabled:     true,
			ServerCaPath:   c.OriginTlsServerCaPath,
			ClientCertPath: c.OriginTlsClientCertPath,
			ClientKeyPath:  c.OriginTlsClientKeyPath,
		}, nil
	}

	return &common.ClusterTlsConfig{}, fmt.Errorf("incomplete TLS configuration for Origin: when using mutual TLS, " +
		"please specify Server CA path, Client Cert path and Client Key path")

}

func (c *Config) ParseTargetTlsConfig(displayLogMessages bool) (*common.ClusterTlsConfig, error) {

	// No TLS defined

	if isNotDefined(c.TargetSecureConnectBundlePath) &&
		isNotDefined(c.TargetTlsServerCaPath) &&
		isNotDefined(c.TargetTlsClientCertPath) &&
		isNotDefined(c.TargetTlsClientKeyPath) {
		if displayLogMessages {
			log.Infof("TLS was not configured for Target")
		}
		return &common.ClusterTlsConfig{
			TlsEnabled: false,
		}, nil
	}

	//SCB specified

	if isDefined(c.TargetSecureConnectBundlePath) {
		if isDefined(c.TargetTlsServerCaPath) || isDefined(c.TargetTlsClientCertPath) || isDefined(c.TargetTlsClientKeyPath) {
			return &common.ClusterTlsConfig{}, fmt.Errorf("Incorrect TLS configuration for Target: Secure Connect Bundle and custom TLS parameters cannot be specified at the same time.")
		}

		return &common.ClusterTlsConfig{
			TlsEnabled:              true,
			SecureConnectBundlePath: c.TargetSecureConnectBundlePath,
		}, nil
	}

	// Custom TLS params specified

	if isDefined(c.TargetTlsServerCaPath) && (isNotDefined(c.TargetTlsClientCertPath) && isNotDefined(c.TargetTlsClientKeyPath)) {
		if displayLogMessages {
			log.Infof("One-way TLS configured for Target. Please note that hostname verification is not currently supported.")
		}
		return &common.ClusterTlsConfig{
			TlsEnabled:   true,
			ServerCaPath: c.TargetTlsServerCaPath,
		}, nil
	}

	if isDefined(c.TargetTlsServerCaPath) && isDefined(c.TargetTlsClientCertPath) && isDefined(c.TargetTlsClientKeyPath) {
		if displayLogMessages {
			log.Infof("Mutual TLS configured for Target. Please note that hostname verification is not currently supported.")
		}
		return &common.ClusterTlsConfig{
			TlsEnabled:     true,
			ServerCaPath:   c.TargetTlsServerCaPath,
			ClientCertPath: c.TargetTlsClientCertPath,
			ClientKeyPath:  c.TargetTlsClientKeyPath,
		}, nil
	}

	return &common.ClusterTlsConfig{}, fmt.Errorf("incomplete TLS configuration for Target: when using mutual TLS, please specify Server CA path, Client Cert path and Client Key path")
}

func (c *Config) ParseProxyTlsConfig(displayLogMessages bool) (*common.ProxyTlsConfig, error) {

	if isNotDefined(c.ProxyTlsCaPath) &&
		isNotDefined(c.ProxyTlsCertPath) &&
		isNotDefined(c.ProxyTlsKeyPath) {
		if displayLogMessages {
			log.Info("Proxy TLS was not configured.")
		}
		return &common.ProxyTlsConfig{
			TlsEnabled: false,
		}, nil
	}

	if isDefined(c.ProxyTlsCaPath) && isDefined(c.ProxyTlsCertPath) && isDefined(c.ProxyTlsKeyPath) {
		if displayLogMessages {
			log.Info("Proxy TLS configured. Please note that hostname verification is not currently supported.")
		}
		return &common.ProxyTlsConfig{
			TlsEnabled:    true,
			ProxyCaPath:   c.ProxyTlsCaPath,
			ProxyCertPath: c.ProxyTlsCertPath,
			ProxyKeyPath:  c.ProxyTlsKeyPath,
			ClientAuth:    c.ProxyTlsRequireClientAuth,
		}, nil
	}

	return &common.ProxyTlsConfig{}, fmt.Errorf("incomplete Proxy TLS configuration: when enabling proxy TLS, please specify CA path, Cert path and Key path")
}

func isDefined(propertyValue string) bool {
	return propertyValue != ""
}

func isNotDefined(propertyValue string) bool {
	return !isDefined(propertyValue)
}
