package config

import (
	"bytes"
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

	OriginCassandraUsername string `required:"true" split_words:"true"`
	OriginCassandraPassword string `required:"true" split_words:"true"`

	OriginCassandraContactPoints           string `split_words:"true"`
	OriginCassandraPort                    int    `default:"9042" split_words:"true"`
	OriginCassandraSecureConnectBundlePath string `split_words:"true"`

	TargetCassandraUsername string `required:"true" split_words:"true"`
	TargetCassandraPassword string `required:"true" split_words:"true"`

	TargetCassandraContactPoints           string `split_words:"true"`
	TargetCassandraPort                    int    `default:"9042" split_words:"true"`
	TargetCassandraSecureConnectBundlePath string `split_words:"true"`

	ProxyMetricsAddress string `default:"localhost" split_words:"true"`
	ProxyMetricsPort    int    `default:"14001" split_words:"true"`
	ProxyQueryPort      int    `default:"14002" split_words:"true"`
	ProxyQueryAddress   string `default:"localhost" split_words:"true"`

	ClusterConnectionTimeoutMs int `default:"30000" split_words:"true"`
	HeartbeatIntervalMs        int `default:"30000" split_words:"true"`

	HeartbeatRetryIntervalMinMs int     `default:"100" split_words:"true"`
	HeartbeatRetryIntervalMaxMs int     `default:"30000" split_words:"true"`
	HeartbeatRetryBackoffFactor float64 `default:"2" split_words:"true"`
	HeartbeatFailureThreshold   int     `default:"1" split_words:"true"`

	OriginBucketsMs string `default:"10, 25, 50, 75, 100, 150, 200, 300, 500, 750, 1000, 2500, 5000" split_words:"true"`
	TargetBucketsMs string `default:"5, 10, 25, 50, 75, 100, 150, 300, 500, 1000, 2000" split_words:"true"`

	EnableMetrics bool `default:"true" split_words:"true"`

	RequestWriteQueueSizeFrames int `default:"128" split_words:"true"`
	RequestWriteBufferSizeBytes int `default:"4096" split_words:"true"`
	RequestReadBufferSizeBytes  int `default:"32768" split_words:"true"`

	ResponseWriteQueueSizeFrames int `default:"128" split_words:"true"`
	ResponseWriteBufferSizeBytes int `default:"8192" split_words:"true"`
	ResponseReadBufferSizeBytes  int `default:"32768" split_words:"true"`

	MaxClientsThreshold int `default:"500" split_words:"true"`

	RequestResponseMaxWorkers int `default:"-1" split_words:"true"`
	WriteMaxWorkers           int `default:"-1" split_words:"true"`
	ReadMaxWorkers            int `default:"-1" split_words:"true"`
	ListenerMaxWorkers        int `default:"-1" split_words:"true"`

	EventQueueSizeFrames int `default:"12" split_words:"true"`

	ForwardReadsToTarget         bool `default:"false" split_words:"true"`
	ForwardSystemQueriesToTarget bool `default:"true" split_words:"true"`

	RequestTimeoutMs int `default:"10000" split_words:"true"`

	Debug bool
}

func (c *Config) String() string {
	var configMap map[string]interface{}
	serializedConfig, _ := json.Marshal(c)
	json.Unmarshal(serializedConfig, &configMap)

	b := new(bytes.Buffer)
	for field, val := range configMap {
		if !strings.Contains(strings.ToLower(field), "username") &&
			!strings.Contains(strings.ToLower(field), "password") {
			fmt.Fprintf(b, "%s=\"%v\"; ", field, val)
		}
	}
	return fmt.Sprintf("Config{%v}", b.String())
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

func (c *Config) ParseVirtualizationConfig() (*TopologyConfig, error) {
	virtualizationEnabled := true
	proxyInstanceCount := c.ProxyInstanceCount
	proxyAddressesTyped := []net.IP{net.ParseIP("127.0.0.1")}
	if c.ProxyAddresses == "" {
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
		return nil, fmt.Errorf("invalid ProxyIndex and ProxyInstanceCount values; " +
			"proxy index (%d) must be less than instance count (%d) and non negative", proxyIndex, proxyInstanceCount)
	}

	if c.ProxyNumTokens <= 0 || c.ProxyNumTokens > 256 {
		return nil, fmt.Errorf("invalid ProxyNumTokens (%v), it must be positive and equal or less than 256", c.ProxyNumTokens)
	}

	return &TopologyConfig{
		VirtualizationEnabled:    virtualizationEnabled,
		Addresses:                proxyAddressesTyped,
		Index:                    proxyIndex,
		Count:                    proxyInstanceCount,
		NumTokens:                c.ProxyNumTokens,
	}, nil
}

func (c *Config) Validate() error {
	_, err := c.ParseTargetContactPoints()
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

	_, err = c.ParseVirtualizationConfig()
	if err != nil {
		return err
	}

	return nil
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
		bucketsArr = append(bucketsArr, bucket / 1000) // convert ms to seconds
	}

	return bucketsArr, nil
}

func (c* Config) ParseOriginContactPoints() ([]string, error) {
	if c.OriginCassandraSecureConnectBundlePath != "" && c.OriginCassandraContactPoints != "" {
		return nil, fmt.Errorf("OriginCassandraSecureConnectBundlePath and OriginCassandraContactPoints are mutually exclusive. Please specify only one of them.")
	}

	if c.OriginCassandraSecureConnectBundlePath == "" && c.OriginCassandraContactPoints == "" {
		return nil, fmt.Errorf("Both OriginCassandraSecureConnectBundlePath and OriginCassandraContactPoints are empty. Please specify either one of them.")
	}

	if (c.OriginCassandraContactPoints != "") && (c.OriginCassandraPort == 0) {
		return nil, fmt.Errorf("OriginCassandraContactPoints was specified but the port is missing. Please provide OriginCassandraPort")
	}

	if c.OriginCassandraSecureConnectBundlePath == "" {
		contactPoints := parseContactPoints(c.OriginCassandraContactPoints)
		if len(contactPoints) <= 0 {
			return nil, fmt.Errorf("could not parse origin contact points: %v", c.OriginCassandraContactPoints)
		}

		return contactPoints, nil
	}

	return nil, nil
}

func (c* Config) ParseTargetContactPoints() ([]string, error) {
	if c.TargetCassandraSecureConnectBundlePath != "" && c.TargetCassandraContactPoints != "" {
		return nil, fmt.Errorf("TargetCassandraSecureConnectBundlePath and TargetCassandraContactPoints are mutually exclusive. Please specify only one of them.")
	}

	if c.TargetCassandraSecureConnectBundlePath == "" && c.TargetCassandraContactPoints == "" {
		return nil, fmt.Errorf("Both TargetCassandraSecureConnectBundlePath and TargetCassandraContactPoints are empty. Please specify either one of them.")
	}

	if (c.TargetCassandraContactPoints != "") && (c.TargetCassandraPort == 0) {
		return nil, fmt.Errorf("TargetCassandraContactPoints was specified but the port is missing. Please provide TargetCassandraPort")
	}

	if c.TargetCassandraSecureConnectBundlePath == "" {
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

// TopologyConfig contains configuration parameters for 2 features related to multi cloudgate-proxy instance deployment:
//   - Virtualization of system.peers
//   - Assignment of C* hosts per proxy instance for request connections
//
type TopologyConfig struct {
	VirtualizationEnabled bool     // enabled if PROXY_ADDRESSES is not empty
	Addresses             []net.IP // comes from PROXY_ADDRESSES
	Count                 int      // comes from PROXY_INSTANCE_COUNT unless PROXY_ADDRESSES is set
	Index                 int      // comes from PROXY_INDEX
	NumTokens             int      // comes from PROXY_NUM_TOKENS
}

func (recv *TopologyConfig) String() string {
	return fmt.Sprintf("TopologyConfig{VirtualizationEnabled=%v, Addresses=%v, Count=%v, Index=%v, NumTokens=%v",
		recv.VirtualizationEnabled, recv.Addresses, recv.Count, recv.Index, recv.NumTokens)
}