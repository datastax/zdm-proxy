package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

// Config holds the values of environment variables necessary for proper Proxy function.
type Config struct {
	ProxyIndex         int `default:"0" split_words:"true"`
	ProxyInstanceCount int `default:"1" split_words:"true"`

	OriginEnableHostAssignment bool `default:"false" split_words:"true"`
	TargetEnableHostAssignment bool `default:"true" split_words:"true"`

	OriginCassandraUsername string `required:"true" split_words:"true"`
	OriginCassandraPassword string `required:"true" split_words:"true"`

	OriginCassandraHostname string `split_words:"true"`
	OriginCassandraPort     int    `default:"9042" split_words:"true"`
	OriginCassandraSecureConnectBundlePath string `split_words:"true"`

	TargetCassandraUsername string `required:"true" split_words:"true"`
	TargetCassandraPassword string `required:"true" split_words:"true"`

	TargetCassandraHostname string `split_words:"true"`
	TargetCassandraPort     int    `default:"9042" split_words:"true"`
	TargetCassandraSecureConnectBundlePath string `split_words:"true"`

	ProxyMetricsAddress string `default:"localhost" split_words:"true"`
	ProxyMetricsPort    int    `required:"true" split_words:"true"`
	ProxyQueryPort      int    `split_words:"true"`
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
	RequestReadBufferSizeBytes int `default:"32768" split_words:"true"`

	ResponseWriteQueueSizeFrames int `default:"128" split_words:"true"`
	ResponseWriteBufferSizeBytes int `default:"8192" split_words:"true"`
	ResponseReadBufferSizeBytes int `default:"32768" split_words:"true"`

	MaxClientsThreshold int `default:"500" split_words:"true"`

	RequestResponseMaxWorkers int `default:"-1" split_words:"true"`
	WriteMaxWorkers           int `default:"-1" split_words:"true"`
	ReadMaxWorkers            int `default:"-1" split_words:"true"`
	ListenerMaxWorkers        int `default:"-1" split_words:"true"`

	EventQueueSizeFrames int `default:"12" split_words:"true"`

	ForwardReadsToTarget bool `default:"false" split_words:"true"`

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

	err = c.validateTargetConfiguration()
	if err != nil {
		return nil, fmt.Errorf("Invalid target configuration: %w", err)
	}

	originBuckets, err := c.ParseOriginBuckets()
	if err != nil {
		return nil, fmt.Errorf("could not parse origin buckets: %v", err)
	}

	targetBuckets, err := c.ParseTargetBuckets()
	if err != nil {
		return nil, fmt.Errorf("could not parse target buckets: %v", err)
	}

	if c.ProxyIndex < 0 || c.ProxyIndex >= c.ProxyInstanceCount {
		return nil, fmt.Errorf("invalid ProxyIndex and ProxyInstanceCount values; " +
			"proxy index must be less than instance count and non negative")
	}

	log.Infof("Parsed configuration: %v", c)
	log.Infof("Parsed buckets: origin{%v}, target{%v}", originBuckets, targetBuckets)

	return c, nil
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

// TODO unify these two validation methods
func (c *Config) validateTargetConfiguration() error {

	if c.TargetCassandraSecureConnectBundlePath != "" && c.TargetCassandraHostname != "" {
		return fmt.Errorf("TargetCassandraSecureConnectBundlePath and TargetCassandraHostname are mutually exclusive. Please specify only one of them.")
	}

	if c.TargetCassandraSecureConnectBundlePath == "" && c.TargetCassandraHostname == "" {
		return fmt.Errorf("Both TargetCassandraSecureConnectBundlePath and TargetCassandraHostname are empty. Please specify either one of them.")
	}

	if (c.TargetCassandraHostname != "") && (c.TargetCassandraPort == 0) {
		return fmt.Errorf("TargetCassandraHostname was specified but the port is missing. Please provide TargetCassandraPort")
	}

	return nil
}

func (c *Config) validateOriginConfiguration() error {

	if c.OriginCassandraSecureConnectBundlePath != "" && c.OriginCassandraHostname != "" {
		return fmt.Errorf("OriginCassandraSecureConnectBundlePath and OriginCassandraHostname are mutually exclusive. Please specify only one of them.")
	}

	if c.OriginCassandraSecureConnectBundlePath == "" && c.OriginCassandraHostname == "" {
		return fmt.Errorf("Both OriginCassandraSecureConnectBundlePath and OriginCassandraHostname are empty. Please specify either one of them.")
	}

	if (c.OriginCassandraHostname != "") && (c.OriginCassandraPort == 0) {
		return fmt.Errorf("OriginCassandraHostname was specified but the port is missing. Please provide OriginCassandraPort")
	}

	return nil
}