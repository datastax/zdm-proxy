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
	OriginCassandraHostname string `required:"true" split_words:"true"`
	OriginCassandraUsername string `required:"true" split_words:"true"`
	OriginCassandraPassword string `required:"true" split_words:"true"`
	OriginCassandraPort     int    `required:"true" split_words:"true"`

	TargetCassandraHostname string `required:"true" split_words:"true"`
	TargetCassandraUsername string `required:"true" split_words:"true"`
	TargetCassandraPassword string `required:"true" split_words:"true"`
	TargetCassandraPort     int    `required:"true" split_words:"true"`

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

	WriteQueueSizeFrames int `default:"2048" split_words:"true"`
	WriteBufferSizeBytes int `default:"16384" split_words:"true"`
	ReadBufferSizeBytes int `default:"16384" split_words:"true"`

	RequestQueueSizeFrames int `default:"128" split_words:"true"`
	ResponseQueueSizeFrames int `default:"128" split_words:"true"`
	EventQueueSizeFrames int `default:"64" split_words:"true"`

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

	originBuckets, err := c.ParseOriginBuckets()
	if err != nil {
		return nil, fmt.Errorf("could not parse origin buckets: %v", err)
	}

	targetBuckets, err := c.ParseTargetBuckets()
	if err != nil {
		return nil, fmt.Errorf("could not parse target buckets: %v", err)
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