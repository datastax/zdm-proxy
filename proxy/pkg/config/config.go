package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
	"strings"
)

// TODO remove unnecessary fields and rename where appropriate
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

	log.Infof("Parsed configuration: %v", c)

	return c, nil
}
