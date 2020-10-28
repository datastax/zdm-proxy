package config

import (
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
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

	Debug bool
}

// New returns an empty Config struct
func New() *Config {
	return &Config{}
}

// ParseEnvVars fills out the fields of the Config struct according to envconfig rules
// See: Usage @ https://github.com/kelseyhightower/envconfig
func (c *Config) ParseEnvVars() *Config {
	err := envconfig.Process("", c)
	if err != nil {
		log.Panicf("could not load environment variables. Error: %s", err.Error())
	}

	return c
}
