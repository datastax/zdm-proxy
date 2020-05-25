package migration

import (
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
)

// Config contains the environment variables for the migration service
type Config struct {
	SourceHostname string `required:"true" split_words:"true" default:"127.0.0.1"`
	SourceUsername string `required:"true" split_words:"true" default:""`
	SourcePassword string `required:"true" split_words:"true" default:""`
	SourcePort     int    `required:"true" split_words:"true" default:"9042"`
	AstraHostname  string `required:"true" split_words:"true" default:"127.0.0.1"`
	AstraUsername  string `required:"true" split_words:"true" default:""`
	AstraPassword  string `required:"true" split_words:"true" default:""`
	AstraPort      int    `required:"true" split_words:"true" default:"9043"`

	MigrationServiceHostname   string `required:"true" split_words:"true"`
	MigrationCommunicationPort int    `required:"true" split_words:"true"`
	MigrationMetricsPort       int    `default:"8081" split_words:"true"`
	ProxyServiceHostname       string `required:"true" split_words:"true"`
	ProxyCommunicationPort     int    `required:"true" split_words:"true"`

	DsbulkPath  string `required:"true" split_words:"true"`
	HardRestart bool   `default:"false" split_words:"true"`
	Threads     int    `default:"1"`
	Debug       bool   `default:"false"`
	MigrationID string `required:"true" split_words:"true"`
}

// NewConfig returns an empty Config struct
func NewConfig() *Config {
	return &Config{}
}

// ParseEnvVars fills out the fields of the Config struct according to envconfig rules.
// See: Usage @ https://github.com/kelseyhightower/envconfig
func (c *Config) ParseEnvVars() *Config {
	err := envconfig.Process("", c)
	if err != nil {
		log.Panicf("could not load environment variables. Error: %s", err.Error())
	}

	return c
}
