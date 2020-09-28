package integration_tests

import (
	"github.com/gocql/gocql"
	"github.com/riptano/cloud-gate/integration-tests/ccm"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	log "github.com/sirupsen/logrus"
	"os"
	"testing"
)

var source setup.TestCluster
var dest setup.TestCluster

func TestMain(m *testing.M) {
	gocql.TimeoutLimit = 5
	log.SetLevel(log.DebugLevel)

	if env.UseCcmGlobal {
		ccm.RemoveCurrent()
		ccm.RemoveCurrent()
	}

	var err error
	source, err = setup.GetGlobalTestClusterOrigin()

	if err != nil {
		log.WithError(err).Fatal()
		os.Exit(-1)
	}

	dest, err = setup.GetGlobalTestClusterTarget()

	if err != nil {
		log.WithError(err).Fatal()
		os.Exit(-1)
	}

	os.Exit(RunTests(m))

	//conf := &config.Config{
	//	OriginCassandraHostname: "127.0.0.1",
	//	OriginCassandraUsername: "",
	//	OriginCassandraPassword: "",
	//	OriginCassandraPort:     9042,
	//
	//	TargetCassandraHostname: "127.0.0.1",
	//	TargetCassandraUsername: "",
	//	TargetCassandraPassword: "",
	//	TargetCassandraPort:     9043,
	//
	//	ProxyServiceHostname:       "127.0.0.1",
	//	ProxyCommunicationPort:     14000,
	//	ProxyMetricsPort:           8080,
	//	ProxyQueryPort:             14002,
	//
	//	Test:         false,
	//	Debug:        true,
	//	MaxQueueSize: 1000,
	//}
}

func RunTests(m *testing.M) int {
	defer setup.CleanUpClusters()

	sourceSession := source.GetSession()
	destSession := dest.GetSession()

	// Seed source and dest with keyspace
	setup.SeedKeyspace(sourceSession, destSession)
	return m.Run()
}

func NewProxyInstance() *cloudgateproxy.CloudgateProxy {
	return cloudgateproxy.Run(NewTestConfig(source, dest))
}

func NewTestConfig(origin setup.TestCluster, target setup.TestCluster) *config.Config {
	conf := config.New()
	conf.OriginCassandraHostname = origin.GetInitialContactPoint()
	conf.OriginCassandraUsername = "cassandra"
	conf.OriginCassandraPassword = "cassandra"
	conf.OriginCassandraPort = 9042

	conf.TargetCassandraHostname = target.GetInitialContactPoint()
	conf.TargetCassandraUsername = "cassandra"
	conf.TargetCassandraPassword = "cassandra"
	conf.TargetCassandraPort = 9042

	conf.ProxyServiceHostname = "localhost"
	conf.ProxyCommunicationPort = 14000
	conf.ProxyMetricsPort = 14001
	conf.ProxyQueryPort = 14002
	conf.Debug = false
	conf.Test = false

	return conf
}