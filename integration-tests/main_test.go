package integration_tests

import (
	"github.com/gocql/gocql"
	"github.com/riptano/cloud-gate/integration-tests/ccm"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/integration-tests/simulacron"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"
	log "github.com/sirupsen/logrus"
	"os"
	"testing"
)

var source setup.TestCluster
var dest setup.TestCluster

func TestMain(m *testing.M) {
	gocql.TimeoutLimit = 5
	log.SetLevel(log.InfoLevel)

	env.InitGlobalVars()

	if env.UseCcm {
		ccm.RemoveCurrent()
		ccm.RemoveCurrent()
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

		sourceSession := source.GetSession()
		destSession := dest.GetSession()

		// Seed source and dest with keyspace
		setup.SeedKeyspace(sourceSession, destSession)
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
	defer func() {
		simulacronProcess := simulacron.GetGlobalSimulacronProcess()
		if simulacronProcess != nil {
			simulacronProcess.Cancel()
		}
	}()
	defer setup.CleanUpClusters()

	return m.Run()
}

func NewDefaultProxyInstance() *cloudgateproxy.CloudgateProxy {
	return setup.NewProxyInstance(source, dest)
}
