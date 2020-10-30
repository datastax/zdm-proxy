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
	log.SetLevel(log.DebugLevel)

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

func NewProxyInstanceForGlobalCcmClusters() *cloudgateproxy.CloudgateProxy {
	return setup.NewProxyInstance(source, dest)
}
