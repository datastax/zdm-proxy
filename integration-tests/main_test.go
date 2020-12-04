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

var originCluster *ccm.Cluster
var targetCluster *ccm.Cluster

func TestMain(m *testing.M) {
	env.InitGlobalVars()

	gocql.TimeoutLimit = 5
	if env.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	if env.UseCcm {
		ccm.RemoveCurrent()
		ccm.RemoveCurrent()
		var err error
		originCluster, err = setup.GetGlobalTestClusterOrigin()

		if err != nil {
			log.WithError(err).Fatal()
			os.Exit(-1)
		}

		targetCluster, err = setup.GetGlobalTestClusterTarget()

		if err != nil {
			log.WithError(err).Fatal()
			os.Exit(-1)
		}

		sourceSession := originCluster.GetSession()
		destSession := targetCluster.GetSession()

		// Seed originCluster and targetCluster with keyspace
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
	return setup.NewProxyInstance(originCluster, targetCluster)
}
