package integration_tests

import (
	"github.com/gocql/gocql"
	"github.com/datastax/zdm-proxy/integration-tests/ccm"
	"github.com/datastax/zdm-proxy/integration-tests/env"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/proxy/pkg/zdmproxy"
	log "github.com/sirupsen/logrus"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	env.InitGlobalVars()

	gocql.TimeoutLimit = 5
	if env.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	os.Exit(RunTests(m))
}

func SetupOrGetGlobalCcmClusters() (*ccm.Cluster, *ccm.Cluster, error) {
	originCluster, err := setup.GetGlobalTestClusterOrigin()
	if err != nil {
		return nil, nil, err
	}

	targetCluster, err := setup.GetGlobalTestClusterTarget()
	if err != nil {
		return nil, nil, err
	}

	return originCluster, targetCluster, err
}

func RunTests(m *testing.M) int {
	defer setup.CleanUpClusters()
	return m.Run()
}

func NewProxyInstanceForGlobalCcmClusters() (*zdmproxy.ZdmProxy, error) {
	originCluster, targetCluster, err := SetupOrGetGlobalCcmClusters()
	if err != nil {
		return nil, err
	}
	return setup.NewProxyInstance(originCluster, targetCluster)
}
