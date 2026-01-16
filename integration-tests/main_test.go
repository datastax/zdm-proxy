package integration_tests

import (
	"os"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/datastax/zdm-proxy/integration-tests/ccm"
	"github.com/datastax/zdm-proxy/integration-tests/env"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/proxy/pkg/zdmproxy"
)

func TestMain(m *testing.M) {
	env.InitGlobalVars()

	if env.Debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	os.Exit(RunTests(m))
}

func SetupOrGetGlobalCcmClusters(t *testing.T) (*ccm.Cluster, *ccm.Cluster, error) {
	originCluster, err := setup.GetGlobalTestClusterOrigin(t)
	if err != nil {
		return nil, nil, err
	}

	targetCluster, err := setup.GetGlobalTestClusterTarget(t)
	if err != nil {
		return nil, nil, err
	}

	return originCluster, targetCluster, err
}

func RunTests(m *testing.M) int {
	defer setup.CleanUpClusters()
	return m.Run()
}

func NewProxyInstanceForGlobalCcmClusters(t *testing.T) (*zdmproxy.ZdmProxy, error) {
	originCluster, targetCluster, err := SetupOrGetGlobalCcmClusters(t)
	if err != nil {
		return nil, err
	}
	return setup.NewProxyInstance(originCluster, targetCluster)
}
