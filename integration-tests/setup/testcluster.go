package setup

import (
	"github.com/gocql/gocql"
	"github.com/riptano/cloud-gate/integration-tests/ccm"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"github.com/riptano/cloud-gate/integration-tests/simulacron"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	log "github.com/sirupsen/logrus"

	"math"
	"sync"
)

type TestCluster interface {
	GetInitialContactPoint() string
	GetVersion() string
	GetId() string
	GetSession() *gocql.Session
	Remove() error
}

var mux = &sync.Mutex{}

var createdGlobalClusters = false

var globalCcmClusterOrigin *ccm.Cluster
var globalCcmClusterTarget *ccm.Cluster

func GetGlobalTestClusterOrigin() (*ccm.Cluster, error) {
	if createdGlobalClusters {
		return globalCcmClusterOrigin, nil
	}

	mux.Lock()
	defer mux.Unlock()
	if createdGlobalClusters {
		return globalCcmClusterOrigin, nil
	}

	err := createClusters()

	if err != nil {
		return nil, err
	}

	return globalCcmClusterOrigin, nil
}

func GetGlobalTestClusterTarget() (*ccm.Cluster, error) {
	if createdGlobalClusters {
		return globalCcmClusterTarget, nil
	}

	mux.Lock()
	defer mux.Unlock()
	if createdGlobalClusters {
		return globalCcmClusterTarget, nil
	}

	err := createClusters()

	if err != nil {
		return nil, err
	}

	return globalCcmClusterTarget, nil
}

func createClusters() error {
	// assuming we have the lock already

	var err error

	firstClusterId := env.Rand.Uint64() % (math.MaxUint64 - 1)
	globalCcmClusterOrigin, err = ccm.GetNewCluster(firstClusterId, 1, env.OriginNodes, true)
	if err != nil {
		return err
	}

	secondClusterId := firstClusterId + 1
	globalCcmClusterTarget, err = ccm.GetNewCluster(secondClusterId, 10, env.TargetNodes, true)
	if err != nil {
		return err
	}

	createdGlobalClusters = true
	return nil
}

func CleanUpClusters() {
	if !createdGlobalClusters {
		return
	}

	globalCcmClusterTarget.SwitchToThis()
	ccm.RemoveCurrent()
	globalCcmClusterOrigin.SwitchToThis()
	ccm.RemoveCurrent()
}

type SimulacronTestSetup struct {
	Origin *simulacron.Cluster
	Target *simulacron.Cluster
	Proxy  *cloudgateproxy.CloudgateProxy
}

type CcmTestSetup struct {
	Origin *ccm.Cluster
	Target *ccm.Cluster
	Proxy  *cloudgateproxy.CloudgateProxy
}

func NewSimulacronTestSetup() *SimulacronTestSetup {
	origin, _ := simulacron.GetNewCluster(1)
	target, _ := simulacron.GetNewCluster(1)
	proxyInstance := NewProxyInstance(origin, target)
	return &SimulacronTestSetup{
		Origin: origin,
		Target: target,
		Proxy:  proxyInstance,
	}
}

func (setup *SimulacronTestSetup) Cleanup() {
	setup.Proxy.Shutdown()

	err := setup.Target.Remove()
	if err != nil {
		log.Errorf("remove target simulacron cluster error: %s", err)
	}

	err = setup.Origin.Remove()
	if err != nil {
		log.Errorf("remove origin simulacron cluster error: %s", err)
	}
}

func NewTemporaryCcmTestSetup(start bool) (*CcmTestSetup, error) {
	firstClusterId := env.Rand.Uint64() % (math.MaxUint64 - 1)
	origin, err := ccm.GetNewCluster(firstClusterId, 20, env.OriginNodes, start)
	if err != nil {
		return nil, err
	}

	secondClusterId := firstClusterId + 1
	target, err := ccm.GetNewCluster(secondClusterId, 30, env.TargetNodes, start)
	if err != nil {
		origin.Remove()
		return nil, err
	}

	var proxyInstance *cloudgateproxy.CloudgateProxy
	if start {
		proxyInstance = NewProxyInstance(origin, target)
	} else {
		proxyInstance = nil
	}

	return &CcmTestSetup{
		Origin: origin,
		Target: target,
		Proxy:  proxyInstance,
	}, nil
}

// To prevent proxy from being started, pass nil config
func (setup *CcmTestSetup) Start(config *config.Config, jvmArgs ...string) error {
	err := setup.Origin.Start(jvmArgs...)
	if err != nil {
		return err
	}
	err = setup.Target.Start(jvmArgs...)
	if err != nil {
		return err
	}
	if config != nil {
		setup.Proxy = NewProxyInstanceWithConfig(config)
	}
	return nil
}

func (setup *CcmTestSetup) Cleanup() {
	if setup.Proxy != nil {
		setup.Proxy.Shutdown()
	}

	err := setup.Target.Remove()
	if err != nil {
		log.Errorf("remove target ccm cluster error: %s", err)
	}

	err = setup.Origin.Remove()
	if err != nil {
		log.Errorf("remove origin ccm cluster error: %s", err)
	}
}

func NewProxyInstance(origin TestCluster, target TestCluster) *cloudgateproxy.CloudgateProxy {
	return NewProxyInstanceWithConfig(NewTestConfig(origin, target))
}

func NewProxyInstanceWithConfig(config *config.Config) *cloudgateproxy.CloudgateProxy {
	return cloudgateproxy.Run(config)
}

func NewTestConfig(origin TestCluster, target TestCluster) *config.Config {
	conf := config.New()
	conf.OriginCassandraHostname = origin.GetInitialContactPoint()
	conf.OriginCassandraPort = 9042

	conf.TargetCassandraHostname = target.GetInitialContactPoint()
	conf.TargetCassandraUsername = "cassandra"
	conf.TargetCassandraPassword = "cassandra"
	conf.TargetCassandraPort = 9042

	conf.ProxyMetricsAddress = "localhost"
	conf.ProxyMetricsPort = 14001
	conf.ProxyQueryPort = 14002
	conf.ProxyQueryAddress = "localhost"
	conf.Debug = false

	return conf
}
