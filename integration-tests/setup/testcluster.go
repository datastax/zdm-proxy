package setup

import (
	"context"
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

func NewSimulacronTestSetupWithSession(createProxy bool, createSession bool) *SimulacronTestSetup {
	origin, err := simulacron.GetNewCluster(createSession, 1)
	if err != nil {
		log.Panic("simulacron origin startup failed: ", err)
	}
	target, err := simulacron.GetNewCluster(createSession, 1)
	if err != nil {
		log.Panic("simulacron target startup failed: ", err)
	}
	var proxyInstance *cloudgateproxy.CloudgateProxy
	if createProxy {
		proxyInstance = NewProxyInstance(origin, target)
	} else {
		proxyInstance = nil
	}
	return &SimulacronTestSetup{
		Origin: origin,
		Target: target,
		Proxy:  proxyInstance,
	}
}

func NewSimulacronTestSetup() *SimulacronTestSetup {
	return NewSimulacronTestSetupWithSession(true, false)
}

func (setup *SimulacronTestSetup) Cleanup() {
	if setup.Proxy != nil {
		setup.Proxy.Shutdown()
	}

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
	return NewProxyInstanceWithConfig(NewTestConfig(origin.GetInitialContactPoint(), target.GetInitialContactPoint()))
}

func NewProxyInstanceWithConfig(config *config.Config) *cloudgateproxy.CloudgateProxy {
	proxy, err := cloudgateproxy.Run(config, context.Background())
	if err != nil {
		panic(err)
	}
	return proxy
}

func NewTestConfig(originHost string, targetHost string) *config.Config {
	conf := config.New()
	conf.OriginCassandraHostname = originHost
	conf.OriginCassandraUsername = "cassandra"
	conf.OriginCassandraPassword = "cassandra"
	conf.OriginCassandraPort = 9042

	conf.TargetCassandraHostname = targetHost
	conf.TargetCassandraUsername = "cassandra"
	conf.TargetCassandraPassword = "cassandra"
	conf.TargetCassandraPort = 9042

	conf.ProxyMetricsAddress = "localhost"
	conf.ProxyMetricsPort = 14001
	conf.ProxyQueryPort = 14002
	conf.ProxyQueryAddress = "localhost"

	conf.ClusterConnectionTimeoutMs = 30000
	conf.HeartbeatIntervalMs = 30000

	conf.HeartbeatRetryIntervalMaxMs = 30000
	conf.HeartbeatRetryIntervalMinMs = 100
	conf.HeartbeatRetryBackoffFactor = 2
	conf.HeartbeatFailureThreshold = 1

	conf.OriginBucketsMs = "10, 25, 50, 75, 100, 150, 200, 300, 500, 750, 1000, 2500, 5000"
	conf.TargetBucketsMs = "5, 10, 25, 50, 75, 100, 150, 300, 500, 1000, 2000"

	conf.EnableMetrics = true

	conf.WriteQueueSizeFrames = 8192
	conf.WriteBufferSizeBytes = 65536
	conf.ReadBufferSizeBytes = 65536

	conf.MaxWorkers = -1
	conf.EventQueueSizeFrames = 64

	conf.ForwardReadsToTarget = false

	conf.Debug = false

	return conf
}
