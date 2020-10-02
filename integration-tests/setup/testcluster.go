package setup

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/riptano/cloud-gate/integration-tests/ccm"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"github.com/riptano/cloud-gate/integration-tests/simulacron"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"math"
	"math/rand"
	"sync"
	"time"
)

type TestCluster interface {
	GetInitialContactPoint() string
	GetVersion() string
	GetId() string
	GetSession() *gocql.Session
	Remove() error
}

const (
	originNodes = 1
	targetNodes = 1
)

var mux = &sync.Mutex{}
var r = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

var createdGlobalClusters = false

var globalCcmClusterOrigin *ccm.Cluster
var globalCcmClusterTarget *ccm.Cluster

var globalOriginCluster TestCluster
var globalTargetCluster TestCluster

func GetGlobalTestClusterOrigin() (TestCluster, error) {
	if createdGlobalClusters {
		return globalOriginCluster, nil
	}

	mux.Lock()
	defer mux.Unlock()
	if createdGlobalClusters {
		return globalOriginCluster, nil
	}

	err := createClusters()

	if err != nil {
		return nil, err
	}

	return globalOriginCluster, nil
}

func GetGlobalTestClusterTarget() (TestCluster, error) {
	if createdGlobalClusters {
		return globalTargetCluster, nil
	}

	mux.Lock()
	defer mux.Unlock()
	if createdGlobalClusters {
		return globalTargetCluster, nil
	}

	err := createClusters()

	if err != nil {
		return nil, err
	}

	return globalTargetCluster, nil
}

func createClusters() error {
	// assuming we have the lock already

	firstClusterId := r.Uint64() % (math.MaxUint64 - 1)

	var err error
	firstClusterName := fmt.Sprintf("test_cluster%d", firstClusterId)
	globalCcmClusterOrigin = ccm.NewCluster(firstClusterName, env.ServerVersion, env.IsDse, 1)
	err = globalCcmClusterOrigin.Create(originNodes)
	globalOriginCluster = globalCcmClusterOrigin

	if err != nil {
		return err
	}

	secondClusterId := firstClusterId + 1
	secondClusterName := fmt.Sprintf("test_cluster%d", secondClusterId)
	globalCcmClusterTarget = ccm.NewCluster(secondClusterName, env.ServerVersion, env.IsDse, 10)
	err = globalCcmClusterTarget.Create(targetNodes)
	globalTargetCluster = globalCcmClusterTarget

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

type TestSetup struct {
	origin TestCluster
	target TestCluster
	proxy  *cloudgateproxy.CloudgateProxy
}

func NewTestSetup() *TestSetup {
	origin, _ := simulacron.GetNewCluster(1)
	target, _ := simulacron.GetNewCluster(1)
	proxyInstance := NewProxyInstance(origin, target)
	return &TestSetup{
		origin: origin,
		target: target,
		proxy:  proxyInstance,
	}
}

func (setup *TestSetup) Cleanup() {
	setup.proxy.Shutdown()
	setup.target.Remove()
	setup.origin.Remove()
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
	conf.ProxyListenAddress = "localhost"
	conf.Debug = false
	conf.Test = false

	return conf
}
