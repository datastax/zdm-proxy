package setup

import (
	"context"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/integration-tests/ccm"
	"github.com/riptano/cloud-gate/integration-tests/cqlserver"
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

	globalCcmClusterTarget.Remove()
	globalCcmClusterOrigin.Remove()
}

type SimulacronTestSetup struct {
	Origin *simulacron.Cluster
	Target *simulacron.Cluster
	Proxy  *cloudgateproxy.CloudgateProxy
}

func NewSimulacronTestSetupWithSession(createProxy bool, createSession bool) (*SimulacronTestSetup, error) {
	return NewSimulacronTestSetupWithSessionAndConfig(createProxy, createSession, nil)
}

func NewSimulacronTestSetupWithSessionAndConfig(createProxy bool, createSession bool, config *config.Config) (*SimulacronTestSetup, error) {
	return NewSimulacronTestSetupWithSessionAndNodesAndConfig(createProxy, createSession, 1, config)
}

func NewSimulacronTestSetupWithSessionAndNodes(createProxy bool, createSession bool, nodes int) (*SimulacronTestSetup, error) {
	return NewSimulacronTestSetupWithSessionAndNodesAndConfig(createProxy, createSession, nodes, nil)
}

func NewSimulacronTestSetupWithSessionAndNodesAndConfig(createProxy bool, createSession bool, nodes int, config *config.Config) (*SimulacronTestSetup, error) {
	origin, err := simulacron.GetNewCluster(createSession, nodes)
	if err != nil {
		log.Panic("simulacron origin startup failed: ", err)
	}
	target, err := simulacron.GetNewCluster(createSession, nodes)
	if err != nil {
		log.Panic("simulacron target startup failed: ", err)
	}
	var proxyInstance *cloudgateproxy.CloudgateProxy
	if createProxy {
		if config == nil {
			config = NewTestConfig(origin.GetInitialContactPoint(), target.GetInitialContactPoint())
		} else {
			config.OriginCassandraContactPoints = origin.GetInitialContactPoint()
			config.TargetCassandraContactPoints = target.GetInitialContactPoint()
		}
		proxyInstance, err = NewProxyInstanceWithConfig(config)
		if err != nil {
			return nil, err
		}
	} else {
		proxyInstance = nil
	}
	return &SimulacronTestSetup{
		Origin: origin,
		Target: target,
		Proxy:  proxyInstance,
	}, nil
}

func NewSimulacronTestSetup() (*SimulacronTestSetup, error) {
	return NewSimulacronTestSetupWithSession(true, false)
}

func NewSimulacronTestSetupWithConfig(c *config.Config) (*SimulacronTestSetup, error) {
	return NewSimulacronTestSetupWithSessionAndConfig(true, false, c)
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

type CcmTestSetup struct {
	Origin *ccm.Cluster
	Target *ccm.Cluster
	Proxy  *cloudgateproxy.CloudgateProxy
}

func NewTemporaryCcmTestSetup(start bool, createProxy bool) (*CcmTestSetup, error) {
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
	if createProxy {
		proxyInstance, err = NewProxyInstance(origin, target)
		if err != nil {
			return nil, err
		}
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
		proxy, err := NewProxyInstanceWithConfig(config)
		if err != nil {
			return err
		}
		setup.Proxy = proxy
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

type CqlServerTestSetup struct {
	Origin *cqlserver.Cluster
	Target *cqlserver.Cluster
	Proxy  *cloudgateproxy.CloudgateProxy
	Client *cqlserver.Client
}

func NewCqlServerTestSetup(conf *config.Config, start bool, createProxy bool, connectClient bool) (*CqlServerTestSetup, error) {
	origin, err := cqlserver.NewCqlServerCluster(conf.OriginCassandraContactPoints, conf.OriginCassandraPort,
		conf.OriginCassandraUsername, conf.OriginCassandraPassword, start)
	if err != nil {
		return nil, err
	}
	target, err := cqlserver.NewCqlServerCluster(conf.TargetCassandraContactPoints, conf.TargetCassandraPort,
		conf.TargetCassandraUsername, conf.TargetCassandraPassword, start)
	if err != nil {
		err2 := origin.Close()
		if err2 != nil {
			log.Warnf("error closing origin cql server cluster after target start failed: %v", err2)
		}
		return nil, err
	}

	var proxyInstance *cloudgateproxy.CloudgateProxy
	if createProxy {
		proxyInstance, err = NewProxyInstanceWithConfig(conf)
		if err != nil {
			err2 := origin.Close()
			if err2 != nil {
				log.Warnf("error closing origin cql server cluster after target start failed: %v", err2)
			}
			err2 = target.Close()
			if err2 != nil {
				log.Warnf("error closing origin cql server cluster after target start failed: %v", err2)
			}
			return nil, err
		}
	} else {
		proxyInstance = nil
	}

	cqlClient, err := cqlserver.NewCqlClient(conf.ProxyQueryAddress, conf.ProxyQueryPort,
		conf.OriginCassandraUsername, conf.OriginCassandraPassword, connectClient)

	if err != nil {
		err2 := origin.Close()
		if err2 != nil {
			log.Warnf("error closing origin cql server cluster after target start failed: %v", err2)
		}
		err2 = target.Close()
		if err2 != nil {
			log.Warnf("error closing origin cql server cluster after target start failed: %v", err2)
		}
		if proxyInstance != nil {
			proxyInstance.Shutdown()
		}
		return nil, err
	}

	return &CqlServerTestSetup{
		Origin: origin,
		Target: target,
		Proxy:  proxyInstance,
		Client: cqlClient,
	}, nil
}

func (setup *CqlServerTestSetup) Start(config *config.Config, connectClient bool, version primitive.ProtocolVersion) error {
	err := setup.Origin.Start()
	if err != nil {
		return err
	}
	err = setup.Target.Start()
	if err != nil {
		return err
	}
	if config != nil {
		proxy, err := NewProxyInstanceWithConfig(config)
		if err != nil {
			return err
		}
		setup.Proxy = proxy
	}
	if connectClient {
		err := setup.Client.Connect(version)
		if err != nil {
			return err
		}
	}
	return nil
}

func (setup *CqlServerTestSetup) Cleanup() {
	if setup.Proxy != nil {
		setup.Proxy.Shutdown()
	}

	err := setup.Target.Close()
	if err != nil {
		log.Errorf("remove target ccm cluster error: %s", err)
	}

	err = setup.Origin.Close()
	if err != nil {
		log.Errorf("remove origin ccm cluster error: %s", err)
	}
}

func NewProxyInstance(origin TestCluster, target TestCluster) (*cloudgateproxy.CloudgateProxy, error) {
	return NewProxyInstanceWithConfig(NewTestConfig(origin.GetInitialContactPoint(), target.GetInitialContactPoint()))
}

func NewProxyInstanceWithConfig(config *config.Config) (*cloudgateproxy.CloudgateProxy, error) {
	return cloudgateproxy.Run(config, context.Background())
}

func NewTestConfig(originHost string, targetHost string) *config.Config {
	conf := config.New()

	conf.ProxyIndex = 0
	conf.ProxyInstanceCount = -1
	conf.ProxyAddresses = ""
	conf.ProxyNumTokens = 8

	conf.OriginEnableHostAssignment = true
	conf.TargetEnableHostAssignment = true

	conf.OriginCassandraContactPoints = originHost
	conf.OriginCassandraUsername = "cassandra"
	conf.OriginCassandraPassword = "cassandra"
	conf.OriginCassandraPort = 9042

	conf.TargetCassandraContactPoints = targetHost
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
	conf.HeartbeatRetryIntervalMinMs = 250
	conf.HeartbeatRetryBackoffFactor = 2
	conf.HeartbeatFailureThreshold = 1

	conf.OriginBucketsMs = "1, 4, 7, 10, 25, 40, 60, 80, 100, 150, 250, 500, 1000, 2500, 5000, 10000, 15000"
	conf.TargetBucketsMs = "1, 4, 7, 10, 25, 40, 60, 80, 100, 150, 250, 500, 1000, 2500, 5000, 10000, 15000"

	conf.EnableMetrics = true

	conf.RequestWriteQueueSizeFrames = 128
	conf.RequestWriteBufferSizeBytes = 4096
	conf.RequestReadBufferSizeBytes = 32768

	conf.ResponseWriteQueueSizeFrames = 128
	conf.ResponseWriteBufferSizeBytes = 8192
	conf.ResponseReadBufferSizeBytes = 32768

	conf.MaxClientsThreshold = 500

	conf.RequestResponseMaxWorkers = -1
	conf.WriteMaxWorkers = -1
	conf.ReadMaxWorkers = -1
	conf.ListenerMaxWorkers = -1

	conf.EventQueueSizeFrames = 12

	conf.ForwardReadsToTarget = false
	conf.ForwardSystemQueriesToTarget = false

	conf.RequestTimeoutMs = 10000

	conf.LogLevel = "INFO"

	return conf
}
