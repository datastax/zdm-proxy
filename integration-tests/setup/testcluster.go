package setup

import (
	"context"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/ccm"
	"github.com/datastax/zdm-proxy/integration-tests/cqlserver"
	"github.com/datastax/zdm-proxy/integration-tests/env"
	"github.com/datastax/zdm-proxy/integration-tests/simulacron"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/datastax/zdm-proxy/proxy/pkg/zdmproxy"
	log "github.com/sirupsen/logrus"
	"math"
	"sync"
	"testing"
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
		globalCcmClusterOrigin.Remove()
		return err
	}

	sourceSession := globalCcmClusterOrigin.GetSession()
	destSession := globalCcmClusterTarget.GetSession()

	// Seed originCluster and targetCluster with keyspace
	err = SeedKeyspace(sourceSession)
	if err != nil {
		globalCcmClusterOrigin.Remove()
		globalCcmClusterTarget.Remove()
		return err
	}

	err = SeedKeyspace(destSession)
	if err != nil {
		globalCcmClusterOrigin.Remove()
		globalCcmClusterTarget.Remove()
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
	Proxy  *zdmproxy.ZdmProxy
}

func NewSimulacronTestSetupWithSession(t *testing.T, createProxy bool, createSession bool) (*SimulacronTestSetup, error) {
	return NewSimulacronTestSetupWithSessionAndConfig(t, createProxy, createSession, nil)
}

func NewSimulacronTestSetupWithSessionAndConfig(t *testing.T, createProxy bool, createSession bool, config *config.Config) (*SimulacronTestSetup, error) {
	return NewSimulacronTestSetupWithSessionAndNodesAndConfig(t, createProxy, createSession, 1, config)
}

func NewSimulacronTestSetupWithSessionAndNodes(t *testing.T, createProxy bool, createSession bool, nodes int) (*SimulacronTestSetup, error) {
	return NewSimulacronTestSetupWithSessionAndNodesAndConfig(t, createProxy, createSession, nodes, nil)
}

func NewSimulacronTestSetupWithSessionAndNodesAndConfig(t *testing.T, createProxy bool, createSession bool, nodes int, config *config.Config) (*SimulacronTestSetup, error) {
	if !env.RunMockTests {
		t.Skip("Skipping Simulacron tests, RUN_MOCKTESTS is set false")
	}
	origin, err := simulacron.GetNewCluster(createSession, nodes)
	if err != nil {
		log.Panic("simulacron origin startup failed: ", err)
	}
	target, err := simulacron.GetNewCluster(createSession, nodes)
	if err != nil {
		log.Panic("simulacron target startup failed: ", err)
	}
	var proxyInstance *zdmproxy.ZdmProxy
	if createProxy {
		if config == nil {
			config = NewTestConfig(origin.GetInitialContactPoint(), target.GetInitialContactPoint())
		} else {
			config.OriginContactPoints = origin.GetInitialContactPoint()
			config.TargetContactPoints = target.GetInitialContactPoint()
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

func NewSimulacronTestSetup(t *testing.T) (*SimulacronTestSetup, error) {
	return NewSimulacronTestSetupWithSession(t, true, false)
}

func NewSimulacronTestSetupWithConfig(t *testing.T, c *config.Config) (*SimulacronTestSetup, error) {
	return NewSimulacronTestSetupWithSessionAndConfig(t, true, false, c)
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
	Proxy  *zdmproxy.ZdmProxy
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

	var proxyInstance *zdmproxy.ZdmProxy
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
	Proxy  *zdmproxy.ZdmProxy
	Client *cqlserver.Client
}

func NewCqlServerTestSetup(t *testing.T, conf *config.Config, start bool, createProxy bool, connectClient bool) (*CqlServerTestSetup, error) {
    if !env.RunMockTests {
        t.Skip("Skipping CQLServer tests, RUN_MOCKTESTS is false")
    }
	origin, err := cqlserver.NewCqlServerCluster(conf.OriginContactPoints, conf.OriginPort,
		conf.OriginUsername, conf.OriginPassword, start)
	if err != nil {
		return nil, err
	}
	target, err := cqlserver.NewCqlServerCluster(conf.TargetContactPoints, conf.TargetPort,
		conf.TargetUsername, conf.TargetPassword, start)
	if err != nil {
		err2 := origin.Close()
		if err2 != nil {
			log.Warnf("error closing origin cql server cluster after target start failed: %v", err2)
		}
		return nil, err
	}

	var proxyInstance *zdmproxy.ZdmProxy
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

	cqlClient, err := cqlserver.NewCqlClient(conf.ProxyListenAddress, conf.ProxyListenPort,
		conf.OriginUsername, conf.OriginPassword, connectClient)

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
		log.Errorf("close target cql server error: %s", err)
	}

	err = setup.Origin.Close()
	if err != nil {
		log.Errorf("close origin cql server error: %s", err)
	}
}

func NewProxyInstance(origin TestCluster, target TestCluster) (*zdmproxy.ZdmProxy, error) {
	return NewProxyInstanceWithConfig(NewTestConfig(origin.GetInitialContactPoint(), target.GetInitialContactPoint()))
}

func NewProxyInstanceWithConfig(config *config.Config) (*zdmproxy.ZdmProxy, error) {
	return zdmproxy.Run(config, context.Background())
}

func NewTestConfig(originHost string, targetHost string) *config.Config {
	conf := config.New()

	conf.ProxyTopologyIndex = 0
	conf.ProxyTopologyAddresses = ""
	conf.ProxyTopologyNumTokens = 8

	conf.OriginEnableHostAssignment = true
	conf.TargetEnableHostAssignment = true

	conf.OriginContactPoints = originHost
	conf.OriginUsername = "cassandra"
	conf.OriginPassword = "cassandra"
	conf.OriginPort = 9042

	conf.TargetContactPoints = targetHost
	conf.TargetUsername = "cassandra"
	conf.TargetPassword = "cassandra"
	conf.TargetPort = 9042

	conf.ForwardClientCredentialsToOrigin = false

	conf.MetricsAddress = "localhost"
	conf.MetricsPort = 14001
	conf.ProxyListenPort = 14002
	conf.ProxyListenAddress = "localhost"

	conf.OriginConnectionTimeoutMs = 30000
	conf.TargetConnectionTimeoutMs = 30000
	conf.HeartbeatIntervalMs = 30000

	conf.HeartbeatRetryIntervalMaxMs = 30000
	conf.HeartbeatRetryIntervalMinMs = 250
	conf.HeartbeatRetryBackoffFactor = 2
	conf.HeartbeatFailureThreshold = 1

	conf.MetricsOriginLatencyBucketsMs = "1, 4, 7, 10, 25, 40, 60, 80, 100, 150, 250, 500, 1000, 2500, 5000, 10000, 15000"
	conf.MetricsTargetLatencyBucketsMs = "1, 4, 7, 10, 25, 40, 60, 80, 100, 150, 250, 500, 1000, 2500, 5000, 10000, 15000"
	conf.MetricsAsyncReadLatencyBucketsMs = "1, 4, 7, 10, 25, 40, 60, 80, 100, 150, 250, 500, 1000, 2500, 5000, 10000, 15000"

	conf.MetricsEnabled = true

	conf.RequestWriteQueueSizeFrames = 128
	conf.RequestWriteBufferSizeBytes = 4096
	conf.RequestReadBufferSizeBytes = 32768

	conf.ResponseWriteQueueSizeFrames = 128
	conf.ResponseWriteBufferSizeBytes = 8192
	conf.ResponseReadBufferSizeBytes = 32768

	conf.ProxyMaxClientConnections = 1000

	conf.RequestResponseMaxWorkers = -1
	conf.WriteMaxWorkers = -1
	conf.ReadMaxWorkers = -1
	conf.ListenerMaxWorkers = -1

	conf.EventQueueSizeFrames = 12

	conf.AsyncConnectorWriteQueueSizeFrames = 2048
	conf.AsyncConnectorWriteBufferSizeBytes = 4096

	conf.PrimaryCluster = config.PrimaryClusterOrigin
	conf.ReadMode = config.ReadModePrimaryOnly
	conf.SystemQueriesMode = config.SystemQueriesModeOrigin
	conf.AsyncHandshakeTimeoutMs = 4000

	conf.ProxyRequestTimeoutMs = 10000

	conf.LogLevel = "INFO"

	return conf
}
