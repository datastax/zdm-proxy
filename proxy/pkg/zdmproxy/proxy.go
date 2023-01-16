package zdmproxy

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/datastax/zdm-proxy/proxy/pkg/common"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics/noopmetrics"
	"github.com/datastax/zdm-proxy/proxy/pkg/metrics/prommetrics"
	"github.com/jpillora/backoff"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type ZdmProxy struct {
	Conf           *config.Config
	TopologyConfig *common.TopologyConfig

	originConnectionConfig ConnectionConfig
	targetConnectionConfig ConnectionConfig

	proxyTlsConfig *common.ProxyTlsConfig

	timeUuidGenerator TimeUuidGenerator

	primaryCluster    common.ClusterType
	readMode          common.ReadMode
	systemQueriesMode common.SystemQueriesMode

	proxyRand *rand.Rand

	lock *sync.RWMutex

	// Listener that enables the proxy to listen for clients on the port specified in the configuration
	clientListener net.Listener
	listenerLock   *sync.Mutex
	listenerClosed bool

	PreparedStatementCache *PreparedStatementCache

	controlConnShutdownCtx     context.Context
	controlConnCancelFn        context.CancelFunc
	controlConnShutdownWg      *sync.WaitGroup
	listenerShutdownWg         *sync.WaitGroup
	shutdownClientListenerChan chan bool

	targetControlConn *ControlConn
	originControlConn *ControlConn

	originBuckets []float64
	targetBuckets []float64
	asyncBuckets  []float64

	activeClients int32

	requestResponseNumWorkers int
	readNumWorkers            int
	writeNumWorkers           int
	listenerNumWorkers        int

	requestResponseScheduler *Scheduler
	writeScheduler           *Scheduler
	readScheduler            *Scheduler
	listenerScheduler        *Scheduler

	clientHandlersShutdownRequestCtx      context.Context
	clientHandlersShutdownRequestCancelFn context.CancelFunc
	globalClientHandlersWg                *sync.WaitGroup

	metricHandler *metrics.MetricHandler
}

func NewZdmProxy(conf *config.Config) (*ZdmProxy, error) {
	zdmProxy := &ZdmProxy{
		Conf: conf,
	}
	err := zdmProxy.initializeGlobalStructures()
	if err != nil {
		zdmProxy.Shutdown()
		return nil, err
	}
	return zdmProxy, nil
}

func (p *ZdmProxy) GetMetricHandler() *metrics.MetricHandler {
	return p.metricHandler
}

// Start starts up the proxy and start listening for client connections.
func (p *ZdmProxy) Start(ctx context.Context) error {
	log.Infof("Validating config...")
	err := p.Conf.Validate()
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	p.lock.Lock()
	p.timeUuidGenerator, err = GetDefaultTimeUuidGenerator()
	p.lock.Unlock()

	if err != nil {
		return fmt.Errorf("could not create timeuuid generator: %w", err)
	}

	p.lock.Lock()
	p.proxyTlsConfig, err = p.Conf.ParseProxyTlsConfig(true)
	p.lock.Unlock()

	if err != nil {
		return fmt.Errorf("could not initialize proxy TLS configuration: %w", err)
	}

	var serverSideTlsConfig *tls.Config
	if p.proxyTlsConfig.TlsEnabled {
		serverSideTlsConfig, err = getServerSideTlsConfigFromProxyClusterTlsConfig(p.proxyTlsConfig)

		if err != nil {
			return fmt.Errorf("could not create server side tls.Config object: %w", err)
		}
	}

	log.Infof("Starting proxy...")

	err = p.initializeMetricHandler()
	if err != nil {
		return err
	}

	err = p.initializeControlConnections(ctx)
	if err != nil {
		return err
	}

	originHosts, err := p.originControlConn.GetHostsInLocalDatacenter()
	if err != nil {
		return fmt.Errorf("failed to initialize proxy, could not get origin orderedHostsInLocalDc: %w", err)
	}

	originAssignedHosts, err := p.originControlConn.GetAssignedHosts()
	if err != nil {
		return fmt.Errorf("failed to initialize proxy, could not get assigned origin hosts: %w", err)
	}

	log.Infof("Initialized origin control connection. Cluster Name: %v, Hosts: %v, Assigned Hosts: %v.",
		p.originControlConn.GetClusterName(), originHosts, originAssignedHosts)

	targetHosts, err := p.targetControlConn.GetHostsInLocalDatacenter()
	if err != nil {
		return fmt.Errorf("failed to initialize proxy, could not get target orderedHostsInLocalDc: %w", err)
	}

	targetAssignedHosts, err := p.targetControlConn.GetAssignedHosts()
	if err != nil {
		return fmt.Errorf("failed to initialize proxy, could not get assigned target hosts: %w", err)
	}

	log.Infof("Initialized target control connection. Cluster Name: %v, Hosts: %v, Assigned Hosts: %v.",
		p.targetControlConn.GetClusterName(), targetHosts, targetAssignedHosts)

	err = p.acceptConnectionsFromClients(p.Conf.ProxyListenAddress, p.Conf.ProxyListenPort, serverSideTlsConfig)
	if err != nil {
		return err
	}

	log.Infof("Proxy connected and ready to accept queries on %v:%d", p.Conf.ProxyListenAddress, p.Conf.ProxyListenPort)
	return nil
}

func (p *ZdmProxy) initializeControlConnections(ctx context.Context) error {
	var err error

	topologyConfig, err := p.Conf.ParseTopologyConfig()
	if err != nil {
		return fmt.Errorf("failed to parse topology config: %w", err)
	}

	log.Infof("Parsed Topology Config: %v", topologyConfig)
	p.lock.Lock()
	p.TopologyConfig = topologyConfig
	p.lock.Unlock()

	parsedOriginContactPoints, err := p.Conf.ParseOriginContactPoints()
	if err != nil {
		return err
	}

	if parsedOriginContactPoints != nil {
		log.Infof("Parsed Origin contact points: %v", parsedOriginContactPoints)
	}

	parsedTargetContactPoints, err := p.Conf.ParseTargetContactPoints()
	if err != nil {
		return err
	}

	if parsedTargetContactPoints != nil {
		log.Infof("Parsed Target contact points: %v", parsedTargetContactPoints)
	}

	originTlsConfig, err := p.Conf.ParseOriginTlsConfig(true)
	if err != nil {
		return err
	}

	// Initialize origin connection configuration and control connection endpoint configuration
	originConnectionConfig, err := InitializeConnectionConfig(originTlsConfig,
		parsedOriginContactPoints,
		p.Conf.OriginPort,
		p.Conf.OriginConnectionTimeoutMs,
		common.ClusterTypeOrigin,
		p.Conf.OriginLocalDatacenter,
		ctx)
	if err != nil {
		return fmt.Errorf("error initializing the connection configuration or control connection for Origin: %w", err)
	}

	p.lock.Lock()
	p.originConnectionConfig = originConnectionConfig
	p.lock.Unlock()

	targetTlsConfig, err := p.Conf.ParseTargetTlsConfig(true)
	if err != nil {
		return err
	}

	// Initialize target connection configuration and control connection endpoint configuration
	targetConnectionConfig, err := InitializeConnectionConfig(targetTlsConfig,
		parsedTargetContactPoints,
		p.Conf.TargetPort,
		p.Conf.TargetConnectionTimeoutMs,
		common.ClusterTypeTarget,
		p.Conf.TargetLocalDatacenter,
		ctx)
	if err != nil {
		return fmt.Errorf("error initializing the connection configuration or control connection for Target: %w", err)
	}
	p.lock.Lock()
	p.targetConnectionConfig = targetConnectionConfig
	p.lock.Unlock()

	originControlConn := NewControlConn(
		p.controlConnShutdownCtx, p.Conf.OriginPort, p.originConnectionConfig,
		p.Conf.OriginUsername, p.Conf.OriginPassword, p.Conf, topologyConfig, p.proxyRand, p.metricHandler)

	if err := originControlConn.Start(p.controlConnShutdownWg, ctx); err != nil {
		return fmt.Errorf("failed to initialize origin control connection: %w", err)
	}

	p.lock.Lock()
	p.originControlConn = originControlConn
	p.lock.Unlock()

	targetControlConn := NewControlConn(
		p.controlConnShutdownCtx, p.Conf.TargetPort, p.targetConnectionConfig,
		p.Conf.TargetUsername, p.Conf.TargetPassword, p.Conf, topologyConfig, p.proxyRand, p.metricHandler)

	if err := targetControlConn.Start(p.controlConnShutdownWg, ctx); err != nil {
		return fmt.Errorf("failed to initialize target control connection: %w", err)
	}

	p.lock.Lock()
	p.targetControlConn = targetControlConn
	p.lock.Unlock()

	return nil
}

func (p *ZdmProxy) initializeMetricHandler() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	// This is the Prometheus-specific implementation of the MetricFactory object that will be provided to the global MetricHandler object
	// To switch to a different implementation, change the type instantiated here to another one that implements
	// metrics.MetricFactory.
	// You will also need to change the HTTP handler, see runner.go.

	var metricFactory metrics.MetricFactory
	if p.Conf.MetricsEnabled {
		metricFactory = prommetrics.NewPrometheusMetricFactory(prometheus.DefaultRegisterer, p.Conf.MetricsPrefix)
	} else {
		metricFactory = noopmetrics.NewNoopMetricFactory()
	}

	proxyMetrics, err := p.CreateProxyMetrics(metricFactory)
	if err != nil {
		return err
	}

	p.metricHandler = metrics.NewMetricHandler(
		metricFactory, p.originBuckets, p.targetBuckets, p.asyncBuckets, proxyMetrics,
		p.CreateOriginNodeMetrics, p.CreateTargetNodeMetrics, p.CreateAsyncNodeMetrics)

	return nil
}

func (p *ZdmProxy) initializeGlobalStructures() error {
	p.lock = &sync.RWMutex{}

	p.listenerLock = &sync.Mutex{}
	p.listenerClosed = false
	p.proxyRand = NewThreadSafeRand()

	maxProcs := runtime.GOMAXPROCS(0)

	var err error
	p.readMode, err = p.Conf.ParseReadMode()
	if err != nil {
		return err
	}

	p.primaryCluster, err = p.Conf.ParsePrimaryCluster()
	if err != nil {
		return err
	}

	p.systemQueriesMode, err = p.Conf.ParseSystemQueriesMode()
	if err != nil {
		return err
	}

	defaultReadWorkers := maxProcs * 8
	defaultWriteWorkers := maxProcs * 4
	if p.readMode == common.ReadModeDualAsyncOnSecondary {
		defaultReadWorkers = maxProcs * 12
		defaultWriteWorkers = maxProcs * 6
	}

	p.requestResponseNumWorkers = p.Conf.RequestResponseMaxWorkers
	if p.requestResponseNumWorkers == -1 {
		p.requestResponseNumWorkers = maxProcs * 4 // default
	} else if p.requestResponseNumWorkers <= 0 {
		log.Warnf("Invalid number of request / response workers %d, using GOMAXPROCS * 4 (%d).", p.requestResponseNumWorkers, maxProcs*4)
		p.requestResponseNumWorkers = maxProcs * 4
	}
	log.Infof("Using %d request / response workers.", p.requestResponseNumWorkers)

	p.writeNumWorkers = p.Conf.WriteMaxWorkers
	if p.writeNumWorkers == -1 {
		p.writeNumWorkers = defaultWriteWorkers // default
	} else if p.writeNumWorkers <= 0 {
		log.Warnf("Invalid number of write workers %d, using default (%d).", p.writeNumWorkers, defaultWriteWorkers)
		p.writeNumWorkers = defaultWriteWorkers
	}
	log.Infof("Using %d write workers.", p.writeNumWorkers)

	p.readNumWorkers = p.Conf.ReadMaxWorkers
	if p.readNumWorkers == -1 {
		p.readNumWorkers = defaultReadWorkers // default
	} else if p.readNumWorkers <= 0 {
		log.Warnf("Invalid number of read workers %d, using default (%d).", p.readNumWorkers, defaultReadWorkers)
		p.readNumWorkers = defaultReadWorkers
	}
	log.Infof("Using %d read workers.", p.readNumWorkers)

	p.listenerNumWorkers = p.Conf.ListenerMaxWorkers
	if p.listenerNumWorkers == -1 {
		p.listenerNumWorkers = maxProcs // default
	} else if p.listenerNumWorkers <= 0 {
		log.Warnf("Invalid number of connection listener workers %d, using GOMAXPROCS (%d).", p.listenerNumWorkers, maxProcs)
		p.listenerNumWorkers = maxProcs
	}
	log.Infof("Using %d listener workers.", p.listenerNumWorkers)

	p.requestResponseScheduler = NewScheduler(p.requestResponseNumWorkers)
	p.writeScheduler = NewScheduler(p.writeNumWorkers)
	p.readScheduler = NewScheduler(p.readNumWorkers)
	p.listenerScheduler = NewScheduler(p.listenerNumWorkers)

	p.lock.Lock()
	defer p.lock.Unlock()

	p.globalClientHandlersWg = &sync.WaitGroup{}
	p.clientHandlersShutdownRequestCtx, p.clientHandlersShutdownRequestCancelFn = context.WithCancel(context.Background())

	p.PreparedStatementCache = NewPreparedStatementCache()

	p.controlConnShutdownCtx, p.controlConnCancelFn = context.WithCancel(context.Background())
	p.controlConnShutdownWg = &sync.WaitGroup{}
	p.listenerShutdownWg = &sync.WaitGroup{}
	p.shutdownClientListenerChan = make(chan bool)

	p.originBuckets, err = p.Conf.ParseOriginBuckets()
	if err != nil {
		return fmt.Errorf("failed to parse origin latency buckets: %w", err)
	} else {
		log.Infof("Parsed Origin latency buckets: %v", p.originBuckets)
	}

	p.targetBuckets, err = p.Conf.ParseTargetBuckets()
	if err != nil {
		return fmt.Errorf("failed to parse target latency buckets: %w", err)
	} else {
		log.Infof("Parsed Target latency buckets: %v", p.targetBuckets)
	}

	p.asyncBuckets, err = p.Conf.ParseAsyncBuckets()
	if err != nil {
		return fmt.Errorf("failed to parse async latency buckets: %w", err)
	} else {
		log.Infof("Parsed Async latency buckets: %v", p.asyncBuckets)
	}

	p.activeClients = 0
	return nil
}

// acceptConnectionsFromClients creates a listener on the passed in port argument, and every connection
// that is received over that port instantiates a ClientHandler that then takes over managing that connection
func (p *ZdmProxy) acceptConnectionsFromClients(address string, port int, serverSideTlsConfig *tls.Config) error {

	protocol := "tcp"
	listenAddr := fmt.Sprintf("%s:%d", address, port)

	var l net.Listener
	var err error
	if serverSideTlsConfig == nil {
		l, err = net.Listen(protocol, listenAddr)
	} else {
		l, err = tls.Listen(protocol, listenAddr, serverSideTlsConfig)
	}

	if err != nil {
		return err
	}

	p.listenerLock.Lock()
	p.clientListener = l
	p.listenerLock.Unlock()

	p.listenerShutdownWg.Add(1)

	go func() {
		defer p.listenerShutdownWg.Done()
		defer func() {
			p.listenerLock.Lock()
			defer p.listenerLock.Unlock()
			if !p.listenerClosed {
				p.listenerClosed = true
				_ = l.Close()
			}
		}()
		wg := &sync.WaitGroup{}
		defer wg.Wait()
		for {
			conn, err := l.Accept()
			if err != nil {
				p.listenerLock.Lock()
				listenerClosed := p.listenerClosed
				p.listenerLock.Unlock()

				if listenerClosed {
					log.Debugf("Shutting down client listener on port %d", port)
					return
				}

				log.Errorf("Error while listening for new connections: %v", err)
				continue
			}

			currentClients := atomic.LoadInt32(&p.activeClients)
			if int(currentClients) >= p.Conf.ProxyMaxClientConnections {
				log.Warnf(
					"Refusing client connection from %v because max clients threshold has been hit (%v).",
					conn.RemoteAddr(), p.Conf.ProxyMaxClientConnections)
				err = conn.Close()
				if err != nil {
					log.Warnf("Error closing client connection from %v: %v", conn.RemoteAddr(), err)
				}
				continue
			}

			atomic.AddInt32(&p.activeClients, 1)
			log.Infof("Accepted connection from %v", conn.RemoteAddr())

			wg.Add(1)
			p.listenerScheduler.Schedule(func() {
				defer wg.Done()
				p.handleNewConnection(conn)
			})
		}
	}()

	return nil
}

// handleNewConnection creates the client handler and connectors for the new client connection
func (p *ZdmProxy) handleNewConnection(clientConn net.Conn) {

	errFunc := func(e error) {
		log.Errorf("Client Handler could not be created: %v", e)
		clientConn.Close()
		atomic.AddInt32(&p.activeClients, -1)
	}

	// there is a ClientHandler for each connection made by a client

	var originEndpoint Endpoint
	var originHost *Host
	var err error
	if p.Conf.OriginEnableHostAssignment {
		originHost, err = p.originControlConn.NextAssignedHost()
		if err != nil {
			errFunc(err)
			return
		}
		originEndpoint = p.originConnectionConfig.CreateEndpoint(originHost)
	} else {
		originEndpoint = p.originControlConn.GetCurrentContactPoint()
		if originEndpoint == nil {
			log.Warnf("Origin ControlConnection current endpoint is nil, "+
				"falling back to first origin contact point (%v) for client connection %v.",
				p.originConnectionConfig.GetContactPoints()[0].String(), clientConn.RemoteAddr().String())
		}
	}

	var targetEndpoint Endpoint
	var targetHost *Host
	if p.Conf.TargetEnableHostAssignment {
		targetHost, err = p.targetControlConn.NextAssignedHost()
		if err != nil {
			errFunc(err)
			return
		}
		targetEndpoint = p.targetConnectionConfig.CreateEndpoint(targetHost)
	} else {
		targetEndpoint = p.targetControlConn.GetCurrentContactPoint()
		if targetEndpoint == nil {
			log.Warnf("Target ControlConnection current endpoint is nil, "+
				"falling back to first target contact point (%v) for client connection %v.",
				p.targetConnectionConfig.GetContactPoints()[0].String(), clientConn.RemoteAddr().String())
		}
	}

	originCassandraConnInfo := NewClusterConnectionInfo(p.originConnectionConfig, originEndpoint, true)
	targetCassandraConnInfo := NewClusterConnectionInfo(p.targetConnectionConfig, targetEndpoint, false)
	clientHandler, err := NewClientHandler(
		clientConn,
		originCassandraConnInfo,
		targetCassandraConnInfo,
		p.originControlConn,
		p.targetControlConn,
		p.Conf,
		p.TopologyConfig,
		p.Conf.TargetUsername,
		p.Conf.TargetPassword,
		p.Conf.OriginUsername,
		p.Conf.OriginPassword,
		p.PreparedStatementCache,
		p.metricHandler,
		p.globalClientHandlersWg,
		p.requestResponseScheduler,
		p.readScheduler,
		p.writeScheduler,
		p.requestResponseNumWorkers,
		p.clientHandlersShutdownRequestCtx,
		originHost,
		targetHost,
		p.timeUuidGenerator,
		p.readMode,
		p.primaryCluster,
		p.systemQueriesMode)

	if err != nil {
		errFunc(err)
		return
	}

	log.Tracef("ClientHandler created")
	clientHandler.run(&p.activeClients)
}

func (p *ZdmProxy) Shutdown() {
	log.Info("Initiating proxy shutdown...")

	log.Debug("Requesting shutdown of the client listener...")
	p.listenerLock.Lock()
	if !p.listenerClosed {
		p.listenerClosed = true
		if p.clientListener != nil {
			p.clientListener.Close()
		}
	}
	p.listenerLock.Unlock()

	p.listenerShutdownWg.Wait()

	log.Debug("Requesting shutdown of the client handlers...")
	p.clientHandlersShutdownRequestCancelFn()

	log.Debug("Waiting until all client handlers are done...")
	p.globalClientHandlersWg.Wait()

	log.Debug("Requesting shutdown of the control connections...")
	p.controlConnCancelFn()

	log.Debug("Waiting until control connections done...")
	p.controlConnShutdownWg.Wait()

	log.Debug("Shutting down the schedulers and metrics handler...")
	p.requestResponseScheduler.Shutdown()
	p.writeScheduler.Shutdown()
	p.readScheduler.Shutdown()
	p.listenerScheduler.Shutdown()

	p.lock.Lock()
	if p.metricHandler != nil {
		err := p.metricHandler.UnregisterAllMetrics()
		if err != nil {
			log.Warnf("Failed to unregister metrics: %v.", err)
		}
	}
	p.lock.Unlock()

	log.Info("Proxy shutdown complete.")
}

func (p *ZdmProxy) GetOriginControlConn() *ControlConn {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.originControlConn
}

func (p *ZdmProxy) GetTargetControlConn() *ControlConn {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.targetControlConn
}

func Run(conf *config.Config, ctx context.Context) (*ZdmProxy, error) {
	zdmProxy, err := NewZdmProxy(conf)
	if err != nil {
		log.Errorf("Couldn't create proxy: %v.", err)
		return nil, err
	}

	err = zdmProxy.Start(ctx)
	if err != nil {
		log.Errorf("Couldn't start proxy: %v.", err)
		zdmProxy.Shutdown()
		return nil, err
	}

	return zdmProxy, nil
}

func RunWithRetries(conf *config.Config, ctx context.Context, b *backoff.Backoff) (*ZdmProxy, error) {
	log.Info("Attempting to start the proxy...")
	for {
		zdmProxy, err := Run(conf, ctx)
		if zdmProxy != nil {
			return zdmProxy, nil
		}

		nextDuration := b.Duration()
		if !errors.Is(err, ShutdownErr) {
			log.Errorf("Couldn't start proxy, retrying in %v: %v.", nextDuration, err)
		}
		timedOut, _ := sleepWithContext(nextDuration, ctx, nil)
		if !timedOut {
			log.Info("Cancellation detected. Aborting proxy startup...")
			return nil, ShutdownErr
		}
	}
}

// sleepWithContext returns false if context Done() returns
func sleepWithContext(d time.Duration, ctx context.Context, reconnectCh chan bool) (timedOut bool, reconnect bool) {
	select {
	case <-time.After(d):
		return true, false
	case <-ctx.Done():
		return false, false
	case <-reconnectCh:
		return false, true
	}
}

func (p *ZdmProxy) CreateProxyMetrics(metricFactory metrics.MetricFactory) (*metrics.ProxyMetrics, error) {
	failedReadsOrigin, err := metricFactory.GetOrCreateCounter(metrics.FailedReadsOrigin)
	if err != nil {
		return nil, err
	}

	failedReadsTarget, err := metricFactory.GetOrCreateCounter(metrics.FailedReadsTarget)
	if err != nil {
		return nil, err
	}

	failedWritesOnOrigin, err := metricFactory.GetOrCreateCounter(metrics.FailedWritesOnOrigin)
	if err != nil {
		return nil, err
	}

	failedWritesOnTarget, err := metricFactory.GetOrCreateCounter(metrics.FailedWritesOnTarget)
	if err != nil {
		return nil, err
	}

	failedWritesOnBoth, err := metricFactory.GetOrCreateCounter(metrics.FailedWritesOnBoth)
	if err != nil {
		return nil, err
	}

	psCacheSize, err := metricFactory.GetOrCreateGaugeFunc(metrics.PSCacheSize, p.PreparedStatementCache.GetPreparedStatementCacheSize)
	if err != nil {
		return nil, err
	}

	psCacheMissCount, err := metricFactory.GetOrCreateCounter(metrics.PSCacheMissCount)
	if err != nil {
		return nil, err
	}

	proxyReadsOriginDuration, err := metricFactory.GetOrCreateHistogram(metrics.ProxyReadsOriginDuration, p.originBuckets)
	if err != nil {
		return nil, err
	}

	proxyReadsTargetDuration, err := metricFactory.GetOrCreateHistogram(metrics.ProxyReadsTargetDuration, p.targetBuckets)
	if err != nil {
		return nil, err
	}

	proxyWritesDuration, err := metricFactory.GetOrCreateHistogram(metrics.ProxyWritesDuration, p.originBuckets)
	if err != nil {
		return nil, err
	}

	inFlightReadsOrigin, err := metricFactory.GetOrCreateGauge(metrics.InFlightReadsOrigin)
	if err != nil {
		return nil, err
	}

	inFlightReadsTarget, err := metricFactory.GetOrCreateGauge(metrics.InFlightReadsTarget)
	if err != nil {
		return nil, err
	}

	inFlightWrites, err := metricFactory.GetOrCreateGauge(metrics.InFlightWrites)
	if err != nil {
		return nil, err
	}

	openClientConnections, err := metricFactory.GetOrCreateGaugeFunc(metrics.OpenClientConnections, func() float64 {
		return float64(atomic.LoadInt32(&p.activeClients))
	})
	if err != nil {
		return nil, err
	}

	proxyMetrics := &metrics.ProxyMetrics{
		FailedReadsOrigin:        failedReadsOrigin,
		FailedReadsTarget:        failedReadsTarget,
		FailedWritesOnOrigin:     failedWritesOnOrigin,
		FailedWritesOnTarget:     failedWritesOnTarget,
		FailedWritesOnBoth:       failedWritesOnBoth,
		PSCacheSize:              psCacheSize,
		PSCacheMissCount:         psCacheMissCount,
		ProxyReadsOriginDuration: proxyReadsOriginDuration,
		ProxyReadsTargetDuration: proxyReadsTargetDuration,
		ProxyWritesDuration:      proxyWritesDuration,
		InFlightReadsOrigin:      inFlightReadsOrigin,
		InFlightReadsTarget:      inFlightReadsTarget,
		InFlightWrites:           inFlightWrites,
		OpenClientConnections:    openClientConnections,
	}

	return proxyMetrics, nil
}

func (p *ZdmProxy) CreateOriginNodeMetrics(
	metricFactory metrics.MetricFactory, originNodeDescription string, originBuckets []float64) (*metrics.NodeMetricsInstance, error) {
	originClientTimeouts, err := metrics.CreateCounterNodeMetric(metricFactory, originNodeDescription, metrics.OriginClientTimeouts)
	if err != nil {
		return nil, err
	}

	originReadTimeouts, err := metrics.CreateCounterNodeMetric(metricFactory, originNodeDescription, metrics.OriginReadTimeouts)
	if err != nil {
		return nil, err
	}

	originReadFailures, err := metrics.CreateCounterNodeMetric(metricFactory, originNodeDescription, metrics.OriginReadFailures)
	if err != nil {
		return nil, err
	}

	originWriteTimeouts, err := metrics.CreateCounterNodeMetric(metricFactory, originNodeDescription, metrics.OriginWriteTimeouts)
	if err != nil {
		return nil, err
	}

	originWriteFailures, err := metrics.CreateCounterNodeMetric(metricFactory, originNodeDescription, metrics.OriginWriteFailures)
	if err != nil {
		return nil, err
	}

	originUnpreparedErrors, err := metrics.CreateCounterNodeMetric(metricFactory, originNodeDescription, metrics.OriginUnpreparedErrors)
	if err != nil {
		return nil, err
	}

	originOverloadedErrors, err := metrics.CreateCounterNodeMetric(metricFactory, originNodeDescription, metrics.OriginOverloadedErrors)
	if err != nil {
		return nil, err
	}

	originUnavailableErrors, err := metrics.CreateCounterNodeMetric(metricFactory, originNodeDescription, metrics.OriginUnavailableErrors)
	if err != nil {
		return nil, err
	}

	originOtherErrors, err := metrics.CreateCounterNodeMetric(metricFactory, originNodeDescription, metrics.OriginOtherErrors)
	if err != nil {
		return nil, err
	}

	originRequestDuration, err := metrics.CreateHistogramNodeMetric(metricFactory, originNodeDescription, metrics.OriginRequestDuration, originBuckets)
	if err != nil {
		return nil, err
	}

	openOriginConnections, err := metrics.CreateGaugeNodeMetric(metricFactory, originNodeDescription, metrics.OpenOriginConnections)
	if err != nil {
		return nil, err
	}

	// inflight requests metric for non async requests are implemented as proxy level metrics (not node metrics)
	inflightRequests, err := noopmetrics.NewNoopMetricFactory().GetOrCreateGauge(nil)
	if err != nil {
		return nil, err
	}

	originUsedStreamIds, err := metrics.CreateGaugeNodeMetric(metricFactory, originNodeDescription, metrics.OriginUsedStreamIds)
	if err != nil {
		return nil, err
	}

	return &metrics.NodeMetricsInstance{
		ClientTimeouts:    originClientTimeouts,
		ReadTimeouts:      originReadTimeouts,
		ReadFailures:      originReadFailures,
		WriteTimeouts:     originWriteTimeouts,
		WriteFailures:     originWriteFailures,
		UnpreparedErrors:  originUnpreparedErrors,
		OverloadedErrors:  originOverloadedErrors,
		UnavailableErrors: originUnavailableErrors,
		OtherErrors:       originOtherErrors,
		RequestDuration:   originRequestDuration,
		OpenConnections:   openOriginConnections,
		InFlightRequests:  inflightRequests,
		UsedStreamIds:     originUsedStreamIds,
	}, nil
}

func (p *ZdmProxy) CreateAsyncNodeMetrics(
	metricFactory metrics.MetricFactory, asyncNodeDescription string, asyncBuckets []float64) (*metrics.NodeMetricsInstance, error) {
	asyncClientTimeouts, err := metrics.CreateCounterNodeMetric(metricFactory, asyncNodeDescription, metrics.AsyncClientTimeouts)
	if err != nil {
		return nil, err
	}

	asyncReadTimeouts, err := metrics.CreateCounterNodeMetric(metricFactory, asyncNodeDescription, metrics.AsyncReadTimeouts)
	if err != nil {
		return nil, err
	}

	asyncReadFailures, err := metrics.CreateCounterNodeMetric(metricFactory, asyncNodeDescription, metrics.AsyncReadFailures)
	if err != nil {
		return nil, err
	}

	asyncWriteTimeouts, err := metrics.CreateCounterNodeMetric(metricFactory, asyncNodeDescription, metrics.AsyncWriteTimeouts)
	if err != nil {
		return nil, err
	}

	asyncWriteFailures, err := metrics.CreateCounterNodeMetric(metricFactory, asyncNodeDescription, metrics.AsyncWriteFailures)
	if err != nil {
		return nil, err
	}

	asyncUnpreparedErrors, err := metrics.CreateCounterNodeMetric(metricFactory, asyncNodeDescription, metrics.AsyncUnpreparedErrors)
	if err != nil {
		return nil, err
	}

	asyncOverloadedErrors, err := metrics.CreateCounterNodeMetric(metricFactory, asyncNodeDescription, metrics.AsyncOverloadedErrors)
	if err != nil {
		return nil, err
	}

	asyncUnavailableErrors, err := metrics.CreateCounterNodeMetric(metricFactory, asyncNodeDescription, metrics.AsyncUnavailableErrors)
	if err != nil {
		return nil, err
	}

	asyncOtherErrors, err := metrics.CreateCounterNodeMetric(metricFactory, asyncNodeDescription, metrics.AsyncOtherErrors)
	if err != nil {
		return nil, err
	}

	asyncRequestDuration, err := metrics.CreateHistogramNodeMetric(metricFactory, asyncNodeDescription, metrics.AsyncRequestDuration, asyncBuckets)
	if err != nil {
		return nil, err
	}

	openAsyncConnections, err := metrics.CreateGaugeNodeMetric(metricFactory, asyncNodeDescription, metrics.OpenAsyncConnections)
	if err != nil {
		return nil, err
	}

	inflightRequestsAsync, err := metrics.CreateGaugeNodeMetric(metricFactory, asyncNodeDescription, metrics.InFlightRequestsAsync)
	if err != nil {
		return nil, err
	}

	asyncUsedStreamIds, err := metrics.CreateGaugeNodeMetric(metricFactory, asyncNodeDescription, metrics.AsyncUsedStreamIds)
	if err != nil {
		return nil, err
	}

	return &metrics.NodeMetricsInstance{
		ClientTimeouts:    asyncClientTimeouts,
		ReadTimeouts:      asyncReadTimeouts,
		ReadFailures:      asyncReadFailures,
		WriteTimeouts:     asyncWriteTimeouts,
		WriteFailures:     asyncWriteFailures,
		UnpreparedErrors:  asyncUnpreparedErrors,
		OverloadedErrors:  asyncOverloadedErrors,
		UnavailableErrors: asyncUnavailableErrors,
		OtherErrors:       asyncOtherErrors,
		RequestDuration:   asyncRequestDuration,
		OpenConnections:   openAsyncConnections,
		InFlightRequests:  inflightRequestsAsync,
		UsedStreamIds:     asyncUsedStreamIds,
	}, nil
}

func (p *ZdmProxy) CreateTargetNodeMetrics(
	metricFactory metrics.MetricFactory, targetNodeDescription string, targetBuckets []float64) (*metrics.NodeMetricsInstance, error) {
	targetClientTimeouts, err := metrics.CreateCounterNodeMetric(metricFactory, targetNodeDescription, metrics.TargetClientTimeouts)
	if err != nil {
		return nil, err
	}

	targetReadTimeouts, err := metrics.CreateCounterNodeMetric(metricFactory, targetNodeDescription, metrics.TargetReadTimeouts)
	if err != nil {
		return nil, err
	}

	targetReadFailures, err := metrics.CreateCounterNodeMetric(metricFactory, targetNodeDescription, metrics.TargetReadFailures)
	if err != nil {
		return nil, err
	}

	targetWriteTimeouts, err := metrics.CreateCounterNodeMetric(metricFactory, targetNodeDescription, metrics.TargetWriteTimeouts)
	if err != nil {
		return nil, err
	}

	targetWriteFailures, err := metrics.CreateCounterNodeMetric(metricFactory, targetNodeDescription, metrics.TargetWriteFailures)
	if err != nil {
		return nil, err
	}

	targetUnpreparedErrors, err := metrics.CreateCounterNodeMetric(metricFactory, targetNodeDescription, metrics.TargetUnpreparedErrors)
	if err != nil {
		return nil, err
	}

	targetOverloadedErrors, err := metrics.CreateCounterNodeMetric(metricFactory, targetNodeDescription, metrics.TargetOverloadedErrors)
	if err != nil {
		return nil, err
	}

	targetUnavailableErrors, err := metrics.CreateCounterNodeMetric(metricFactory, targetNodeDescription, metrics.TargetUnavailableErrors)
	if err != nil {
		return nil, err
	}

	targetOtherErrors, err := metrics.CreateCounterNodeMetric(metricFactory, targetNodeDescription, metrics.TargetOtherErrors)
	if err != nil {
		return nil, err
	}

	targetRequestDuration, err := metrics.CreateHistogramNodeMetric(metricFactory, targetNodeDescription, metrics.TargetRequestDuration, targetBuckets)
	if err != nil {
		return nil, err
	}

	openTargetConnections, err := metrics.CreateGaugeNodeMetric(metricFactory, targetNodeDescription, metrics.OpenTargetConnections)
	if err != nil {
		return nil, err
	}

	// inflight requests metric for non async requests are implemented as proxy level metrics (not node metrics)
	inflightRequests, err := noopmetrics.NewNoopMetricFactory().GetOrCreateGauge(nil)
	if err != nil {
		return nil, err
	}

	targetUsedStreamIds, err := metrics.CreateGaugeNodeMetric(metricFactory, targetNodeDescription, metrics.TargetUsedStreamIds)
	if err != nil {
		return nil, err
	}

	return &metrics.NodeMetricsInstance{
		ClientTimeouts:    targetClientTimeouts,
		ReadTimeouts:      targetReadTimeouts,
		ReadFailures:      targetReadFailures,
		WriteTimeouts:     targetWriteTimeouts,
		WriteFailures:     targetWriteFailures,
		UnpreparedErrors:  targetUnpreparedErrors,
		OverloadedErrors:  targetOverloadedErrors,
		UnavailableErrors: targetUnavailableErrors,
		OtherErrors:       targetOtherErrors,
		RequestDuration:   targetRequestDuration,
		OpenConnections:   openTargetConnections,
		InFlightRequests:  inflightRequests,
		UsedStreamIds:     targetUsedStreamIds,
	}, nil
}
