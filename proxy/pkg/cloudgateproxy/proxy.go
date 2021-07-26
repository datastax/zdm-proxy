package cloudgateproxy

import (
	"context"
	"errors"
	"fmt"
	"github.com/jpillora/backoff"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics/noopmetrics"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics/prommetrics"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type CloudgateProxy struct {
	Conf           *config.Config
	TopologyConfig *config.TopologyConfig

	originConnectionConfig *ConnectionConfig
	originContactPoints    []Endpoint

	targetConnectionConfig *ConnectionConfig
	targetContactPoints    []Endpoint

	proxyRand *rand.Rand

	lock *sync.RWMutex

	// Listener that enables the proxy to listen for clients on the port specified in the configuration
	clientListener net.Listener
	listenerLock   *sync.Mutex
	listenerClosed bool

	PreparedStatementCache *PreparedStatementCache

	shutdownContext            context.Context
	cancelFunc                 context.CancelFunc
	shutdownWaitGroup          *sync.WaitGroup
	shutdownClientListenerChan chan bool

	targetControlConn *ControlConn
	originControlConn *ControlConn

	originBuckets []float64
	targetBuckets []float64

	activeClients int32

	requestResponseNumWorkers int
	readNumWorkers            int
	writeNumWorkers           int
	listenerNumWorkers        int

	requestResponseScheduler *Scheduler
	writeScheduler           *Scheduler
	readScheduler            *Scheduler
	listenerScheduler        *Scheduler

	shutdownRequestCtx      context.Context
	shutdownRequestCancelFn context.CancelFunc
	requestLoopWaitGroup    *sync.WaitGroup

	metricHandler *metrics.MetricHandler
}

func NewCloudgateProxy(conf *config.Config) *CloudgateProxy {
	cp := &CloudgateProxy{
		Conf: conf,
	}
	cp.initializeGlobalStructures()
	return cp
}

func (p *CloudgateProxy) GetMetricHandler() *metrics.MetricHandler {
	return p.metricHandler
}

// Start starts up the proxy and start listening for client connections.
func (p *CloudgateProxy) Start(ctx context.Context) error {
	log.Infof("Validating config...")
	err := p.Conf.Validate()
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	log.Infof("Starting proxy...")

	err = p.initializeControlConnections(ctx)
	if err != nil {
		return err
	}

	originHosts, err := p.originControlConn.GetHostsInLocalDatacenter()
	if err != nil {
		return fmt.Errorf("failed to initialize proxy, could not get origin hostsInLocalDc: %w", err)
	}

	originAssignedHosts, err := p.originControlConn.GetAssignedHosts()
	if err != nil {
		return fmt.Errorf("failed to initialize proxy, could not get assigned origin hosts: %w", err)
	}

	log.Infof("Initialized origin control connection. Cluster Name: %v, Hosts: %v, Assigned Hosts: %v.",
		p.originControlConn.GetClusterName(), originHosts, originAssignedHosts)

	targetHosts, err := p.targetControlConn.GetHostsInLocalDatacenter()
	if err != nil {
		return fmt.Errorf("failed to initialize proxy, could not get target hostsInLocalDc: %w", err)
	}

	targetAssignedHosts, err := p.targetControlConn.GetAssignedHosts()
	if err != nil {
		return fmt.Errorf("failed to initialize proxy, could not get assigned target hosts: %w", err)
	}

	log.Infof("Initialized target control connection. Cluster Name: %v, Hosts: %v, Assigned Hosts: %v.",
		p.targetControlConn.GetClusterName(), targetHosts, targetAssignedHosts)

	err = p.initializeMetricHandler()
	if err != nil {
		return err
	}

	err = p.acceptConnectionsFromClients(p.Conf.ProxyQueryAddress, p.Conf.ProxyQueryPort)
	if err != nil {
		return err
	}

	log.Infof("Proxy connected and ready to accept queries on %v:%d", p.Conf.ProxyQueryAddress, p.Conf.ProxyQueryPort)
	return nil
}


func (p *CloudgateProxy) initializeControlConnections(ctx context.Context) error {
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

	// Initialize origin connection configuration and control connection endpoint configuration
	originConnectionConfig, originContactPoints, err := initializeConnectionConfig(p.Conf.OriginCassandraSecureConnectBundlePath,
		parsedOriginContactPoints,
		p.Conf.OriginCassandraPort,
		p.Conf.ClusterConnectionTimeoutMs,
		OriginCassandra,
		p.Conf.OriginDatacenter)
	if err != nil {
		return fmt.Errorf("error initializing the connection configuration or control connection for Origin: %w", err)
	}

	p.lock.Lock()
	p.originConnectionConfig = originConnectionConfig
	p.originContactPoints = originContactPoints
	p.lock.Unlock()

	// Initialize target connection configuration and control connection endpoint configuration
	targetConnectionConfig, targetContactPoints, err := initializeConnectionConfig(p.Conf.TargetCassandraSecureConnectBundlePath,
		parsedTargetContactPoints,
		p.Conf.TargetCassandraPort,
		p.Conf.ClusterConnectionTimeoutMs,
		TargetCassandra,
		p.Conf.TargetDatacenter)
	if err != nil {
		return fmt.Errorf("error initializing the connection configuration or control connection for Target: %w", err)
	}
	p.lock.Lock()
	p.targetConnectionConfig = targetConnectionConfig
	p.targetContactPoints = targetContactPoints
	p.lock.Unlock()

	firstContactPointIndex := p.proxyRand.Intn(len(p.originContactPoints))

	originControlConn := NewControlConn(
		p.shutdownContext, p.Conf.OriginCassandraPort, p.originConnectionConfig, p.originContactPoints,
		p.Conf.OriginCassandraUsername, p.Conf.OriginCassandraPassword, p.Conf, topologyConfig, p.proxyRand)

	if err := originControlConn.Start(p.shutdownWaitGroup, ctx, firstContactPointIndex); err != nil {
		return fmt.Errorf("failed to initialize origin control connection: %w", err)
	}

	p.lock.Lock()
	p.originControlConn = originControlConn
	p.lock.Unlock()

	targetControlConn := NewControlConn(
		p.shutdownContext, p.Conf.TargetCassandraPort, p.targetConnectionConfig, p.targetContactPoints,
		p.Conf.TargetCassandraUsername, p.Conf.TargetCassandraPassword, p.Conf, topologyConfig, p.proxyRand)

	firstContactPointIndex = p.proxyRand.Intn(len(p.targetContactPoints))
	if err := targetControlConn.Start(p.shutdownWaitGroup, ctx, firstContactPointIndex); err != nil {
		return fmt.Errorf("failed to initialize target control connection: %w", err)
	}

	p.lock.Lock()
	p.targetControlConn = targetControlConn
	p.lock.Unlock()

	return nil
}

func (p *CloudgateProxy) initializeMetricHandler() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	// This is the Prometheus-specific implementation of the MetricFactory object that will be provided to the global MetricHandler object
	// To switch to a different implementation, change the type instantiated here to another one that implements
	// metrics.MetricFactory.
	// You will also need to change the HTTP handler, see runner.go.

	var metricFactory metrics.MetricFactory
	if p.Conf.EnableMetrics {
		metricFactory = prommetrics.NewPrometheusMetricFactory(prometheus.DefaultRegisterer)
	} else {
		metricFactory = noopmetrics.NewNoopMetricFactory()
	}

	proxyMetrics, err := p.CreateProxyMetrics(metricFactory)
	if err != nil {
		return err
	}

	p.metricHandler = metrics.NewMetricHandler(
		metricFactory, p.originBuckets, p.targetBuckets, proxyMetrics, p.CreateOriginNodeMetrics, p.CreateTargetNodeMetrics)

	return nil
}

func (p *CloudgateProxy) initializeGlobalStructures() {
	p.lock = &sync.RWMutex{}
	p.listenerLock = &sync.Mutex{}
	p.listenerClosed = false
	p.proxyRand = NewThreadSafeRand()

	maxProcs := runtime.GOMAXPROCS(0)

	p.requestResponseNumWorkers = p.Conf.RequestResponseMaxWorkers
	if p.requestResponseNumWorkers == -1 {
		p.requestResponseNumWorkers = maxProcs * 4 // default
	} else if p.requestResponseNumWorkers <= 0 {
		log.Warnf("Invalid number of request / response workers %d, using GOMAXPROCS * 4 (%d).", p.requestResponseNumWorkers, maxProcs * 4)
		p.requestResponseNumWorkers = maxProcs * 4
	}
	log.Infof("Using %d request / response workers.", p.requestResponseNumWorkers)

	p.writeNumWorkers = p.Conf.WriteMaxWorkers
	if p.writeNumWorkers == -1 {
		p.writeNumWorkers = maxProcs * 4 // default
	} else if p.writeNumWorkers <= 0 {
		log.Warnf("Invalid number of write workers %d, using GOMAXPROCS * 4 (%d).", p.writeNumWorkers, maxProcs * 4)
		p.writeNumWorkers = maxProcs * 4
	}
	log.Infof("Using %d write workers.", p.writeNumWorkers)

	p.readNumWorkers = p.Conf.ReadMaxWorkers
	if p.readNumWorkers == -1 {
		p.readNumWorkers = maxProcs * 8 // default
	} else if p.readNumWorkers <= 0 {
		log.Warnf("Invalid number of read workers %d, using GOMAXPROCS * 8 (%d).", p.readNumWorkers, maxProcs * 8)
		p.readNumWorkers = maxProcs * 8
	}
	log.Infof("Using %d read workers.", p.readNumWorkers)

	p.listenerNumWorkers = p.Conf.ListenerMaxWorkers
	if p.listenerNumWorkers == -1 {
		p.listenerNumWorkers = maxProcs // default
	} else if p.listenerNumWorkers <= 0 {
		log.Warnf("Invalid number of cluster connector workers %d, using GOMAXPROCS (%d).", p.listenerNumWorkers, maxProcs)
		p.listenerNumWorkers = maxProcs
	}
	log.Infof("Using %d listener workers.", p.listenerNumWorkers)

	p.requestResponseScheduler = NewScheduler(p.requestResponseNumWorkers)
	p.writeScheduler = NewScheduler(p.writeNumWorkers)
	p.readScheduler = NewScheduler(p.readNumWorkers)
	p.listenerScheduler = NewScheduler(p.listenerNumWorkers)

	p.lock.Lock()

	p.requestLoopWaitGroup = &sync.WaitGroup{}
	p.shutdownRequestCtx, p.shutdownRequestCancelFn = context.WithCancel(context.Background())

	p.PreparedStatementCache = NewPreparedStatementCache()

	p.shutdownContext, p.cancelFunc = context.WithCancel(context.Background())
	p.shutdownWaitGroup = &sync.WaitGroup{}
	p.shutdownClientListenerChan = make(chan bool)

	var err error
	p.originBuckets, err = p.Conf.ParseOriginBuckets()
	if err != nil {
		log.Errorf("Failed to parse origin buckets, falling back to default buckets: %v", err)
	} else {
		log.Infof("Parsed Origin buckets: %v", p.originBuckets)
	}

	p.targetBuckets, err = p.Conf.ParseTargetBuckets()
	if err != nil {
		log.Errorf("Failed to parse target buckets, falling back to default buckets: %v", err)
	} else {
		log.Infof("Parsed Target buckets: %v", p.targetBuckets)
	}

	p.activeClients = 0
	p.lock.Unlock()
}

// acceptConnectionsFromClients creates a listener on the passed in port argument, and every connection
// that is received over that port instantiates a ClientHandler that then takes over managing that connection
func (p *CloudgateProxy) acceptConnectionsFromClients(address string, port int) error {
	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", address, port))
	if err != nil {
		return err
	}

	p.listenerLock.Lock()
	p.clientListener = l
	p.listenerLock.Unlock()

	p.shutdownWaitGroup.Add(1)

	go func() {
		defer p.shutdownWaitGroup.Done()
		defer func() {
			p.listenerLock.Lock()
			defer p.listenerLock.Unlock()
			if !p.listenerClosed {
				p.listenerClosed = true
				l.Close()
			}
		}()
		for {
			conn, err := l.Accept()
			if err != nil {
				p.listenerLock.Lock()
				listenerClosed := p.listenerClosed
				p.listenerLock.Unlock()

				if listenerClosed || p.shutdownContext.Err() != nil || p.shutdownRequestCtx.Err() != nil {
					log.Debugf("Shutting down client listener on port %d", port)
					return
				}

				log.Errorf("Error while listening for new connections: %v", err)
				continue
			}

			currentClients := atomic.LoadInt32(&p.activeClients)
			if int(currentClients) >= p.Conf.MaxClientsThreshold {
				log.Warnf(
					"Refusing client connection from %v because max clients threshold has been hit (%v).",
					conn.RemoteAddr(), p.Conf.MaxClientsThreshold)
				err = conn.Close()
				if err != nil {
					log.Warnf("Error closing client connection from %v: %v", conn.RemoteAddr(), err)
				}
				continue
			}

			atomic.AddInt32(&p.activeClients, 1)
			log.Infof("Accepted connection from %v", conn.RemoteAddr())

			p.listenerScheduler.Schedule(func() {
				p.handleNewConnection(conn)
			})
		}
	}()

	return nil
}

// handleNewConnection creates the client handler and connectors for the new client connection
func (p *CloudgateProxy) handleNewConnection(clientConn net.Conn) {

	errFunc := func(e error) {
		log.Errorf("Client Handler could not be created: %v", e)
		clientConn.Close()
		atomic.AddInt32(&p.activeClients, -1)
	}

	// there is a ClientHandler for each connection made by a client

	var originEndpoint Endpoint
	if p.Conf.OriginEnableHostAssignment {
		originHost, err := p.originControlConn.NextAssignedHost()
		if err != nil {
			errFunc(err)
			return
		}
		originEndpoint = p.originConnectionConfig.endpointFactory(originHost)
	} else {
		originEndpoint = p.originControlConn.GetCurrentContactPoint()
		if originEndpoint == nil {
			log.Warnf("Origin ControlConnection current endpoint is nil, " +
				"falling back to first origin contact point (%v) for client connection %v.",
				p.originContactPoints[0].String(), clientConn.RemoteAddr().String())
		}
	}

	var targetEndpoint Endpoint
	if p.Conf.TargetEnableHostAssignment {
		targetHost, err := p.targetControlConn.NextAssignedHost()
		if err != nil {
			errFunc(err)
			return
		}
		targetEndpoint = p.targetConnectionConfig.endpointFactory(targetHost)
	} else {
		targetEndpoint = p.targetControlConn.GetCurrentContactPoint()
		if targetEndpoint == nil {
			log.Warnf("Target ControlConnection current endpoint is nil, " +
				"falling back to first target contact point (%v) for client connection %v.",
				p.targetContactPoints[0].String(), clientConn.RemoteAddr().String())
		}
	}

	originCassandraConnInfo := NewClusterConnectionInfo(p.originConnectionConfig, originEndpoint, true)
	targetCassandraConnInfo := NewClusterConnectionInfo(p.targetConnectionConfig, targetEndpoint,false)
	clientHandler, err := NewClientHandler(
		clientConn,
		originCassandraConnInfo,
		targetCassandraConnInfo,
		p.originControlConn,
		p.targetControlConn,
		p.Conf,
		p.TopologyConfig,
		p.Conf.OriginCassandraUsername,
		p.Conf.OriginCassandraPassword,
		p.PreparedStatementCache,
		p.metricHandler,
		p.shutdownWaitGroup,
		p.shutdownContext,
		p.requestResponseScheduler,
		p.readScheduler,
		p.writeScheduler,
		p.requestResponseNumWorkers,
		p.shutdownRequestCtx,
		p.requestLoopWaitGroup)

	if err != nil {
		errFunc(err)
		return
	}

	log.Tracef("ClientHandler created")
	clientHandler.run(&p.activeClients)
}

func (p *CloudgateProxy) Shutdown() {
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

	log.Debug("Requesting shutdown of the request loop...")
	p.shutdownRequestCancelFn()

	log.Debug("Waiting until all in flight requests are done...")
	p.requestLoopWaitGroup.Wait()

	log.Debug("Requesting shutdown of the cluster connectors (which will trigger a complete shutdown)...")
	p.lock.Lock()
	p.cancelFunc()
	p.originControlConn = nil
	p.targetControlConn = nil
	p.lock.Unlock()

	log.Debug("Waiting until all loops are done...")
	p.shutdownWaitGroup.Wait()

	log.Debug("Shutting down the scheduler and metrics handler...")

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

func (p *CloudgateProxy) GetOriginControlConn() *ControlConn {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.originControlConn
}

func (p *CloudgateProxy) GetTargetControlConn() *ControlConn {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.targetControlConn
}

func Run(conf *config.Config, ctx context.Context) (*CloudgateProxy, error) {
	cp := NewCloudgateProxy(conf)

	err := cp.Start(ctx)
	if err != nil {
		log.Errorf("Couldn't start proxy: %v.", err)
		cp.Shutdown()
		return nil, err
	}

	return cp, nil
}

func RunWithRetries(conf *config.Config, ctx context.Context, b *backoff.Backoff) (*CloudgateProxy, error) {
	log.Info("Attempting to start the proxy...")
	for {
		cp, err := Run(conf, ctx)
		if cp != nil {
			return cp, nil
		}

		nextDuration := b.Duration()
		if !errors.Is(err, ShutdownErr) {
			log.Errorf("Couldn't start proxy, retrying in %v: %v.", nextDuration, err)
		}
		if !sleepWithContext(nextDuration, ctx) {
			log.Info("Cancellation detected. Aborting proxy startup...")
			return nil, ShutdownErr
		}
	}
}

// sleepWithContext returns false if context Done() returns
func sleepWithContext(d time.Duration, ctx context.Context) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *CloudgateProxy) CreateProxyMetrics(metricFactory metrics.MetricFactory) (*metrics.ProxyMetrics, error) {
	failedRequestsOrigin, err := metricFactory.GetOrCreateCounter(metrics.FailedRequestsOrigin)
	if err != nil {
		return nil, err
	}

	failedRequestsTarget, err := metricFactory.GetOrCreateCounter(metrics.FailedRequestsTarget)
	if err != nil {
		return nil, err
	}

	failedRequestsBothFailedOnOriginOnly, err := metricFactory.GetOrCreateCounter(metrics.FailedRequestsBothFailedOnOriginOnly)
	if err != nil {
		return nil, err
	}

	failedRequestsBothFailedOnTargetOnly, err := metricFactory.GetOrCreateCounter(metrics.FailedRequestsBothFailedOnTargetOnly)
	if err != nil {
		return nil, err
	}

	failedRequestsBoth, err := metricFactory.GetOrCreateCounter(metrics.FailedRequestsBoth)
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

	proxyRequestDurationOrigin, err := metricFactory.GetOrCreateHistogram(metrics.ProxyRequestDurationOrigin, p.originBuckets)
	if err != nil {
		return nil, err
	}

	proxyRequestDurationTarget, err := metricFactory.GetOrCreateHistogram(metrics.ProxyRequestDurationTarget, p.targetBuckets)
	if err != nil {
		return nil, err
	}

	proxyRequestDurationBoth, err := metricFactory.GetOrCreateHistogram(metrics.ProxyRequestDurationBoth, p.originBuckets)
	if err != nil {
		return nil, err
	}

	inflightRequestsOrigin, err := metricFactory.GetOrCreateGauge(metrics.InFlightRequestsOrigin)
	if err != nil {
		return nil, err
	}

	inflightRequestsTarget, err := metricFactory.GetOrCreateGauge(metrics.InFlightRequestsTarget)
	if err != nil {
		return nil, err
	}

	inflightRequestsBoth, err := metricFactory.GetOrCreateGauge(metrics.InFlightRequestsBoth)
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
		FailedRequestsOrigin:                 failedRequestsOrigin,
		FailedRequestsTarget:                 failedRequestsTarget,
		FailedRequestsBothFailedOnOriginOnly: failedRequestsBothFailedOnOriginOnly,
		FailedRequestsBothFailedOnTargetOnly: failedRequestsBothFailedOnTargetOnly,
		FailedRequestsBoth:                   failedRequestsBoth,
		PSCacheSize:                          psCacheSize,
		PSCacheMissCount:                     psCacheMissCount,
		ProxyRequestDurationOrigin:           proxyRequestDurationOrigin,
		ProxyRequestDurationTarget:           proxyRequestDurationTarget,
		ProxyRequestDurationBoth:             proxyRequestDurationBoth,
		InFlightRequestsOrigin:               inflightRequestsOrigin,
		InFlightRequestsTarget:               inflightRequestsTarget,
		InFlightRequestsBoth:                 inflightRequestsBoth,
		OpenClientConnections:                openClientConnections,
	}

	return proxyMetrics, nil
}

func (p *CloudgateProxy) CreateOriginNodeMetrics(
	metricFactory metrics.MetricFactory, originNodeDescription string, originBuckets []float64) (*metrics.OriginMetrics, error) {
	originClientTimeouts, err := metrics.CreateCounterNodeMetric(metricFactory, originNodeDescription, metrics.OriginClientTimeouts)
	if err != nil {
		return nil, err
	}

	originReadTimeouts, err := metrics.CreateCounterNodeMetric(metricFactory, originNodeDescription, metrics.OriginReadTimeouts)
	if err != nil {
		return nil, err
	}

	originWriteTimeouts, err := metrics.CreateCounterNodeMetric(metricFactory, originNodeDescription, metrics.OriginWriteTimeouts)
	if err != nil {
		return nil, err
	}

	originUnpreparedErrors, err := metrics.CreateCounterNodeMetric(metricFactory, originNodeDescription, metrics.OriginUnpreparedErrors)
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

	return &metrics.OriginMetrics{
		OriginClientTimeouts:   originClientTimeouts,
		OriginReadTimeouts:     originReadTimeouts,
		OriginWriteTimeouts:    originWriteTimeouts,
		OriginUnpreparedErrors: originUnpreparedErrors,
		OriginOtherErrors:      originOtherErrors,
		OriginRequestDuration:  originRequestDuration,
		OpenOriginConnections:  openOriginConnections,
	}, nil
}

func (p *CloudgateProxy) CreateTargetNodeMetrics(
	metricFactory metrics.MetricFactory, targetNodeDescription string, targetBuckets []float64) (*metrics.TargetMetrics, error) {
	targetClientTimeouts, err := metrics.CreateCounterNodeMetric(metricFactory, targetNodeDescription, metrics.TargetClientTimeouts)
	if err != nil {
		return nil, err
	}

	targetReadTimeouts, err := metrics.CreateCounterNodeMetric(metricFactory, targetNodeDescription, metrics.TargetReadTimeouts)
	if err != nil {
		return nil, err
	}

	targetWriteTimeouts, err := metrics.CreateCounterNodeMetric(metricFactory, targetNodeDescription, metrics.TargetWriteTimeouts)
	if err != nil {
		return nil, err
	}

	targetUnpreparedErrors, err := metrics.CreateCounterNodeMetric(metricFactory, targetNodeDescription, metrics.TargetUnpreparedErrors)
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

	return &metrics.TargetMetrics{
		TargetClientTimeouts:   targetClientTimeouts,
		TargetReadTimeouts:     targetReadTimeouts,
		TargetWriteTimeouts:    targetWriteTimeouts,
		TargetUnpreparedErrors: targetUnpreparedErrors,
		TargetOtherErrors:      targetOtherErrors,
		TargetRequestDuration:  targetRequestDuration,
		OpenTargetConnections:  openTargetConnections,
	}, nil
}