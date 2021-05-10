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
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// TODO: Make these configurable
	maxQueryRetries = 5

	cassMaxLen = 256 * 1024 * 1024 // 268435456 // 256 MB, per spec		// TODO is this an actual limit??

	listenerDefaultWorkers = 20
)

type CloudgateProxy struct {
	Conf *config.Config

	originCassandraIP string
	targetCassandraIP string

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

	// Global metricsHandler object. Created here and passed around to any other struct or function that needs it,
	// so all metricsHandler are incremented globally
	metricsHandler metrics.IMetricsHandler

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
}

func NewCloudgateProxy(conf *config.Config) *CloudgateProxy {
	cp := &CloudgateProxy{
		Conf: conf,
	}
	cp.initializeGlobalStructures()
	return cp
}

func (p *CloudgateProxy) GetMetricsHandler() metrics.IMetricsHandler {
	return p.metricsHandler
}

// Start starts up the proxy and start listening for client connections.
func (p *CloudgateProxy) Start(ctx context.Context) error {
	log.Infof("Starting proxy...")

	timeout := time.Duration(p.Conf.ClusterConnectionTimeoutMs) * time.Millisecond
	openConnectionTimeoutCtx, _ := context.WithTimeout(ctx, timeout)
	originConn, err := openConnection(p.originCassandraIP, openConnectionTimeoutCtx)
	if err != nil {
		return err
	}
	log.Debugf("origin connection check passed (to %v)", p.originCassandraIP)

	openConnectionTimeoutCtx, _ = context.WithTimeout(ctx, timeout)
	targetConn, err := openConnection(p.targetCassandraIP, openConnectionTimeoutCtx)
	if err != nil {
		return err
	}
	log.Debugf("target connection check passed (to %v)", p.targetCassandraIP)

	initializeControlConnectionTimeoutCtx, _ := context.WithTimeout(ctx, timeout)
	err = p.initializeControlConnections(originConn, targetConn, initializeControlConnectionTimeoutCtx)
	if err != nil {
		return err
	}

	originHosts, err := p.originControlConn.GetHosts()
	if err != nil {
		return fmt.Errorf("failed to initialize proxy, could not get origin hosts: %w", err)
	}

	log.Infof("Initialized origin control connection. Cluster Name: %v, Hosts: %v",
		p.originControlConn.GetClusterName(), originHosts)

	targetHosts, err := p.targetControlConn.GetHosts()
	if err != nil {
		return fmt.Errorf("failed to initialize proxy, could not get target hosts: %w", err)
	}

	log.Infof("Initialized target control connection. Cluster Name: %v, Hosts: %v",
		p.targetControlConn.GetClusterName(), targetHosts)

	p.initializeMetricsHandler()

	err = p.acceptConnectionsFromClients(p.Conf.ProxyQueryAddress, p.Conf.ProxyQueryPort)
	if err != nil {
		return err
	}

	log.Infof("Proxy connected and ready to accept queries on port %d", p.Conf.ProxyQueryPort)
	return nil
}

func (p *CloudgateProxy) initializeControlConnections(origin net.Conn, target net.Conn, ctx context.Context) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.originControlConn =
		NewControlConn(
			origin, p.shutdownContext, p.originCassandraIP, p.Conf.OriginCassandraPort, p.Conf.OriginCassandraUsername, p.Conf.OriginCassandraPassword, p.Conf)
	if err := p.originControlConn.Start(p.shutdownWaitGroup, ctx); err != nil {
		return fmt.Errorf("failed to initialize origin control connection: %w", err)
	}

	p.targetControlConn =
		NewControlConn(
			target, p.shutdownContext, p.targetCassandraIP, p.Conf.TargetCassandraPort, p.Conf.TargetCassandraUsername, p.Conf.TargetCassandraPassword, p.Conf)
	if err := p.targetControlConn.Start(p.shutdownWaitGroup, ctx); err != nil {
		return fmt.Errorf("failed to initialize target control connection: %w", err)
	}

	return nil
}

func (p *CloudgateProxy) initializeMetricsHandler() {
	p.lock.Lock()
	defer p.lock.Unlock()

	// This is the Prometheus-specific implementation of the global IMetricsHandler object
	// To switch to a different implementation, change the type instantiated here to another one that implements
	// metrics.IMetricsHandler.
	// You will also need to change the HTTP handler, see runner.go.

	if p.Conf.EnableMetrics {
		p.metricsHandler = prommetrics.NewPrometheusCloudgateProxyMetrics(prometheus.DefaultRegisterer)
	} else {
		p.metricsHandler = noopmetrics.NewNoopMetricsHandler()
	}

	m := p.metricsHandler

	// proxy level metrics

	m.AddCounter(metrics.FailedRequestsBoth)
	m.AddCounter(metrics.FailedRequestsBothFailedOnOriginOnly)
	m.AddCounter(metrics.FailedRequestsBothFailedOnTargetOnly)
	m.AddCounter(metrics.FailedRequestsOrigin)
	m.AddCounter(metrics.FailedRequestsTarget)

	m.AddCounter(metrics.PSCacheMissCount)
	m.AddGaugeFunction(metrics.PSCacheSize, p.PreparedStatementCache.GetPreparedStatementCacheSize)

	m.AddHistogram(metrics.ProxyRequestDurationBoth, p.originBuckets)
	m.AddHistogram(metrics.ProxyRequestDurationOrigin, p.originBuckets)
	m.AddHistogram(metrics.ProxyRequestDurationTarget, p.targetBuckets)

	m.AddGauge(metrics.InFlightRequestsBoth)
	m.AddGauge(metrics.InFlightRequestsOrigin)
	m.AddGauge(metrics.InFlightRequestsTarget)

	m.AddGaugeFunction(metrics.OpenClientConnections, func() float64 {
		return float64(atomic.LoadInt32(&p.activeClients))
	})

	// cluster level metrics

	m.AddHistogram(metrics.OriginRequestDuration, p.originBuckets)
	m.AddHistogram(metrics.TargetRequestDuration, p.targetBuckets)

	m.AddCounter(metrics.OriginClientTimeouts)
	m.AddCounter(metrics.OriginReadTimeouts)
	m.AddCounter(metrics.OriginWriteTimeouts)
	m.AddCounter(metrics.OriginUnpreparedErrors)
	m.AddCounter(metrics.OriginOtherErrors)

	m.AddCounter(metrics.TargetClientTimeouts)
	m.AddCounter(metrics.TargetReadTimeouts)
	m.AddCounter(metrics.TargetWriteTimeouts)
	m.AddCounter(metrics.TargetUnpreparedErrors)
	m.AddCounter(metrics.TargetOtherErrors)

	m.AddGauge(metrics.OpenOriginConnections)
	m.AddGauge(metrics.OpenTargetConnections)
}

func (p *CloudgateProxy) initializeGlobalStructures() {
	p.lock = &sync.RWMutex{}
	p.listenerLock = &sync.Mutex{}
	p.listenerClosed = false

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

	p.originCassandraIP = fmt.Sprintf("%s:%d", p.Conf.OriginCassandraHostname, p.Conf.OriginCassandraPort)
	p.targetCassandraIP = fmt.Sprintf("%s:%d", p.Conf.TargetCassandraHostname, p.Conf.TargetCassandraPort)

	p.PreparedStatementCache = NewPreparedStatementCache()

	p.shutdownContext, p.cancelFunc = context.WithCancel(context.Background())
	p.shutdownWaitGroup = &sync.WaitGroup{}
	p.shutdownClientListenerChan = make(chan bool)

	var err error
	p.originBuckets, err = p.Conf.ParseOriginBuckets()
	if err != nil {
		log.Errorf("Failed to parse origin buckets, falling back to default buckets: %v", err)
	}

	p.targetBuckets, err = p.Conf.ParseTargetBuckets()
	if err != nil {
		log.Errorf("Failed to parse target buckets, falling back to default buckets: %v", err)
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
				if p.shutdownContext.Err() != nil || p.shutdownRequestCtx.Err() != nil {
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
func (p *CloudgateProxy) handleNewConnection(conn net.Conn) {

	// there is a ClientHandler for each connection made by a client
	originCassandraConnInfo := NewClusterConnectionInfo(p.Conf.OriginCassandraHostname, p.Conf.OriginCassandraPort, true)
	targetCassandraConnInfo := NewClusterConnectionInfo(p.Conf.TargetCassandraHostname, p.Conf.TargetCassandraPort, false)
	clientHandler, err := NewClientHandler(
		conn,
		originCassandraConnInfo,
		targetCassandraConnInfo,
		p.Conf,
		p.Conf.OriginCassandraUsername,
		p.Conf.OriginCassandraPassword,
		p.PreparedStatementCache,
		p.metricsHandler,
		p.shutdownWaitGroup,
		p.shutdownContext,
		p.requestResponseScheduler,
		p.readScheduler,
		p.writeScheduler,
		p.requestResponseNumWorkers,
		p.shutdownRequestCtx,
		p.requestLoopWaitGroup)

	if err != nil {
		log.Errorf("Client Handler could not be created: %v", err)
		conn.Close()
		atomic.AddInt32(&p.activeClients, -1)
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
	if p.metricsHandler != nil {
		err := p.metricsHandler.UnregisterAllMetrics()
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
