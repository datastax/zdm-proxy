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
	"time"
)

const (
	// TODO: Make these configurable
	maxQueryRetries = 5
	queryTimeout    = 10 * time.Second

	cassMaxLen = 256 * 1024 * 1024 // 268435456 // 256 MB, per spec		// TODO is this an actual limit??
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

	numWorkers int
	scheduler  *Scheduler
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

	p.initializeControlConnections(originConn, targetConn)
	p.initializeMetricsHandler()

	err = p.acceptConnectionsFromClients(p.Conf.ProxyQueryAddress, p.Conf.ProxyQueryPort)
	if err != nil {
		return err
	}

	log.Infof("Proxy connected and ready to accept queries on port %d", p.Conf.ProxyQueryPort)
	return nil
}

func (p *CloudgateProxy) initializeControlConnections(origin net.Conn, target net.Conn) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.originControlConn =
		NewControlConn(
			origin, p.shutdownContext, p.originCassandraIP, p.Conf.OriginCassandraUsername, p.Conf.OriginCassandraPassword, p.Conf)
	p.originControlConn.Start(p.shutdownWaitGroup)
	p.targetControlConn =
		NewControlConn(
			target, p.shutdownContext, p.targetCassandraIP, p.Conf.TargetCassandraUsername, p.Conf.TargetCassandraPassword, p.Conf)
	p.targetControlConn.Start(p.shutdownWaitGroup)
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

	m.AddGauge(metrics.OpenClientConnections)

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
	p.numWorkers = p.Conf.MaxWorkers
	maxProcs := runtime.GOMAXPROCS(0)
	if p.numWorkers == -1 {
		p.numWorkers = maxProcs // default
	} else if p.numWorkers <= 0 {
		log.Warn("Invalid number of workers %d, using GOMAXPROCS (%d).", p.numWorkers, maxProcs)
		p.numWorkers = maxProcs
	}
	log.Infof("Using %d workers.", p.numWorkers)
	p.scheduler = NewScheduler(p.numWorkers)

	p.lock.Lock()

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
				if p.shutdownContext.Err() != nil {
					log.Debugf("Shutting down client listener on port %d", port)
					return
				}

				log.Errorf("Error while listening for new connections: %v", err)
				continue
			}

			go p.handleNewConnection(conn)
		}
	}()

	return nil
}

// handleNewConnection creates the client handler and connectors for the new client connection
func (p *CloudgateProxy) handleNewConnection(conn net.Conn) {
	p.metricsHandler.IncrementCountByOne(metrics.OpenClientConnections)
	log.Infof("Accepted connection from %v", conn.RemoteAddr())

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
		p.scheduler,
		p.numWorkers)

	if err != nil {
		log.Errorf("Client Handler could not be created: %v", err)
		conn.Close()
		p.metricsHandler.DecrementCountByOne(metrics.OpenClientConnections)
		return
	}

	log.Tracef("ClientHandler created")
	clientHandler.run()
}

func (p *CloudgateProxy) Shutdown() {
	log.Info("Shutting down proxy...")

	var shutdownWaitGroup *sync.WaitGroup

	p.lock.Lock()
	p.cancelFunc()
	p.originControlConn = nil
	p.targetControlConn = nil
	shutdownWaitGroup = p.shutdownWaitGroup
	p.lock.Unlock()

	p.listenerLock.Lock()
	if !p.listenerClosed {
		p.listenerClosed = true
		if p.clientListener != nil {
			p.clientListener.Close()
		}
	}
	p.listenerLock.Unlock()

	shutdownWaitGroup.Wait()

	p.lock.Lock()
	p.scheduler.Shutdown()
	if p.metricsHandler != nil {
		err := p.metricsHandler.UnregisterAllMetrics()
		if err != nil {
			log.Warnf("Failed to unregister metrics: %v.", err)
		}
	}
	p.lock.Unlock()

	log.Info("Proxy shutdown.")
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
