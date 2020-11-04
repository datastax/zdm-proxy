package cloudgateproxy

import (
	"context"
	"fmt"
	"github.com/jpillora/backoff"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"net"
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

	preparedStatementCache *PreparedStatementCache

	shutdownContext            context.Context
	cancelFunc                 context.CancelFunc
	shutdownWaitGroup          *sync.WaitGroup
	shutdownClientListenerChan chan bool

	// Global metricsHandler object. Created here and passed around to any other struct or function that needs it,
	// so all metricsHandler are incremented globally
	metricsHandler metrics.IMetricsHandler
}

// Start starts up the proxy and start listening for client connections.
func (p *CloudgateProxy) Start() error {
	log.Infof("Starting proxy...")
	p.initializeGlobalStructures()

	err := checkConnection(p.originCassandraIP, p.shutdownContext)
	if err != nil {
		return err
	}
	log.Debugf("connection check passed (to %v)", p.originCassandraIP)

	err = checkConnection(p.targetCassandraIP, p.shutdownContext)
	if err != nil {
		return err
	}
	log.Debugf("connection check passed (to %v)", p.targetCassandraIP)

	p.initializeMetricsHandler()

	err = p.acceptConnectionsFromClients(p.Conf.ProxyQueryAddress, p.Conf.ProxyQueryPort)
	if err != nil {
		return err
	}

	log.Infof("Proxy connected and ready to accept queries on port %d", p.Conf.ProxyQueryPort)
	return nil
}

func (p *CloudgateProxy) initializeMetricsHandler() {
	p.lock.Lock()
	defer p.lock.Unlock()

	// This is the Prometheus-specific implementation of the global metricsHandler object
	// To switch to a different implementation, change the type instantiated here to another one that implements metricsHandler.metricsHandler
	p.metricsHandler = metrics.NewPrometheusCloudgateProxyMetrics()
}

func (p *CloudgateProxy) initializeGlobalStructures() {
	p.lock = &sync.RWMutex{}
	p.listenerLock = &sync.Mutex{}
	p.listenerClosed = false

	p.lock.Lock()

	p.originCassandraIP = fmt.Sprintf("%s:%d", p.Conf.OriginCassandraHostname, p.Conf.OriginCassandraPort)
	p.targetCassandraIP = fmt.Sprintf("%s:%d", p.Conf.TargetCassandraHostname, p.Conf.TargetCassandraPort)

	p.preparedStatementCache = NewPreparedStatementCache()

	p.shutdownContext, p.cancelFunc = context.WithCancel(context.Background())
	p.shutdownWaitGroup = &sync.WaitGroup{}
	p.shutdownClientListenerChan = make(chan bool)

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
				select {
				case <-p.shutdownContext.Done():
					log.Debugf("Shutting down client listener on port %d", port)
					return
				default:
					log.Errorf("Error while listening for new connections: %v", err)
					continue
				}
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
		p.Conf.TargetCassandraUsername,
		p.Conf.TargetCassandraPassword,
		p.preparedStatementCache,
		p.metricsHandler,
		p.shutdownWaitGroup,
		p.shutdownContext)

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
	if p.metricsHandler != nil {
		p.metricsHandler.UnregisterAllMetrics()
	}
	p.cancelFunc()
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
	log.Info("Proxy shutdown.")
}

func Run(conf *config.Config) (*CloudgateProxy, error) {
	cp := &CloudgateProxy{
		Conf: conf,
	}

	err2 := cp.Start()
	if err2 != nil {
		cp.Shutdown()
		return nil, err2
	}

	return cp, nil
}

func RunWithRetries(conf *config.Config, ctx context.Context, b *backoff.Backoff) (*CloudgateProxy, error) {
	log.Info("Attempting to start the proxy...")
	for {
		cp, err := Run(conf)
		if cp != nil {
			return cp, nil
		}

		nextDuration := b.Duration()
		log.Errorf("Couldn't start proxy, retrying in %v: %v.", nextDuration, err)
		if !sleepWithContext(nextDuration, ctx) {
			log.Error("Cancellation detected. Aborting proxy startup...")
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
