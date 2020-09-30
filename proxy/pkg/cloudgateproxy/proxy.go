package cloudgateproxy

import (
	"context"
	"fmt"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// TODO: Make these configurable
	maxQueryRetries = 5
	queryTimeout    = 10 * time.Second

	cassHdrLen = 9
	cassMaxLen = 256 * 1024 * 1024 // 268435456 // 256 MB, per spec		// TODO is this an actual limit??
)

type CloudgateProxy struct {

	Conf *config.Config

	originCassandraIP string
	targetCassandraIP string

	lock *sync.RWMutex

	// Listener that enables the proxy to listen for clients on the port specified in the configuration
	clientListener net.Listener
	listenerLock *sync.Mutex
	listenerClosed bool

	preparedStatementCache *PreparedStatementCache

	shutdown bool	// TODO can this go?

	shutdownContext            context.Context
	cancelFunc                 context.CancelFunc
	shutdownWaitGroup          *sync.WaitGroup
	shutdownClientListenerChan chan bool

	// Created here and passed around to any other struct or function that needs it, so all metrics are incremented globally
	Metrics *metrics.Metrics
}


// Start starts up the proxy and start listening for client connections.
func (p *CloudgateProxy) Start() error {
	p.initializeGlobalStructures()
	err := checkConnection(p.originCassandraIP, p.shutdownContext)
	if err != nil {
		return err
	}

	log.Debugf("connection check passed (to %s)", p.originCassandraIP)

	err = checkConnection(p.targetCassandraIP, p.shutdownContext)
	if err != nil {
		return err
	}

	log.Debugf("connection check passed (to %s)", p.targetCassandraIP)

	err = p.acceptConnectionsFromClients(p.Conf.ProxyQueryPort)
	if err != nil {
		return err
	}

	log.Infof("Proxy connected and ready to accept queries on port %d", p.Conf.ProxyQueryPort)
	return nil
}

func (p *CloudgateProxy) initializeGlobalStructures() {
	p.lock = &sync.RWMutex{}
	p.listenerLock = &sync.Mutex{}
	p.listenerClosed = false

	p.lock.Lock()

	p.originCassandraIP = fmt.Sprintf("%s:%d", p.Conf.OriginCassandraHostname, p.Conf.OriginCassandraPort)
	p.targetCassandraIP = fmt.Sprintf("%s:%d", p.Conf.TargetCassandraHostname, p.Conf.TargetCassandraPort)

	p.preparedStatementCache = NewPreparedStatementCache()

	p.shutdown = false

	p.shutdownContext, p.cancelFunc = context.WithCancel(context.Background())
	p.shutdownWaitGroup = &sync.WaitGroup{}
	p.shutdownClientListenerChan = make(chan bool)

	p.Metrics = metrics.New(p.Conf.ProxyMetricsPort)
	p.Metrics.Expose(p.shutdownContext)

	p.lock.Unlock()
}

// acceptConnectionsFromClients creates a listener on the passed in port argument, and every connection
// that is received over that port instantiates a ClientHandler that then takes over managing that connection
func (p *CloudgateProxy) acceptConnectionsFromClients(port int) error {
	log.Debugf("Proxy connected and ready to accept queries on port %d", port)
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	p.listenerLock.Lock()
	p.clientListener = l
	p.listenerLock.Unlock()

	p.shutdownWaitGroup.Add(1)

	go func() {
		defer p.shutdownWaitGroup.Done()
		defer func(){
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
					log.Error(err)
					continue
				}
			}

			go func() {
				<-p.shutdownContext.Done()
				err := conn.Close()
				if err != nil {
					log.Warnf("error received while closing connection to %s", conn.RemoteAddr().String())
				}
			}()

			// there is a ClientHandler for each connection made by a client
			originCassandraConnInfo := NewClusterConnectionInfo(p.Conf.OriginCassandraHostname, p.Conf.OriginCassandraPort, true,
																p.Conf.OriginCassandraUsername, p.Conf.OriginCassandraPassword)
			targetCassandraConnInfo := NewClusterConnectionInfo(p.Conf.TargetCassandraHostname, p.Conf.TargetCassandraPort, false,
																p.Conf.TargetCassandraUsername, p.Conf.TargetCassandraPassword)
			clientHandler, err := NewClientHandler(
				conn,
				originCassandraConnInfo,
				targetCassandraConnInfo,
				p.preparedStatementCache,
				p.Metrics,
				p.shutdownWaitGroup,
				p.shutdownContext)

			if err != nil {
				log.Errorf("Client Handler could not be created. Error %s", err)
				conn.Close()
				continue
			}
			// TODO if we want to keep the ClientHandler instances into an array or map, store it here
			log.Debugf("ClientHandler created")
			clientHandler.run()
		}
	}()

	return nil
}

func Run(conf *config.Config) *CloudgateProxy {
	cp := &CloudgateProxy{
		Conf: conf,
	}

	err2 := cp.Start()
	if err2 != nil {
		// TODO: handle error
		log.Error(err2)
		panic(err2)
	}

	return cp
}

func (p *CloudgateProxy) Shutdown() {
	p.cancelFunc()
	p.listenerLock.Lock()
	if !p.listenerClosed {
		p.listenerClosed = true
		p.clientListener.Close()
	}
	p.listenerLock.Unlock()
	p.shutdownWaitGroup.Wait()
}