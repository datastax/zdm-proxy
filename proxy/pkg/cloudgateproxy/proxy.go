package cloudgateproxy

import (
	"fmt"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"net"
	"sync"
	"time"
	"github.com/go-kit/kit/metrics"
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

	// Channel signalling that the proxy is now ready to process queries
	ReadyChan chan struct{}		// TODO is this still needed?

	// Channel to signal to coordinator that there are no more open connections to the Client's Database
	// and that the coordinator can redirect Envoy to point directly to TargetCassandra without any negative side effects
	//TODO this will probably go in the end but it is here to make the main method work for the moment
	ReadyForRedirect chan struct{}

	// Listener that enables the proxy to listen for clients on the port specified in the configuration
	clientListener net.Listener

	preparedStatementCache *PreparedStatementCache

	shutdown bool	// TODO can this go?

	// Created here and passed around to any other struct or function that needs it, so all metrics are incremented globally
	

}


// Start starts up the proxy and start listening for client connections.
func (p *CloudgateProxy) Start() error {
	p.initializeGlobalStructures()
	checkConnection(p.originCassandraIP)
	checkConnection(p.targetCassandraIP)

	err := p.acceptConnectionsFromClients(p.Conf.ProxyQueryPort)
	if err != nil {
		return err
	}

	log.Infof("Proxy connected and ready to accept queries on port %d", p.Conf.ProxyQueryPort)
	return nil
}

func (p *CloudgateProxy) initializeGlobalStructures() {
	p.lock = &sync.RWMutex{}

	p.lock.Lock()

	p.originCassandraIP = fmt.Sprintf("%s:%d", p.Conf.OriginCassandraHostname, p.Conf.OriginCassandraPort)
	p.targetCassandraIP = fmt.Sprintf("%s:%d", p.Conf.TargetCassandraHostname, p.Conf.TargetCassandraPort)

	p.ReadyChan = make(chan struct{})

	p.preparedStatementCache = NewPreparedStatementCache()

	p.shutdown = false
	p.Metrics = metrics.New(p.Conf.ProxyMetricsPort)
	p.Metrics.Expose()

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

	p.lock.Lock()
	p.clientListener = l
	p.lock.Unlock()

	go func() {
		defer l.Close()
		for {
			conn, err := l.Accept()
			if err != nil {
				if p.shutdown {
					log.Debugf("Shutting down client listener on port %d", port)
					return
				}
				log.Error(err)
				continue
			}
			// there is a ClientHandler for each connection made by a client
			originCassandraConnInfo := NewClusterConnectionInfo(p.Conf.OriginCassandraHostname, p.Conf.OriginCassandraPort, true,
																p.Conf.OriginCassandraUsername, p.Conf.OriginCassandraPassword)
			targetCassandraConnInfo := NewClusterConnectionInfo(p.Conf.TargetCassandraHostname, p.Conf.TargetCassandraPort, false,
																p.Conf.TargetCassandraUsername, p.Conf.TargetCassandraPassword)
			clientHandler := NewClientHandler(conn, originCassandraConnInfo, targetCassandraConnInfo, p.preparedStatementCache, p.Metrics)
//			if err != nil {
//				log.Errorf("Client Handler could not be created. Error %s", err)
				// TODO error handling!
			//}
			// TODO if we want to keep the ClientHandler instances into an array or map, store it here
			log.Debugf("ClientHandler created")
			go clientHandler.run()
		}
	}()

	return nil
}
