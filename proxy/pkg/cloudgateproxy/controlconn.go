package cloudgateproxy

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/jpillora/backoff"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	log "github.com/sirupsen/logrus"
	"math"
	"net"
	"sync"
	"time"
)

type ControlConn struct {
	cqlConn               CqlConnection
	retryBackoffPolicy    *backoff.Backoff
	heartbeatPeriod       time.Duration
	context               context.Context
	endpoint              string
	port                  int
	username              string
	password              string
	counterLock           *sync.RWMutex
	consecutiveFailures   int
	OpenConnectionTimeout time.Duration
	cqlConnLock           *sync.Mutex
	genericTypeCodec      *GenericTypeCodec
	topologyLock          *sync.Mutex
	hosts                 []*Host
	clusterName           string
}

const ccProtocolVersion = primitive.ProtocolVersion3
const ccWriteTimeout = 5 * time.Second
const ccReadTimeout = 10 * time.Second

func NewControlConn(conn net.Conn, ctx context.Context, endpoint string, port int, username string, password string, conf *config.Config) *ControlConn {
	return &ControlConn{
		cqlConn: NewCqlConnection(conn, username, password, ccReadTimeout, ccWriteTimeout),
		retryBackoffPolicy: &backoff.Backoff{
			Factor: conf.HeartbeatRetryBackoffFactor,
			Jitter: true,
			Min:    time.Duration(conf.HeartbeatRetryIntervalMinMs) * time.Millisecond,
			Max:    time.Duration(conf.HeartbeatRetryIntervalMaxMs) * time.Millisecond,
		},
		heartbeatPeriod:       time.Duration(conf.HeartbeatIntervalMs) * time.Millisecond,
		context:               ctx,
		endpoint:              endpoint,
		port:                  port,
		username:              username,
		password:              password,
		counterLock:           &sync.RWMutex{},
		consecutiveFailures:   0,
		OpenConnectionTimeout: time.Duration(conf.ClusterConnectionTimeoutMs) * time.Millisecond,
		cqlConnLock:           &sync.Mutex{},
		genericTypeCodec:      NewDefaultGenericTypeCodec(ccProtocolVersion),
		topologyLock:          &sync.Mutex{},
		hosts:                 nil,
	}
}

func (cc *ControlConn) Start(wg *sync.WaitGroup, ctx context.Context) error {
	conn := cc.getConn()
	log.Infof("Opening control connection to %v.", cc.endpoint)
	newConn, err := cc.Open(conn, ctx)
	if err != nil {
		cc.setConn(conn, nil)
		return fmt.Errorf("failed to initialize control connection: %w", err)
	} else {
		cc.setConn(conn, newConn)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cc.Close()
		defer log.Infof("Shutting down control connection to %v,", cc.endpoint)
		for cc.context.Err() == nil {
			conn = cc.getConn()
			if conn == nil || !conn.IsInitialized() {
				log.Infof("Reopening control connection to %v.", cc.endpoint)
				newConn, err = cc.Open(conn, nil)
				if err != nil {
					cc.setConn(conn, nil)
					timeUntilRetry := cc.retryBackoffPolicy.Duration()
					log.Warnf("Failed to open control connection to %v, retrying in %v: %v", cc.endpoint, timeUntilRetry, err)
					cc.IncrementFailureCounter()
					sleepWithContext(timeUntilRetry, cc.context)
					continue
				} else {
					if !cc.setConn(conn, newConn) {
						log.Infof("Failed to set the new control connection, race condition?")
						timeUntilRetry := cc.retryBackoffPolicy.Duration()
						sleepWithContext(timeUntilRetry, cc.context)
						continue
					}
					log.Infof("Control connection (%v) opened successfully", cc.endpoint)
					cc.ResetFailureCounter()
					cc.retryBackoffPolicy.Reset()
				}
			}

			err := conn.SendHeartbeat(cc.context)
			action := success
			if err != nil {
				action = failure
			}

			if cc.context.Err() != nil {
				continue
			}

			switch action {
			case fatalFailure:
				log.Errorf("Closing control connection to %v and will NOT attempt to re-open it due to a fatal failure: %v", cc.endpoint, err)
				cc.Close()
				return
			case failure:
				log.Warnf("Heartbeat failed on %v. Closing and opening a new connection: %v.", conn, err)
				cc.IncrementFailureCounter()
				cc.Close()
			case success:
				logMsg := "Heartbeat successful on %v, waiting %v until next heartbeat."
				if cc.ReadFailureCounter() != 0 {
					log.Infof(logMsg, conn, cc.heartbeatPeriod)
					cc.ResetFailureCounter()
				} else {
					log.Debugf(logMsg, conn, cc.heartbeatPeriod)
				}
				sleepWithContext(cc.heartbeatPeriod, cc.context)
			}
		}
	}()
	return nil
}

type heartbeatResultAction int

const (
	failure      = heartbeatResultAction(2)
	success      = heartbeatResultAction(3)
	fatalFailure = heartbeatResultAction(4)
)

func (cc *ControlConn) IncrementFailureCounter() {
	cc.counterLock.Lock()
	defer cc.counterLock.Unlock()
	cc.consecutiveFailures++
	if cc.consecutiveFailures < 0 {
		cc.consecutiveFailures = math.MaxInt32
	}
}

func (cc *ControlConn) ResetFailureCounter() {
	cc.counterLock.Lock()
	defer cc.counterLock.Unlock()
	cc.consecutiveFailures = 0
}

func (cc *ControlConn) ReadFailureCounter() int {
	cc.counterLock.RLock()
	defer cc.counterLock.RUnlock()
	return cc.consecutiveFailures
}

func (cc *ControlConn) Open(conn CqlConnection, ctx context.Context) (CqlConnection, error) {
	if ctx == nil {
		ctx = cc.context
	}

	if conn == nil {
		dialer := net.Dialer{}
		openConnectionTimeoutCtx, _ := context.WithTimeout(ctx, cc.OpenConnectionTimeout)
		tcpConn, err := dialer.DialContext(openConnectionTimeoutCtx, "tcp", cc.endpoint)
		if err != nil {
			return nil, fmt.Errorf("failed to open connection to %v: %w", cc.endpoint, err)
		}
		conn = NewCqlConnection(tcpConn, cc.username, cc.password, ccReadTimeout, ccWriteTimeout)
	}

	err := conn.InitializeContext(ccProtocolVersion, 0, ctx)
	if err == nil {
		_, err = cc.RefreshHosts(conn)
	}

	if err != nil {
		log.Infof("Error while opening control connection, triggering shutdown of connection: %v", err)
		err2 := conn.Close()
		if err2 != nil {
			log.Warnf("Failed to close cql connection: %v", err2)
		}

		return nil, fmt.Errorf("cql connection initialization failure: %w", err)
	}

	return conn, nil
}

func (cc *ControlConn) Close() {
	cc.cqlConnLock.Lock()
	conn := cc.cqlConn
	cc.cqlConn = nil
	cc.cqlConnLock.Unlock()

	if conn != nil {
		err := conn.Close()
		if err != nil {
			log.Warn("Failed to close connection, re-opening a new one anyway (possible leaked connection).")
		}
	}
}

func (cc *ControlConn) GetAddr() string {
	return cc.endpoint
}

func (cc *ControlConn) RefreshHosts(conn CqlConnection) ([]*Host, error) {
	localQueryResult, err := conn.Query("SELECT * FROM system.local", cc.genericTypeCodec)
	if err != nil {
		return nil, fmt.Errorf("could not fetch information from system.local table: %w", err)
	}

	localHost, clusterName, err := ParseSystemLocalResult(localQueryResult, cc.port)
	if err != nil {
		return nil, err
	}

	peersQuery, err := conn.Query("SELECT * FROM system.peers", cc.genericTypeCodec)
	if err != nil {
		return nil, fmt.Errorf("could not fetch information from system.peers table: %w", err)
	}

	hosts := ParseSystemPeersResult(peersQuery, cc.port, false)

	hosts = append([]*Host{localHost}, hosts...)

	cc.topologyLock.Lock()
	cc.clusterName = clusterName
	cc.hosts = hosts
	cc.topologyLock.Unlock()

	return hosts, nil
}

func (cc *ControlConn) GetHosts() ([]*Host, error) {
	cc.topologyLock.Lock()
	defer cc.topologyLock.Unlock()

	if cc.hosts == nil {
		return nil, fmt.Errorf("could not get hosts because topology information has not been retrieved yet")
	}

	return cc.hosts, nil
}

func (cc *ControlConn) GetClusterName() string {
	cc.topologyLock.Lock()
	defer cc.topologyLock.Unlock()

	return cc.clusterName
}

func (cc *ControlConn) setConn(oldConn CqlConnection, newConn CqlConnection) bool {
	cc.cqlConnLock.Lock()
	defer cc.cqlConnLock.Unlock()
	if cc.cqlConn == oldConn {
		cc.cqlConn = newConn
		return true
	}

	return false
}

func (cc *ControlConn) getConn() CqlConnection {
	cc.cqlConnLock.Lock()
	conn := cc.cqlConn
	defer cc.cqlConnLock.Unlock()
	return conn
}