package cloudgateproxy

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
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
	addr                  string
	username              string
	password              string
	counterLock           *sync.RWMutex
	consecutiveFailures   int
	OpenConnectionTimeout time.Duration
}

const ccProtocolVersion = primitive.ProtocolVersion3
const ccWriteTimeout = 5 * time.Second
const ccReadTimeout = 10 * time.Second

func NewControlConn(conn net.Conn, ctx context.Context, addr string, username string, password string, conf *config.Config) *ControlConn {
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
		addr:                  addr,
		username:              username,
		password:              password,
		counterLock:           &sync.RWMutex{},
		consecutiveFailures:   0,
		OpenConnectionTimeout: time.Duration(conf.ClusterConnectionTimeoutMs) * time.Millisecond,
	}
}

func (cc *ControlConn) Start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cc.Close()
		defer log.Infof("Shutting down control connection to %v,", cc.addr)
		for cc.context.Err() == nil {
			if cc.cqlConn == nil || !cc.cqlConn.IsInitialized() {
				log.Infof("Opening control connection to %v.", cc.addr)
				err := cc.Open()
				if err != nil {
					timeUntilRetry := cc.retryBackoffPolicy.Duration()
					log.Warnf("Failed to open control connection to %v, retrying in %v: %v", cc.addr, timeUntilRetry, err)
					cc.IncrementFailureCounter()
					sleepWithContext(timeUntilRetry, cc.context)
					continue
				} else {
					log.Infof("Control connection (%v) opened successfully", cc.addr)
					cc.ResetFailureCounter()
					cc.retryBackoffPolicy.Reset()
				}
			}

			err, action := cc.sendHeartbeat()

			if cc.context.Err() != nil {
				continue
			}

			switch action {
			case fatalFailure:
				log.Errorf("Closing control connection to %v and will NOT attempt to re-open it due to a fatal failure: %v", cc.addr, err)
				cc.Close()
				return
			case failure:
				log.Warnf("Heartbeat failed on %v. Closing and opening a new connection: %v.", cc.cqlConn, err)
				cc.IncrementFailureCounter()
				cc.Close()
			case success:
				log.Infof("Heartbeat successful on %v, waiting %v until next heartbeat.", cc.cqlConn, cc.heartbeatPeriod)
				cc.ResetFailureCounter()
				sleepWithContext(cc.heartbeatPeriod, cc.context)
			}
		}
	}()
}

type heartbeatResultAction int

const (
	failure         = heartbeatResultAction(2)
	success         = heartbeatResultAction(3)
	fatalFailure    = heartbeatResultAction(4)
)

func (cc *ControlConn) sendHeartbeat() (error, heartbeatResultAction) {

	optionsMsg := &message.Options{}
	heartBeatFrame, err :=
		frame.NewRequestFrame(
			ccProtocolVersion, 1, false, nil, optionsMsg, false)
	if err != nil {
		return fmt.Errorf("unexpected failure trying to build OPTIONS frame: %w", err), fatalFailure
	}

	err = cc.cqlConn.SendContext(heartBeatFrame, cc.context)
	if err != nil {
		action := failure
		return fmt.Errorf("failed to send heartbeat: %v", err), action
	}

	response, err := cc.cqlConn.ReceiveContext(cc.context)

	if err != nil {
		action := failure
		return fmt.Errorf("failed to receive heartbeat response: %v", err), action
	}

	_, ok := response.Body.Message.(*message.Supported)
	if !ok {
		log.Warnf("Expected SUPPORTED but got %v. Considering this a successful heartbeat regardless.", response.Body.Message)
	}

	return nil, success
}

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

func (cc *ControlConn) Open() error {
	if cc.cqlConn == nil {
		dialer := net.Dialer{}
		openConnectionTimeoutCtx, _ := context.WithTimeout(cc.context, cc.OpenConnectionTimeout)
		conn, err := dialer.DialContext(openConnectionTimeoutCtx, "tcp", cc.addr)
		if err != nil {
			return fmt.Errorf("failed to open connection to %v: %w", cc.addr, err)
		}
		cc.cqlConn = NewCqlConnection(conn, cc.username, cc.password, ccReadTimeout, ccWriteTimeout)
	}

	err := cc.cqlConn.InitializeContext(ccProtocolVersion, 0, cc.context)
	if err != nil {
		err2 := cc.cqlConn.Close()
		if err2 != nil {
			log.Warnf("Failed to close cql connection: %v", err2)
		}

		cc.cqlConn = nil
		return fmt.Errorf("cql connection initialization failure: %w", err)
	}

	return nil
}

func (cc *ControlConn) Close() {
	if cc.cqlConn != nil {
		err := cc.cqlConn.Close()
		if err != nil {
			log.Warn("Failed to close connection, re-opening a new one anyway (possible leaked connection).")
		}
	}
	cc.cqlConn = nil
}

func (cc *ControlConn) GetAddr() string {
	return cc.addr
}
