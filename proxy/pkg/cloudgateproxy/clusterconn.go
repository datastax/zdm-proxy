package cloudgateproxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"sync"
	"time"
)

type ClusterConnectionInfo struct {
	ipAddress         string
	port              int
	isOriginCassandra bool
}

type ClusterType string

const (
	OriginCassandra = ClusterType("originCassandra")
	TargetCassandra = ClusterType("targetCassandra")
)

type ClusterConnector struct {
	connection              net.Conn
	conf                    *config.Config
	clusterType             ClusterType

	clusterConnEventsChan   chan *frame.RawFrame
	metricsHandler          metrics.IMetricsHandler
	waitGroup               *sync.WaitGroup
	clientHandlerContext    context.Context
	clientHandlerCancelFunc context.CancelFunc
	responseChan            chan<- *Response

	writeCoalescer          *writeCoalescer
	doneChan                chan bool

	readScheduler *Scheduler
}

func NewClusterConnectionInfo(ipAddress string, port int, isOriginCassandra bool) *ClusterConnectionInfo {
	return &ClusterConnectionInfo{
		ipAddress:         ipAddress,
		port:              port,
		isOriginCassandra: isOriginCassandra,
	}
}

func NewClusterConnector(
	connInfo *ClusterConnectionInfo,
	conf *config.Config,
	metricsHandler metrics.IMetricsHandler,
	waitGroup *sync.WaitGroup,
	clientHandlerContext context.Context,
	clientHandlerCancelFunc context.CancelFunc,
	responseChan chan<- *Response,
	readScheduler *Scheduler,
	writeScheduler *Scheduler) (*ClusterConnector, error) {

	var clusterType ClusterType
	if connInfo.isOriginCassandra {
		clusterType = OriginCassandra
	} else {
		clusterType = TargetCassandra
	}

	connectionOpenTimeout := time.Duration(conf.ClusterConnectionTimeoutMs)*time.Millisecond
	timeoutCtx, _ := context.WithTimeout(clientHandlerContext, connectionOpenTimeout)
	conn, err := openConnectionToCluster(connInfo, timeoutCtx, metricsHandler)
	if err != nil {
		if errors.Is(err, ShutdownErr) {
			if timeoutCtx.Err() != nil {
				return nil, fmt.Errorf("context timed out or cancelled while opening connection to %v: %w", clusterType, timeoutCtx.Err())
			}
		}
		return nil, fmt.Errorf("could not open connection to %v: %w", clusterType, err)
	}

	go func() {
		<-clientHandlerContext.Done()
		closeConnectionToCluster(conn, clusterType, metricsHandler)
	}()

	return &ClusterConnector{
		connection:              conn,
		conf:                    conf,
		clusterType:             clusterType,
		clusterConnEventsChan:   make(chan *frame.RawFrame, conf.EventQueueSizeFrames),
		metricsHandler:          metricsHandler,
		waitGroup:               waitGroup,
		clientHandlerContext:    clientHandlerContext,
		clientHandlerCancelFunc: clientHandlerCancelFunc,
		writeCoalescer: NewWriteCoalescer(
			conf,
			conn,
			metricsHandler,
			waitGroup,
			clientHandlerContext,
			clientHandlerCancelFunc,
			"ClusterConnector",
			true,
			writeScheduler),
		responseChan:  responseChan,
		doneChan:      make(chan bool),
		readScheduler: readScheduler,
	}, nil
}

func (cc *ClusterConnector) run() {
	cc.runResponseListeningLoop()
	cc.writeCoalescer.RunWriteQueueLoop()
}

func openConnectionToCluster(connInfo *ClusterConnectionInfo, context context.Context, metricsHandler metrics.IMetricsHandler) (net.Conn, error) {
	conn, err := establishConnection(connInfo.getConnectionString(), context)
	if err != nil {
		return nil, err
	}

	if connInfo.isOriginCassandra {
		metricsHandler.IncrementCountByOne(metrics.OpenOriginConnections)
	} else {
		metricsHandler.IncrementCountByOne(metrics.OpenTargetConnections)
	}
	return conn, nil
}

func closeConnectionToCluster(conn net.Conn, clusterType ClusterType, metricsHandler metrics.IMetricsHandler) {
	log.Infof("[ClusterConnector] Closing connection to %v (%v)", clusterType, conn.RemoteAddr())
	err := conn.Close()
	if err != nil {
		log.Warnf("[ClusterConnector] Error closing connection to %v (%v)", clusterType, conn.RemoteAddr())
	}

	if clusterType == OriginCassandra {
		metricsHandler.DecrementCountByOne(metrics.OpenOriginConnections)
	} else {
		metricsHandler.DecrementCountByOne(metrics.OpenTargetConnections)
	}

}

/**
 *	Starts a long-running loop that listens for replies being sent by the cluster
 */
func (cc *ClusterConnector) runResponseListeningLoop() {

	cc.waitGroup.Add(1)
	log.Debugf("Listening to replies sent by node %v", cc.connection.RemoteAddr())
	go func() {
		defer cc.waitGroup.Done()
		defer close(cc.clusterConnEventsChan)
		defer close(cc.doneChan)

		bufferedReader := bufio.NewReaderSize(cc.connection, cc.conf.ResponseReadBufferSizeBytes)
		connectionAddr := cc.connection.RemoteAddr().String()
		wg := &sync.WaitGroup{}
		defer wg.Wait()
		for {
			response, err := readRawFrame(bufferedReader, connectionAddr, cc.clientHandlerContext)
			if err != nil {
				handleConnectionError(
					err, cc.clientHandlerCancelFunc, fmt.Sprintf("ClusterConnector %v", cc.clusterType), "reading", connectionAddr)
				break
			}

			wg.Add(1)
			cc.readScheduler.Schedule(func() {
				defer wg.Done()
				log.Debugf("Received response from %v (%v): %v",
					cc.clusterType, connectionAddr, response.Header)

				if response.Header.OpCode == primitive.OpCodeEvent {
					cc.clusterConnEventsChan <- response
				} else {
					cc.responseChan <- NewResponse(response, cc.clusterType)
				}
				log.Tracef("Response sent to response channel: %v", response.Header)
			})
		}
		log.Debugf("Shutting down response listening loop from %v", connectionAddr)
	}()
}

func (cc *ClusterConnector) sendRequestToCluster(frame *frame.RawFrame) {
	cc.writeCoalescer.Enqueue(frame)
}

func (cci *ClusterConnectionInfo) getConnectionString() string {
	return fmt.Sprintf("%s:%d", cci.ipAddress, cci.port)
}

// Checks if the error was due to a shutdown request, triggering the cancellation function if it was not.
// Also logs the error appropriately.
func handleConnectionError(err error, cancelFn context.CancelFunc, logPrefix string, operation string, connectionAddr string) {
	if errors.Is(err, ShutdownErr) {
		return
	}

	if errors.Is(err, io.EOF) || IsClosingErr(err) {
		log.Infof("[%v] %v disconnected", logPrefix, connectionAddr)
	} else {
		log.Errorf("[%v] error %v: %v", logPrefix, operation, err)
	}

	cancelFn()
}