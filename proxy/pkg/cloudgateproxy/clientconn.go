package cloudgateproxy

import (
	"bufio"
	"context"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"net"
	"sync"
)

/*
  This owns:
    - a response channel to send responses back to the client
    - the actual TCP connection
*/

type ClientConnector struct {

	// connection to the client
	connection net.Conn

	// configuration object of the proxy
	conf *config.Config

	// channel on which the ClientConnector sends requests as it receives them from the client
	requestChannel chan<- *frame.RawFrame

	metricsHandler metrics.IMetricsHandler // Global metricsHandler object

	waitGroup                 *sync.WaitGroup
	clientHandlerContext      context.Context
	clientHandlerCancelFunc   context.CancelFunc

	writeCoalescer *writeCoalescer

	responsesDoneChan <-chan bool
	eventsDoneChan    <-chan bool
}

func NewClientConnector(
	connection net.Conn,
	conf *config.Config,
	metricsHandler metrics.IMetricsHandler,
	waitGroup *sync.WaitGroup,
	requestsChan chan<- *frame.RawFrame,
	clientHandlerContext context.Context,
	clientHandlerCancelFunc context.CancelFunc,
	responsesDoneChan <-chan bool,
	eventsDoneChan <-chan bool) *ClientConnector {
	return &ClientConnector{
		connection:                connection,
		conf:                      conf,
		requestChannel:            requestsChan,
		metricsHandler:            metricsHandler,
		waitGroup:                 waitGroup,
		clientHandlerContext:      clientHandlerContext,
		clientHandlerCancelFunc:   clientHandlerCancelFunc,
		writeCoalescer:            NewWriteCoalescer(
			conf,
			connection,
			metricsHandler,
			waitGroup,
			clientHandlerContext,
			clientHandlerCancelFunc,
			"ClientConnector"),
		responsesDoneChan:         responsesDoneChan,
		eventsDoneChan:            eventsDoneChan,
	}
}

/**
 *	Starts two listening loops: one for receiving requests from the client, one for the responses that must be sent to the client
 */
func (cc *ClientConnector) run() {
	cc.listenForRequests()
	cc.writeCoalescer.RunWriteQueueLoop()
	cc.waitGroup.Add(1)
	go func() {
		defer cc.waitGroup.Done()
		<- cc.responsesDoneChan
		<- cc.eventsDoneChan
		cc.writeCoalescer.Close()

		log.Infof("[ClientConnector] Shutting down connection to %v", cc.connection.RemoteAddr())
		err := cc.connection.Close()
		if err != nil {
			log.Warnf("[ClientConnector] Error received while closing connection to %v: %v", cc.connection.RemoteAddr(), err)
		}
		cc.metricsHandler.DecrementCountByOne(metrics.OpenClientConnections)
	}()
}

func (cc *ClientConnector) listenForRequests() {

	log.Tracef("listenForRequests for client %v", cc.connection.RemoteAddr())

	cc.waitGroup.Add(1)

	go func() {
		lock := &sync.Mutex{}
		closed := false

		go func() {
			<-cc.clientHandlerContext.Done()
			lock.Lock()
			close(cc.requestChannel)
			closed = true
			lock.Unlock()
		}()

		defer cc.waitGroup.Done()

		bufferedReader := bufio.NewReaderSize(cc.connection, cc.conf.ReadBufferSizeBytes)
		connectionAddr := cc.connection.RemoteAddr().String()
		for cc.clientHandlerContext.Err() == nil {
			f, err := readRawFrame(bufferedReader, connectionAddr, cc.clientHandlerContext)
			if err != nil {
				handleConnectionError(
					err, cc.clientHandlerCancelFunc, "ClientConnector", "reading", connectionAddr)
				break
			}

			log.Debugf("Received request on client connector: %v", f.Header)

			lock.Lock()
			if closed {
				lock.Unlock()
				break
			}
			cc.requestChannel <- f
			lock.Unlock()

			log.Tracef("Request sent to client connector's request channel: %v", f.Header)
		}

		log.Debugf("Shutting down client connector request listener %v", connectionAddr)
	}()
}

func (cc *ClientConnector) sendResponseToClient(frame *frame.RawFrame) {
	cc.writeCoalescer.Enqueue(frame)
}
