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
	requestChannel chan *frame.RawFrame
	// channel on which the ClientConnector listens for responses to send to the client
	responseChannel chan *frame.RawFrame
	// channel on which the ClientConnector listens for event messages to send to the client
	eventsChannel chan *frame.RawFrame

	metricsHandler metrics.IMetricsHandler // Global metricsHandler object

	waitGroup                 *sync.WaitGroup
	responseChannelsWaitGroup *sync.WaitGroup
	clientHandlerContext      context.Context
	clientHandlerCancelFunc   context.CancelFunc

	writeCoalescer *writeCoalescer
}

func NewClientConnector(
	connection net.Conn,
	eventsChannel chan *frame.RawFrame,
	conf *config.Config,
	metricsHandler metrics.IMetricsHandler,
	waitGroup *sync.WaitGroup,
	clientHandlerContext context.Context,
	clientHandlerCancelFunc context.CancelFunc) *ClientConnector {
	return &ClientConnector{
		connection:                connection,
		conf:                      conf,
		requestChannel:            make(chan *frame.RawFrame, conf.RequestQueueSizeFrames),
		responseChannel:           make(chan *frame.RawFrame, conf.ResponseQueueSizeFrames),
		eventsChannel:             eventsChannel,
		metricsHandler:            metricsHandler,
		waitGroup:                 waitGroup,
		responseChannelsWaitGroup: &sync.WaitGroup{},
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
	}
}

/**
 *	Starts two listening loops: one for receiving requests from the client, one for the responses that must be sent to the client
 */
func (cc *ClientConnector) run() {
	cc.listenForRequests()
	cc.listenForResponses()
	cc.listenForEvents()
	cc.writeCoalescer.RunWriteQueueLoop()
	cc.waitGroup.Add(1)
	go func() {
		defer cc.waitGroup.Done()
		cc.responseChannelsWaitGroup.Wait()
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

		log.Infof("Shutting down client connector request listener %v", connectionAddr)
	}()
}

// listens on responseChannel, dequeues any responses and sends them to the client
func (cc *ClientConnector) listenForResponses() {
	clientAddrStr := cc.connection.RemoteAddr().String()
	log.Tracef("listenForResponses for client %v", clientAddrStr)

	cc.waitGroup.Add(1)
	cc.responseChannelsWaitGroup.Add(1)
	go func() {
		defer cc.responseChannelsWaitGroup.Done()
		defer cc.waitGroup.Done()
		for {
			log.Tracef("Waiting for next response to dispatch to client %v", clientAddrStr)
			response, ok := <-cc.responseChannel
			if !ok {
				break
			}

			log.Debugf("Response received (%v), dispatching to client %v", response.Header, clientAddrStr)

			ok = cc.sendResponseToClient(cc.clientHandlerContext, response)
			if !ok {
				break
			}

			log.Tracef("Response with opcode %d dispatched to client %v", response.Header.OpCode, clientAddrStr)
		}
		log.Infof("Shutting down response forwarder to %v", clientAddrStr)
	}()
}

// listens on eventsChannel, dequeues any events and sends them to the client
func (cc *ClientConnector) listenForEvents() {
	log.Tracef("listenForEvents for client %v", cc.connection.RemoteAddr())

	cc.waitGroup.Add(1)
	cc.responseChannelsWaitGroup.Add(1)
	connectionAddr := cc.connection.RemoteAddr().String()
	go func() {
		defer cc.responseChannelsWaitGroup.Done()
		defer cc.waitGroup.Done()
		for {
			log.Tracef("Waiting for next event to dispatch to client %v", connectionAddr)
			event, ok := <-cc.eventsChannel
			if !ok {
				break
			}

			log.Debugf("Event received (%v), dispatching to client %v", event.Header, connectionAddr)
			ok = cc.sendResponseToClient(cc.clientHandlerContext, event)
			if !ok {
				break
			}
		}
		log.Infof("Shutting down event forwarder to %v", cc.connection.RemoteAddr())
	}()
}


func (cc *ClientConnector) sendResponseToClient(clientHandlerContext context.Context, frame *frame.RawFrame) bool {
	return cc.writeCoalescer.Enqueue(clientHandlerContext, frame)
}
