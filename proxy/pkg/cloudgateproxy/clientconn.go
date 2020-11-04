package cloudgateproxy

import (
	"context"
	"errors"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/riptano/cloud-gate/proxy/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"io"
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

	// channel on which the ClientConnector sends requests as it receives them from the client
	requestChannel chan *frame.RawFrame
	// channel on which the ClientConnector listens for responses to send to the client
	responseChannel chan *frame.RawFrame
	// channel on which the ClientConnector listens for event messages to send to the client
	eventsChannel chan *frame.RawFrame

	lock           *sync.RWMutex           // TODO do we need a lock here?
	metricsHandler metrics.IMetricsHandler // Global metricsHandler object

	waitGroup                *sync.WaitGroup
	clientConnectorWaitGroup *sync.WaitGroup
	clientHandlerContext     context.Context
	clientHandlerCancelFunc  context.CancelFunc
}

func NewClientConnector(connection net.Conn,
	eventsChannel chan *frame.RawFrame,
	metricsHandler metrics.IMetricsHandler,
	waitGroup *sync.WaitGroup,
	clientHandlerContext context.Context,
	clientHandlerCancelFunc context.CancelFunc) *ClientConnector {
	return &ClientConnector{
		connection:               connection,
		requestChannel:           make(chan *frame.RawFrame),
		responseChannel:          make(chan *frame.RawFrame),
		eventsChannel:            eventsChannel,
		lock:                     &sync.RWMutex{},
		metricsHandler:           metricsHandler,
		waitGroup:                waitGroup,
		clientConnectorWaitGroup: &sync.WaitGroup{},
		clientHandlerContext:     clientHandlerContext,
		clientHandlerCancelFunc:  clientHandlerCancelFunc,
	}
}

/**
 *	Starts two listening loops: one for receiving requests from the client, one for the responses that must be sent to the client
 */
func (cc *ClientConnector) run() {
	cc.listenForRequests()
	cc.listenForResponses()
	cc.listenForEvents()
	go func() {
		cc.clientConnectorWaitGroup.Wait()
		log.Infof("Shutting down client connection to %v", cc.connection.RemoteAddr())
		err := cc.connection.Close()
		if err != nil {
			log.Warnf("error received while closing connection to %v: %v", cc.connection.RemoteAddr(), err)
		}
		cc.metricsHandler.DecrementCountByOne(metrics.OpenClientConnections)
	}()
}

func (cc *ClientConnector) listenForRequests() {

	log.Tracef("listenForRequests for client %v", cc.connection.RemoteAddr())

	cc.waitGroup.Add(1)

	go func() {
		defer cc.waitGroup.Done()
		defer close(cc.requestChannel)
		for {
			frame, err := readRawFrame(cc.connection, cc.clientHandlerContext)

			if err != nil {
				if errors.Is(err, ShutdownErr) {
					break
				}

				if errors.Is(err, io.EOF) {
					log.Infof("[ClientConnector] In listenForRequests: %v disconnected", cc.connection.RemoteAddr())
				} else {
					log.Errorf("[ClientConnector] In listenForRequests: error reading: %v", err)
				}

				cc.clientHandlerCancelFunc()
				break
			}

			log.Debugf("Received request on client connector: %v", frame.Header)
			select {
			case cc.requestChannel <- frame:
			case <-cc.clientHandlerContext.Done():
				break
			}

			log.Tracef("Request sent to client connector's request channel: %v", frame.Header)
		}
		log.Infof("Shutting down client connector request listener %v", cc.connection.RemoteAddr())
	}()
}

// listens on responseChannel, dequeues any responses and sends them to the client
func (cc *ClientConnector) listenForResponses() {
	clientAddrStr := cc.connection.RemoteAddr().String()
	log.Tracef("listenForResponses for client %v", clientAddrStr)

	cc.waitGroup.Add(1)
	cc.clientConnectorWaitGroup.Add(1)
	go func() {
		defer cc.clientConnectorWaitGroup.Done()
		defer cc.waitGroup.Done()
		for {
			log.Tracef("Waiting for next response to dispatch to client %v", clientAddrStr)
			response, ok := <-cc.responseChannel
			if !ok {
				break
			}

			log.Debugf("Response received (%v), dispatching to client %v", response.Header, clientAddrStr)

			err := writeRawFrame(cc.connection, cc.clientHandlerContext, response)
			log.Tracef("Response with opcode %d dispatched to client %v", response.Header.OpCode, clientAddrStr)
			if errors.Is(err, ShutdownErr) {
				break
			} else if err != nil {
				log.Errorf("Error writing response to client connection: %v", err)
				break
			}
		}
		log.Infof("Shutting down response forwarder to %v", clientAddrStr)
	}()
}

// listens on eventsChannel, dequeues any events and sends them to the client
func (cc *ClientConnector) listenForEvents() {
	log.Tracef("listenForEvents for client %v", cc.connection.RemoteAddr())

	cc.waitGroup.Add(1)
	cc.clientConnectorWaitGroup.Add(1)
	go func() {
		defer cc.clientConnectorWaitGroup.Done()
		defer cc.waitGroup.Done()
		for {
			log.Tracef("Waiting for next event to dispatch to client %v", cc.connection.RemoteAddr())
			event, ok := <-cc.eventsChannel
			if !ok {
				break
			}

			log.Debugf("Event received (%v), dispatching to client %v", event.Header, cc.connection.RemoteAddr())

			err := writeRawFrame(cc.connection, cc.clientHandlerContext, event)
			log.Tracef("Event with opcode %d dispatched to client %v", event.Header.OpCode, cc.connection.RemoteAddr())
			if errors.Is(err, ShutdownErr) {
				break
			} else if err != nil {
				log.Errorf("Error writing event to client connection: %v", err)
				break
			}
		}
		log.Infof("shutting down event forwarder to %v", cc.connection.RemoteAddr())
	}()
}
