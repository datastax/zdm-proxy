package cloudgateproxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	log "github.com/sirupsen/logrus"
	"net"
	"sync"
	"sync/atomic"
)

const ClientConnectorLogPrefix = "CLIENT-CONNECTOR"

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

	clientHandlerWg         *sync.WaitGroup
	clientHandlerContext    context.Context
	clientHandlerCancelFunc context.CancelFunc

	writeCoalescer *writeCoalescer

	responsesDoneChan <-chan bool
	requestsDoneCtx   context.Context
	eventsDoneChan    <-chan bool

	clientConnectorRequestsDoneChan chan bool

	readScheduler *Scheduler

	shutdownRequestCtx context.Context
}

func NewClientConnector(
	connection net.Conn,
	conf *config.Config,
	localClientHandlerWg *sync.WaitGroup,
	requestsChan chan<- *frame.RawFrame,
	clientHandlerContext context.Context,
	clientHandlerCancelFunc context.CancelFunc,
	responsesDoneChan <-chan bool,
	requestsDoneCtx context.Context,
	eventsDoneChan <-chan bool,
	readScheduler *Scheduler,
	writeScheduler *Scheduler,
	shutdownRequestCtx context.Context) *ClientConnector {
	return &ClientConnector{
		connection:              connection,
		conf:                    conf,
		requestChannel:          requestsChan,
		clientHandlerWg:         localClientHandlerWg,
		clientHandlerContext:    clientHandlerContext,
		clientHandlerCancelFunc: clientHandlerCancelFunc,
		writeCoalescer: NewWriteCoalescer(
			conf,
			connection,
			localClientHandlerWg,
			clientHandlerContext,
			clientHandlerCancelFunc,
			ClientConnectorLogPrefix,
			false,
			false,
			writeScheduler),
		responsesDoneChan:               responsesDoneChan,
		requestsDoneCtx:                 requestsDoneCtx,
		eventsDoneChan:                  eventsDoneChan,
		clientConnectorRequestsDoneChan: make(chan bool, 1),
		readScheduler:                   readScheduler,
		shutdownRequestCtx:              shutdownRequestCtx,
	}
}

/**
 *	Starts two listening loops: one for receiving requests from the client, one for the responses that must be sent to the client
 */
func (cc *ClientConnector) run(activeClients *int32) {
	cc.listenForRequests()
	cc.writeCoalescer.RunWriteQueueLoop()
	cc.clientHandlerWg.Add(1)
	go func() {
		defer cc.clientHandlerWg.Done()
		<- cc.responsesDoneChan
		<- cc.requestsDoneCtx.Done()
		<- cc.eventsDoneChan

		log.Debugf("[%s] All in flight requests are done, requesting cluster connections of client handler %v " +
			"to be terminated.", ClientConnectorLogPrefix, cc.connection.RemoteAddr())
		cc.clientHandlerCancelFunc()

		log.Infof("[%s] Shutting down client connection to %v", ClientConnectorLogPrefix, cc.connection.RemoteAddr())
		err := cc.connection.Close()
		if err != nil {
			log.Warnf("[%s] Error received while closing connection to %v: %v", ClientConnectorLogPrefix, cc.connection.RemoteAddr(), err)
		}

		log.Debugf("[%s] Waiting until request listener is done.", ClientConnectorLogPrefix)
		<- cc.clientConnectorRequestsDoneChan
		log.Debugf("[%s] Shutting down write coalescer.", ClientConnectorLogPrefix)
		cc.writeCoalescer.Close()

		atomic.AddInt32(activeClients, -1)
	}()
}

func (cc *ClientConnector) listenForRequests() {

	log.Tracef("[%s] listenForRequests for client %v", ClientConnectorLogPrefix, cc.connection.RemoteAddr())

	cc.clientHandlerWg.Add(1)
	go func() {
		defer cc.clientHandlerWg.Done()
		defer close(cc.clientConnectorRequestsDoneChan)

		wg := &sync.WaitGroup{}
		defer wg.Wait()
		defer log.Debugf("[%s] Shutting down request listener, waiting until request listener tasks are done...", ClientConnectorLogPrefix)

		lock := &sync.Mutex{}
		closed := false

		cc.clientHandlerWg.Add(1)
		go func() {
			defer cc.clientHandlerWg.Done()
			select {
			case <-cc.clientHandlerContext.Done():
			case <-cc.shutdownRequestCtx.Done():
				log.Debugf("[%s] Entering \"draining\" mode of request listener %v", ClientConnectorLogPrefix, cc.connection.RemoteAddr())
			}

			lock.Lock()
			close(cc.requestChannel)
			closed = true
			lock.Unlock()
		}()

		bufferedReader := bufio.NewReaderSize(cc.connection, cc.conf.RequestWriteBufferSizeBytes)
		connectionAddr := cc.connection.RemoteAddr().String()
		protocolErrOccurred := false
		for cc.clientHandlerContext.Err() == nil {
			f, err := readRawFrame(bufferedReader, connectionAddr, cc.clientHandlerContext)

			if protocolErrOccurred {
				log.Infof("[%s] Request received after a protocol error occured, " +
					"forcibly closing the client connection.", ClientConnectorLogPrefix)
				cc.clientHandlerCancelFunc()
				break
			}

			if err != nil {
				protocolVersionErr := &frame.ProtocolVersionErr{}
				if errors.As(err, &protocolVersionErr) {
					protocolErrOccurred = true
					if !cc.handleUnsupportedProtocol(protocolVersionErr, protocolVersionErr.Version, protocolVersionErr.UseBeta) {
						break
					}
					continue
				} else {
					handleConnectionError(
						err, cc.clientHandlerContext, cc.clientHandlerCancelFunc, ClientConnectorLogPrefix, "reading", connectionAddr)
					break
				}
			}

			wg.Add(1)
			cc.readScheduler.Schedule(func() {
				defer wg.Done()
				log.Tracef("[%s] Received request on client connector: %v", ClientConnectorLogPrefix, f.Header)
				lock.Lock()
				if closed {
					lock.Unlock()
					msg := &message.Overloaded{
						ErrorMessage: "Shutting down, please retry on next host.",
					}
					response := frame.NewFrame(f.Header.Version, f.Header.StreamId, msg)
					rawResponse, err := defaultCodec.ConvertToRawFrame(response)
					if err != nil {
						log.Errorf("[%s] Could not convert frame (%v) to raw frame: %v", ClientConnectorLogPrefix, response, err)
					}
					cc.sendResponseToClient(rawResponse)
					return
				}
				cc.requestChannel <- f
				lock.Unlock()
				log.Tracef("[%s] Request sent to client connector's request channel: %v", ClientConnectorLogPrefix, f.Header)
			})
		}
	}()
}

// handleUnsupportedProtocol was necessary when the native protocol library did not support v5 but now the function is here just
// in case the library throws an error while decoding the version (maybe the client tries to use v1 or v6)
func (cc *ClientConnector) handleUnsupportedProtocol(err error, version primitive.ProtocolVersion, useBetaFlag bool) bool {
	var protocolErrMsg *message.ProtocolError
	if version.IsBeta() && !useBetaFlag {
		protocolErrMsg = &message.ProtocolError{
			ErrorMessage: fmt.Sprintf("Beta version of the protocol used (%d/v%d-beta), but USE_BETA flag is unset",
				version, version)}
	} else {
		protocolErrMsg = &message.ProtocolError{
			ErrorMessage: fmt.Sprintf("Invalid or unsupported protocol version (%d)", version)}
	}

	log.Infof("[%s] Protocol error detected while decoding a frame: %v. " +
		"Returning a protocol error to the client: %v.", ClientConnectorLogPrefix, err, protocolErrMsg)

	// ideally we would use the maximum version between the versions used by both control connections if
	// control connections implemented protocol version negotiation
	response := frame.NewFrame(primitive.ProtocolVersion4, 0, protocolErrMsg)
	rawResponse, err := defaultCodec.ConvertToRawFrame(response)
	if err != nil {
		log.Errorf("[%s] Could not convert frame (%v) to raw frame: %v", ClientConnectorLogPrefix, response, err)
		cc.clientHandlerCancelFunc()
		return false
	}

	cc.sendResponseToClient(rawResponse)
	return true
}

func (cc *ClientConnector) sendResponseToClient(frame *frame.RawFrame) {
	cc.writeCoalescer.Enqueue(frame)
}
