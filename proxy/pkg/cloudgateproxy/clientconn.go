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
			"ClientConnector",
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

		log.Debugf("[ClientConnector] All in flight requests are done, requesting cluster connections of client handler %v to be terminated.", cc.connection.RemoteAddr())
		cc.clientHandlerCancelFunc()

		log.Infof("[ClientConnector] Shutting down client connection to %v", cc.connection.RemoteAddr())
		err := cc.connection.Close()
		if err != nil {
			log.Warnf("[ClientConnector] Error received while closing connection to %v: %v", cc.connection.RemoteAddr(), err)
		}

		log.Debugf("[ClientConnector] Waiting until request listener is done.")
		<- cc.clientConnectorRequestsDoneChan
		log.Debugf("[ClientConnector] Shutting down write coalescer.")
		cc.writeCoalescer.Close()

		atomic.AddInt32(activeClients, -1)
	}()
}

func (cc *ClientConnector) listenForRequests() {

	log.Tracef("listenForRequests for client %v", cc.connection.RemoteAddr())

	cc.clientHandlerWg.Add(1)
	go func() {
		defer cc.clientHandlerWg.Done()
		defer close(cc.clientConnectorRequestsDoneChan)

		wg := &sync.WaitGroup{}
		defer wg.Wait()
		defer log.Debugf("[ClientConnector] Shutting down request listener, waiting until request listener tasks are done...")

		lock := &sync.Mutex{}
		closed := false

		cc.clientHandlerWg.Add(1)
		go func() {
			defer cc.clientHandlerWg.Done()
			select {
			case <-cc.clientHandlerContext.Done():
			case <-cc.shutdownRequestCtx.Done():
				log.Debugf("[ClientConnector] Entering \"draining\" mode of request listener %v", cc.connection.RemoteAddr())
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
				log.Infof("Request received after a protocol error occured, forcibly closing the client connection.")
				cc.clientHandlerCancelFunc()
				break
			}

			if err != nil {
				protocolVersionErr := &frame.ProtocolVersionErr{}
				if errors.As(err, &protocolVersionErr) {
					var protocolErrMsg *message.ProtocolError
					if protocolVersionErr.Version == primitive.ProtocolVersion5 && !protocolVersionErr.UseBeta {
						protocolErrMsg = &message.ProtocolError{
							ErrorMessage: "Beta version of the protocol used (5/v5-beta), but USE_BETA flag is unset"}
					} else {
						protocolErrMsg = &message.ProtocolError{
							ErrorMessage: fmt.Sprintf("Invalid or unsupported protocol version (%d); " +
								"supported versions are (3/v3, 4/v4, 5/v5-beta))", protocolVersionErr.Version)}
					}
					protocolErrOccurred = true

					log.Infof("Protocol error detected while decoding a frame: %v. " +
						"Returning a protocol error to the client: %v.", protocolVersionErr, protocolErrMsg)

					var responseVersion primitive.ProtocolVersion
					if primitive.IsValidProtocolVersion(protocolVersionErr.Version) && !protocolVersionErr.Version.IsBeta() {
						responseVersion = protocolVersionErr.Version
					} else {
						responseVersion = primitive.ProtocolVersion4
					}

					response := frame.NewFrame(responseVersion, 0, protocolErrMsg)
					rawResponse, err := defaultCodec.ConvertToRawFrame(response)
					if err != nil {
						log.Errorf("Could not convert frame (%v) to raw frame: %v", response, err)
						cc.clientHandlerCancelFunc()
						break
					}

					cc.sendResponseToClient(rawResponse)
					continue
				} else {
					handleConnectionError(
						err, cc.clientHandlerContext, cc.clientHandlerCancelFunc, "ClientConnector", "reading", connectionAddr)
					break
				}
			}

			wg.Add(1)
			cc.readScheduler.Schedule(func() {
				defer wg.Done()
				log.Tracef("Received request on client connector: %v", f.Header)
				lock.Lock()
				if closed {
					lock.Unlock()
					msg := &message.Overloaded{
						ErrorMessage: "Shutting down, please retry on next host.",
					}
					response := frame.NewFrame(f.Header.Version, f.Header.StreamId, msg)
					rawResponse, err := defaultCodec.ConvertToRawFrame(response)
					if err != nil {
						log.Errorf("Could not convert frame (%v) to raw frame: %v", response, err)
					}
					cc.sendResponseToClient(rawResponse)
					return
				}
				cc.requestChannel <- f
				lock.Unlock()
				log.Tracef("Request sent to client connector's request channel: %v", f.Header)
			})
		}
	}()
}

func (cc *ClientConnector) sendResponseToClient(frame *frame.RawFrame) {
	cc.writeCoalescer.Enqueue(frame)
}
