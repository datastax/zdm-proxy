package zdmproxy

import (
	"bufio"
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	log "github.com/sirupsen/logrus"
	"net"
	"sync"
	"sync/atomic"
)

const ClientConnectorLogPrefix = "CLIENT-CONNECTOR"

const (
	ProtocolErrorDecodeError int8 = iota
	ProtocolErrorUnsupportedVersion
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

	// not used atm but should be used when a protocol error occurs after #68 has been addressed
	clientHandlerShutdownRequestCancelFn context.CancelFunc

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
	shutdownRequestCtx context.Context,
	clientHandlerShutdownRequestCancelFn context.CancelFunc) *ClientConnector {

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
		responsesDoneChan:                    responsesDoneChan,
		requestsDoneCtx:                      requestsDoneCtx,
		eventsDoneChan:                       eventsDoneChan,
		clientConnectorRequestsDoneChan:      make(chan bool, 1),
		readScheduler:                        readScheduler,
		shutdownRequestCtx:                   shutdownRequestCtx,
		clientHandlerShutdownRequestCancelFn: clientHandlerShutdownRequestCancelFn,
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
		<-cc.responsesDoneChan
		<-cc.requestsDoneCtx.Done()
		<-cc.eventsDoneChan

		log.Debugf("[%s] All in flight requests are done, requesting cluster connections of client handler %v "+
			"to be terminated.", ClientConnectorLogPrefix, cc.connection.RemoteAddr())
		cc.clientHandlerCancelFunc()

		log.Infof("[%s] Shutting down client connection to %v", ClientConnectorLogPrefix, cc.connection.RemoteAddr())
		err := cc.connection.Close()
		if err != nil {
			log.Warnf("[%s] Error received while closing connection to %v: %v", ClientConnectorLogPrefix, cc.connection.RemoteAddr(), err)
		}

		log.Debugf("[%s] Waiting until request listener is done.", ClientConnectorLogPrefix)
		<-cc.clientConnectorRequestsDoneChan
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

		lock := &sync.RWMutex{}
		closed := false

		setDrainModeNowFunc := func() {
			lock.Lock()
			if !closed {
				close(cc.requestChannel)
				closed = true
			}
			lock.Unlock()
		}

		cc.clientHandlerWg.Add(1)
		go func() {
			defer cc.clientHandlerWg.Done()
			select {
			case <-cc.clientHandlerContext.Done():
			case <-cc.shutdownRequestCtx.Done():
				log.Debugf("[%s] Entering \"draining\" mode of request listener %v", ClientConnectorLogPrefix, cc.connection.RemoteAddr())
			}

			setDrainModeNowFunc()
		}()

		bufferedReader := bufio.NewReaderSize(cc.connection, cc.conf.RequestWriteBufferSizeBytes)
		connectionAddr := cc.connection.RemoteAddr().String()
		protocolErrOccurred := false
		var alreadySentProtocolErr *frame.RawFrame
		for cc.clientHandlerContext.Err() == nil {
			f, err := readRawFrame(bufferedReader, connectionAddr, cc.clientHandlerContext)

			protocolErrResponseFrame, err, _ := checkProtocolError(f, err, protocolErrOccurred, ClientConnectorLogPrefix)
			if err != nil {
				handleConnectionError(
					err, cc.clientHandlerContext, cc.clientHandlerCancelFunc, ClientConnectorLogPrefix, "reading", connectionAddr)
				break
			} else if protocolErrResponseFrame != nil {
				alreadySentProtocolErr = protocolErrResponseFrame
				protocolErrOccurred = true
				cc.sendResponseToClient(protocolErrResponseFrame)
				continue
			} else if alreadySentProtocolErr != nil {
				clonedProtocolErr := alreadySentProtocolErr.Clone()
				clonedProtocolErr.Header.StreamId = f.Header.StreamId
				cc.sendResponseToClient(clonedProtocolErr)
				continue
			}

			wg.Add(1)
			cc.readScheduler.Schedule(func() {
				defer wg.Done()
				log.Tracef("[%s] Received request on client connector: %v", ClientConnectorLogPrefix, f.Header)
				lock.RLock()
				if closed {
					lock.RUnlock()
					cc.sendOverloadedToClient(f)
					return
				}
				cc.requestChannel <- f
				lock.RUnlock()
				log.Tracef("[%s] Request sent to client connector's request channel: %v", ClientConnectorLogPrefix, f.Header)
			})
		}
	}()
}

func (cc *ClientConnector) sendOverloadedToClient(request *frame.RawFrame) {
	msg := &message.Overloaded{
		ErrorMessage: "Shutting down, please retry on next host.",
	}
	response := frame.NewFrame(request.Header.Version, request.Header.StreamId, msg)
	rawResponse, err := defaultCodec.ConvertToRawFrame(response)
	if err != nil {
		log.Errorf("[%s] Could not convert frame (%v) to raw frame: %v", ClientConnectorLogPrefix, response, err)
	} else {
		cc.sendResponseToClient(rawResponse)
	}
}

func checkProtocolError(f *frame.RawFrame, connErr error, protocolErrorOccurred bool, prefix string) (protocolErrResponse *frame.RawFrame, fatalErr error, errorCode int8) {
	var protocolErrMsg *message.ProtocolError
	var streamId int16
	var logMsg string
	if connErr != nil {
		protocolErrMsg = checkUnsupportedProtocolError(connErr)
		logMsg = fmt.Sprintf("Protocol error detected while decoding a frame: %v.", connErr)
		streamId = 0
		errorCode = ProtocolErrorDecodeError
	} else {
		protocolErrMsg = checkProtocolVersion(f.Header.Version)
		logMsg = "Protocol v5 detected while decoding a frame."
		streamId = f.Header.StreamId
		errorCode = ProtocolErrorUnsupportedVersion
	}

	if protocolErrMsg != nil {
		if !protocolErrorOccurred {
			log.Debugf("[%v] %v Returning a protocol error to the client to force a downgrade: %v.", prefix, logMsg, protocolErrMsg)
		}
		rawProtocolErrResponse, err := generateProtocolErrorResponseFrame(streamId, protocolErrMsg)
		if err != nil {
			return nil, fmt.Errorf("could not generate protocol error response raw frame (%v): %v", protocolErrMsg, err), -1
		} else {
			return rawProtocolErrResponse, nil, errorCode
		}
	} else {
		return nil, connErr, -1
	}
}

func generateProtocolErrorResponseFrame(streamId int16, protocolErrMsg *message.ProtocolError) (*frame.RawFrame, error) {
	// ideally we would use the maximum version between the versions used by both control connections if
	// control connections implemented protocol version negotiation
	response := frame.NewFrame(primitive.ProtocolVersion4, streamId, protocolErrMsg)
	rawResponse, err := defaultCodec.ConvertToRawFrame(response)
	if err != nil {
		return nil, err
	}

	return rawResponse, nil
}

func (cc *ClientConnector) sendResponseToClient(frame *frame.RawFrame) {
	cc.writeCoalescer.Enqueue(frame)
}
