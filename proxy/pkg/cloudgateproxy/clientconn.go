package cloudgateproxy

import (
	"bufio"
	"context"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
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

	waitGroup                 *sync.WaitGroup
	clientHandlerContext      context.Context
	clientHandlerCancelFunc   context.CancelFunc

	writeCoalescer *writeCoalescer

	responsesDoneChan <-chan bool
	requestsDoneChan <-chan bool
	eventsDoneChan    <-chan bool

	clientConnectorRequestsDoneChan chan bool

	readScheduler *Scheduler

	shutdownRequestCtx context.Context
}

func NewClientConnector(
	connection net.Conn,
	conf *config.Config,
	waitGroup *sync.WaitGroup,
	requestsChan chan<- *frame.RawFrame,
	clientHandlerContext context.Context,
	clientHandlerCancelFunc context.CancelFunc,
	responsesDoneChan <-chan bool,
	requestsDoneChan <-chan bool,
	eventsDoneChan <-chan bool,
	readScheduler *Scheduler,
	writeScheduler *Scheduler,
	shutdownRequestCtx context.Context) *ClientConnector {
	return &ClientConnector{
		connection:              connection,
		conf:                    conf,
		requestChannel:          requestsChan,
		waitGroup:               waitGroup,
		clientHandlerContext:    clientHandlerContext,
		clientHandlerCancelFunc: clientHandlerCancelFunc,
		writeCoalescer: NewWriteCoalescer(
			conf,
			connection,
			waitGroup,
			clientHandlerContext,
			clientHandlerCancelFunc,
			"ClientConnector",
			false,
			writeScheduler),
		responsesDoneChan:               responsesDoneChan,
		requestsDoneChan:                requestsDoneChan,
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
	cc.waitGroup.Add(1)
	go func() {
		defer cc.waitGroup.Done()
		<- cc.responsesDoneChan
		<- cc.requestsDoneChan
		<- cc.eventsDoneChan

		log.Debugf("[ClientConnector] Requesting shutdown of request listener %v", cc.connection.RemoteAddr())
		cc.clientHandlerCancelFunc()

		log.Infof("[ClientConnector] Shutting down connection to %v", cc.connection.RemoteAddr())
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

	cc.waitGroup.Add(1)

	go func() {
		lock := &sync.Mutex{}
		closed := false

		go func() {
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

		defer cc.waitGroup.Done()
		defer close(cc.clientConnectorRequestsDoneChan)

		bufferedReader := bufio.NewReaderSize(cc.connection, cc.conf.RequestWriteBufferSizeBytes)
		connectionAddr := cc.connection.RemoteAddr().String()
		wg := &sync.WaitGroup{}
		defer wg.Wait()
		defer log.Debugf("[ClientConnector] Shutting down request listener, waiting until request listener tasks are done...")
		for cc.clientHandlerContext.Err() == nil {
			f, err := readRawFrame(bufferedReader, connectionAddr, cc.clientHandlerContext)
			if err != nil {
				handleConnectionError(
					err, cc.clientHandlerCancelFunc, "ClientConnector", "reading", connectionAddr)
				break
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
