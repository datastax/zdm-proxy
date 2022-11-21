package zdmproxy

import (
	"context"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	numberOfStreamIds = int16(2048)
	eventQueueLength  = 2048

	maxIncomingPending = 2048
	maxOutgoingPending = 2048

	timeOutsThreshold = 1024
)

type CqlConnection interface {
	IsInitialized() bool
	InitializeContext(version primitive.ProtocolVersion, ctx context.Context) error
	SendAndReceive(request *frame.Frame, ctx context.Context) (*frame.Frame, error)
	Close() error
	Execute(msg message.Message, ctx context.Context) (message.Message, error)
	Query(cql string, genericTypeCodec *GenericTypeCodec, version primitive.ProtocolVersion, ctx context.Context) (*ParsedRowSet, error)
	SendHeartbeat(ctx context.Context) error
	SetEventHandler(eventHandler func(f *frame.Frame, conn CqlConnection))
	SubscribeToProtocolEvents(ctx context.Context, eventTypes []primitive.EventType) error
	IsAuthEnabled() (bool, error)
}

// Not thread safe
type cqlConn struct {
	readTimeout           time.Duration
	writeTimeout          time.Duration
	conn                  net.Conn
	credentials           *AuthCredentials
	initialized           bool
	cancelFn              context.CancelFunc
	ctx                   context.Context
	wg                    *sync.WaitGroup
	outgoingCh            chan *frame.Frame
	eventsQueue           chan *frame.Frame
	pendingOperations     map[int16]chan *frame.Frame
	pendingOperationsLock *sync.RWMutex
	timedOutOperations    int
	closed                bool
	eventHandler          func(f *frame.Frame, conn CqlConnection)
	eventHandlerLock      *sync.Mutex
	authEnabled           bool
	frameProcessor        InternalCqlFrameProcessor
}

var (
	StreamIdMismatchErr = errors.New("stream id of the response is different from the stream id of the request")
)

func (c *cqlConn) String() string {
	return fmt.Sprintf("cqlConn{conn: %v}", c.conn.RemoteAddr().String())
}

func NewCqlConnection(
	conn net.Conn,
	username string, password string,
	readTimeout time.Duration, writeTimeout time.Duration) CqlConnection {
	ctx, cFn := context.WithCancel(context.Background())
	cqlConn := &cqlConn{
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
		conn:         conn,
		credentials: &AuthCredentials{
			Username: username,
			Password: password,
		},
		initialized:           false,
		ctx:                   ctx,
		cancelFn:              cFn,
		wg:                    &sync.WaitGroup{},
		outgoingCh:            make(chan *frame.Frame, maxOutgoingPending),
		eventsQueue:           make(chan *frame.Frame, eventQueueLength),
		pendingOperations:     make(map[int16]chan *frame.Frame),
		pendingOperationsLock: &sync.RWMutex{},
		timedOutOperations:    0,
		closed:                false,
		eventHandlerLock:      &sync.Mutex{},
		authEnabled:           true,
		frameProcessor:        NewInternalCqlStreamIdProcessor("cqlconn"),
	}
	cqlConn.StartRequestLoop()
	cqlConn.StartResponseLoop()
	cqlConn.StartEventLoop()
	return cqlConn
}

func (c *cqlConn) SetEventHandler(eventHandler func(f *frame.Frame, conn CqlConnection)) {
	c.eventHandlerLock.Lock()
	defer c.eventHandlerLock.Unlock()
	c.eventHandler = eventHandler
}

func (c *cqlConn) SubscribeToProtocolEvents(ctx context.Context, eventTypes []primitive.EventType) error {
	registerMsg := &message.Register{EventTypes: eventTypes}
	responseMsg, err := c.Execute(registerMsg, ctx)
	if err != nil {
		return fmt.Errorf("could not register event handler: %w", err)
	}

	if _, ok := responseMsg.(*message.Ready); !ok {
		return fmt.Errorf("expected Ready response when subscribing to server events but got: %v", responseMsg)
	}

	return nil
}

func (c *cqlConn) StartResponseLoop() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer close(c.eventsQueue)
		defer log.Debugf("Shutting down response loop on %v.", c)
		for c.ctx.Err() == nil {
			f, err := defaultCodec.DecodeFrame(c.conn)
			if err != nil {
				if (!errors.Is(err, io.EOF) && !IsClosingErr(err)) || c.ctx.Err() == nil {
					log.Errorf("Failed to read/decode frame on cql connection %v: %v", c, err)
				}
				c.cancelFn()
				break
			}

			if f.Body.Message.GetOpCode() == primitive.OpCodeEvent {
				select {
				case c.eventsQueue <- f:
				default:
					log.Warnf("[CqlConnection] events queue is full, blocking response loop until event queue is not full...")
					select {
					case c.eventsQueue <- f:
					case <-c.ctx.Done():
					}
				}
				continue
			}

			c.pendingOperationsLock.Lock()
			respChan, ok := c.pendingOperations[f.Header.StreamId]
			if !ok {
				log.Warnf("[CqlConnection] could not find response channel for streamid %d, skipping", f.Header.StreamId)
				c.pendingOperationsLock.Unlock()
				continue
			}

			delete(c.pendingOperations, f.Header.StreamId)
			c.pendingOperationsLock.Unlock()

			respChan <- f
			close(respChan)
		}
		c.pendingOperationsLock.Lock()
		for streamId, respChan := range c.pendingOperations {
			close(respChan)
			delete(c.pendingOperations, streamId)
		}
		c.pendingOperationsLock.Unlock()
	}()
}

func (c *cqlConn) StartRequestLoop() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer log.Debug("Shutting down request loop on ", c)
		for c.ctx.Err() == nil {
			select {
			case f := <-c.outgoingCh:
				err := defaultCodec.EncodeFrame(f, c.conn)
				if err != nil {
					if (!errors.Is(err, io.EOF) && !IsClosingErr(err)) || c.ctx.Err() == nil {
						log.Errorf("Failed to write/encode frame on cql connection %v: %v", c, err)
					}
					c.cancelFn()
					return
				}
			case <-c.ctx.Done():
				return
			}
		}
	}()
}

func (c *cqlConn) StartEventLoop() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer log.Debugf("Shutting down event loop on %v.", c)

		event, ok := <-c.eventsQueue
		for ; ok; event, ok = <-c.eventsQueue {
			c.eventHandlerLock.Lock()
			if c.eventHandler != nil {
				c.eventHandler(event, c)
			}
			c.eventHandlerLock.Unlock()
		}
	}()
}

func (c *cqlConn) IsInitialized() bool {
	return c.initialized
}

func (c *cqlConn) IsAuthEnabled() (bool, error) {
	if !c.IsInitialized() {
		return true, fmt.Errorf("cql connection not initialized, can not check whether auth is enabled or not")
	}

	return c.authEnabled, nil
}

func (c *cqlConn) InitializeContext(version primitive.ProtocolVersion, ctx context.Context) error {
	authEnabled, err := c.PerformHandshake(version, ctx)
	if err != nil {
		return fmt.Errorf("failed to perform handshake: %w", err)
	}

	c.initialized = true
	c.authEnabled = authEnabled
	return nil
}

func (c *cqlConn) Close() error {
	c.pendingOperationsLock.Lock()
	c.closed = true
	c.pendingOperationsLock.Unlock()
	c.cancelFn()
	err := c.conn.Close()
	if err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}
	c.wg.Wait()
	return nil
}

func (c *cqlConn) sendContext(request *frame.Frame, ctx context.Context) (chan *frame.Frame, error) {
	if c.ctx.Err() != nil {
		return nil, fmt.Errorf("cql connection was closed: %w", io.EOF)
	}

	timeoutCtx, _ := context.WithTimeout(ctx, c.writeTimeout)

	var respChan = make(chan *frame.Frame, 1)

	c.pendingOperationsLock.Lock()
	if c.closed {
		c.pendingOperationsLock.Unlock()
		return nil, errors.New("response channel closed")
	}

	c.pendingOperations[request.Header.StreamId] = respChan
	c.pendingOperationsLock.Unlock()

	var err error
	select {
	case c.outgoingCh <- request:
		return respChan, nil
	case <-c.ctx.Done():
		err = fmt.Errorf("cql connection was closed: %w", io.EOF)
	case <-timeoutCtx.Done():
		err = fmt.Errorf("context finished before completing sending of frame on %v: %w", c, ctx.Err())
	}

	c.pendingOperationsLock.Lock()
	if c.closed {
		c.pendingOperationsLock.Unlock()
		return nil, err
	}
	close(c.pendingOperations[request.Header.StreamId])
	delete(c.pendingOperations, request.Header.StreamId)
	c.pendingOperationsLock.Unlock()
	return nil, err
}

func (c *cqlConn) SendAndReceive(request *frame.Frame, ctx context.Context) (*frame.Frame, error) {
	c.frameProcessor.AssignUniqueId(request)
	respChan, err := c.sendContext(request, ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to send request frame: %w", err)
	}

	readTimeoutCtx, _ := context.WithTimeout(ctx, c.readTimeout)
	select {
	case response, ok := <-respChan:
		if !ok {
			return nil, fmt.Errorf("failed to receive response frame")
		}
		return response, nil
	case <-readTimeoutCtx.Done():
		c.pendingOperationsLock.Lock()
		c.timedOutOperations++
		timedOutOps := c.timedOutOperations
		c.pendingOperationsLock.Unlock()
		if timedOutOps > timeOutsThreshold {
			c.Close()
		}
		return nil, fmt.Errorf("context finished before completing receiving frame on %v: %w", c, readTimeoutCtx.Err())
	}
}

func (c *cqlConn) PerformHandshake(version primitive.ProtocolVersion, ctx context.Context) (auth bool, err error) {
	log.Debug("performing handshake")
	startup := frame.NewFrame(version, -1, message.NewStartup())
	var response *frame.Frame
	authenticator := &DsePlainTextAuthenticator{c.credentials}
	authEnabled := false
	if response, err = c.SendAndReceive(startup, ctx); err == nil {
		switch response.Body.Message.(type) {
		case *message.Ready:
			log.Warnf("%v: expected AUTHENTICATE, got READY – is authentication required?", c)
			break
		case *message.Authenticate:
			authEnabled = true
			var authResponse *frame.Frame
			authResponse, err = performHandshakeStep(authenticator, version, -1, response)
			if err == nil {
				if response, err = c.SendAndReceive(authResponse, ctx); err != nil {
					err = fmt.Errorf("could not send AUTH RESPONSE: %w", err)
				} else if _, authSuccess := response.Body.Message.(*message.AuthSuccess); !authSuccess {
					authResponse, err = performHandshakeStep(authenticator, version, -1, response)
					if err == nil {
						if response, err = c.SendAndReceive(authResponse, ctx); err != nil {
							err = fmt.Errorf("could not send AUTH RESPONSE: %w", err)
						} else if _, authSuccess := response.Body.Message.(*message.AuthSuccess); !authSuccess {
							err = fmt.Errorf("expected AUTH_SUCCESS, got %v", response.Body.Message)
						}
					}
				}
			}
		default:
			err = fmt.Errorf("expected AUTHENTICATE or READY, got %v", response.Body.Message)
		}
	}
	if err == nil {
		log.Debugf("%v: handshake successful", c)
		c.initialized = true
	} else {
		log.Errorf("%v: handshake failed: %v", c, err)
	}
	return authEnabled, err
}

func (c *cqlConn) Query(
	cql string, genericTypeCodec *GenericTypeCodec, version primitive.ProtocolVersion, ctx context.Context) (*ParsedRowSet, error) {
	queryMsg := &message.Query{
		Query: cql,
		Options: &message.QueryOptions{
			Consistency: primitive.ConsistencyLevelLocalQuorum,
		},
	}

	queryFrame := frame.NewFrame(ccProtocolVersion, -1, queryMsg)
	var rowSet *ParsedRowSet
	for {
		localResponse, err := c.SendAndReceive(queryFrame, ctx)
		if err != nil {
			return nil, err
		}

		switch m := localResponse.Body.Message.(type) {
		case *message.RowsResult:
			var newRowSet *ParsedRowSet
			var columns []*message.ColumnMetadata
			var columnsIndexes map[string]int
			if rowSet != nil {
				columns = rowSet.Columns
				columnsIndexes = rowSet.ColumnIndexes
			} else {
				columns = nil
				columnsIndexes = nil
			}

			newRowSet, err = ParseRowsResult(genericTypeCodec, version, m, columns, columnsIndexes)
			if err != nil {
				return nil, fmt.Errorf("could not parse rows result: %w", err)
			}

			var oldRows []*ParsedRow
			oldRows = nil
			if rowSet != nil {
				oldRows = rowSet.Rows
			}

			rowSet = &ParsedRowSet{
				ColumnIndexes: newRowSet.ColumnIndexes,
				Columns:       newRowSet.Columns,
				PagingState:   newRowSet.PagingState,
				Rows:          append(oldRows, newRowSet.Rows...),
			}

			if rowSet.PagingState == nil {
				return rowSet, nil
			}
		case *message.VoidResult:
			if rowSet == nil {
				return nil, fmt.Errorf("server returned void result instead of rows result for query %v", cql)
			}
			return rowSet, nil
		case message.Error:
			return nil, fmt.Errorf("server returned error %v for query %v", m, cql)
		}
	}
}

func (c *cqlConn) Execute(msg message.Message, ctx context.Context) (message.Message, error) {
	queryFrame := frame.NewFrame(ccProtocolVersion, -1, msg)
	localResponse, err := c.SendAndReceive(queryFrame, ctx)
	if err != nil {
		return nil, err
	}

	return localResponse.Body.Message, nil
}

func (c *cqlConn) SendHeartbeat(ctx context.Context) error {
	optionsMsg := &message.Options{}
	heartBeatFrame := frame.NewFrame(ccProtocolVersion, -1, optionsMsg)

	response, err := c.SendAndReceive(heartBeatFrame, ctx)
	if err != nil {
		return fmt.Errorf("failed to send heartbeat: %v", err)
	}

	_, ok := response.Body.Message.(*message.Supported)
	if !ok {
		log.Warnf("Expected SUPPORTED but got %v. Considering this a successful heartbeat regardless.", response.Body.Message)
	}

	return nil
}

// https://github.com/golang/go/issues/4373#issuecomment-671142941
// go 1.16 should fix this
func IsClosingErr(err error) bool {
	return strings.Contains(err.Error(), "use of closed network connection")
}

func IsPeerDisconnect(err error) bool {
	if runtime.GOOS == "windows" {
		return strings.Contains(err.Error(), "forcibly closed by the remote host")
	} else {
		return strings.Contains(err.Error(), "connection reset by peer")
	}
}
