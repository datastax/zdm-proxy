package cloudgateproxy

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
	"strings"
	"sync"
	"time"
)

const maxIncomingPending = 2048
const maxOutgoingPending = 2048

type CqlConnection interface {
	IsInitialized() bool
	Initialize(version primitive.ProtocolVersion, streamId int16) error
	InitializeContext(version primitive.ProtocolVersion, streamId int16, ctx context.Context) error
	Send(request *frame.Frame) error
	Receive() (*frame.Frame, error)
	SendContext(request *frame.Frame, ctx context.Context) error
	ReceiveContext(ctx context.Context) (*frame.Frame, error)
	SendAndReceive(request *frame.Frame) (*frame.Frame, error)
	Close() error
}

// Not thread safe
type cqlConn struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
	conn         net.Conn
	credentials  *AuthCredentials
	initialized  bool
	cancelFn     context.CancelFunc
	ctx          context.Context
	wg           *sync.WaitGroup
	incomingCh   chan *frame.Frame
	outgoingCh   chan *frame.Frame
}

var (
	StreamIdMismatchErr = errors.New("stream id of the response is different from the stream id of the request")
)

func (c *cqlConn) String() string {
	return fmt.Sprintf("cqlConn{conn: %v}", c.conn.RemoteAddr().String())
}

func NewCqlConnection(conn net.Conn, username string, password string, readTimeout time.Duration, writeTimeout time.Duration) CqlConnection {
	ctx, cFn := context.WithCancel(context.Background())
	cqlConn := &cqlConn{
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
		conn:         conn,
		credentials: &AuthCredentials{
			Username: username,
			Password: password,
		},
		initialized: false,
		ctx:         ctx,
		cancelFn:    cFn,
		wg:          &sync.WaitGroup{},
		incomingCh:  make(chan *frame.Frame, maxIncomingPending),
		outgoingCh:  make(chan *frame.Frame, maxOutgoingPending),
	}
	cqlConn.StartRequestLoop()
	cqlConn.StartResponseLoop()
	return cqlConn
}

func (c *cqlConn) StartResponseLoop() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer close(c.incomingCh)
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
			select {
			case c.incomingCh <- f:
			case <-c.ctx.Done():
			}
		}
	}()
}

func (c *cqlConn) StartRequestLoop() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer log.Debug("Shutting down request loop on %v.", c)
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

func (c *cqlConn) IsInitialized() bool {
	return c.initialized
}

func (c *cqlConn) InitializeContext(version primitive.ProtocolVersion, streamId int16, ctx context.Context) error {
	err := c.PerformHandshake(version, streamId, ctx)
	if err != nil {
		return fmt.Errorf("failed to perform handshake: %w", err)
	}

	c.initialized = true
	return nil
}

func (c *cqlConn) Initialize(version primitive.ProtocolVersion, streamId int16) error {
	return c.InitializeContext(version, streamId, context.Background())
}

func (c *cqlConn) Close() error {
	c.cancelFn()
	err := c.conn.Close()
	if err != nil {
		return fmt.Errorf("failed to close connection: %w", err)
	}
	c.wg.Wait()
	return nil
}

func (c *cqlConn) Send(request *frame.Frame) error {
	return c.SendContext(request, context.Background())
}

func (c *cqlConn) SendContext(request *frame.Frame, ctx context.Context) error {
	if c.ctx.Err() != nil {
		return fmt.Errorf("cql connection was closed: %w", io.EOF)
	}

	timeoutCtx, _ := context.WithTimeout(ctx, c.writeTimeout)
	select {
	case c.outgoingCh <- request:
		return nil
	case <-c.ctx.Done():
		return fmt.Errorf("cql connection was closed: %w", io.EOF)
	case <-timeoutCtx.Done():
		return fmt.Errorf("context finished before completing sending of frame on %v: %w", c, ctx.Err())
	}
}

func (c *cqlConn) Receive() (*frame.Frame, error) {
	return c.ReceiveContext(context.Background())
}

func (c *cqlConn) ReceiveContext(ctx context.Context) (*frame.Frame, error) {
	timeoutCtx, _ := context.WithTimeout(ctx, c.readTimeout)
	select {
	case response, ok := <-c.incomingCh:
		if !ok {
			return nil, fmt.Errorf("failed to receive because connection closed: %w", io.EOF)
		}
		return response, nil
	case <-timeoutCtx.Done():
		return nil, fmt.Errorf("context finished before completing receiving frame on %v: %w", c, ctx.Err())
	}
}

func (c *cqlConn) SendAndReceiveContext(request *frame.Frame, ctx context.Context) (*frame.Frame, error) {
	writeTimeoutCtx, _ := context.WithTimeout(ctx, c.writeTimeout)
	err := c.SendContext(request, writeTimeoutCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to send request frame: %w", err)
	}

	readTimeoutCtx, _ := context.WithTimeout(ctx, c.readTimeout)
	response, err := c.ReceiveContext(readTimeoutCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to receive response frame: %w", err)
	}

	if response.Header.StreamId != request.Header.StreamId {
		return response, fmt.Errorf(
			"stream id mismatch; %d (request) vs %d (response): %w",
			request.Header.StreamId, response.Header.StreamId, StreamIdMismatchErr)
	}
	return response, nil
}
func (c *cqlConn) SendAndReceive(request *frame.Frame) (*frame.Frame, error) {
	return c.SendAndReceiveContext(request, context.Background())
}

func (c *cqlConn) PerformHandshake(version primitive.ProtocolVersion, streamId int16, ctx context.Context) (err error) {
	log.Debug("performing handshake")
	startup := frame.NewFrame(version, streamId, message.NewStartup())
	var response *frame.Frame
	authenticator := &DsePlainTextAuthenticator{c.credentials}
	if response, err = c.SendAndReceiveContext(startup, ctx); err == nil {
		switch response.Body.Message.(type) {
		case *message.Ready:
			log.Warnf("%v: expected AUTHENTICATE, got READY – is authentication required?", c)
			break
		case *message.Authenticate:
			var authResponse *frame.Frame
			authResponse, err = performHandshakeStep(authenticator, version, streamId, response)
			if err == nil {
				if response, err = c.SendAndReceiveContext(authResponse, ctx); err != nil {
					err = fmt.Errorf("could not send AUTH RESPONSE: %w", err)
				} else if _, authSuccess := response.Body.Message.(*message.AuthSuccess); !authSuccess {
					authResponse, err = performHandshakeStep(authenticator, version, streamId, response)
					if err == nil {
						if response, err = c.SendAndReceiveContext(authResponse, ctx); err != nil {
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
	return err
}

// https://github.com/golang/go/issues/4373#issuecomment-671142941
// go 1.16 should fix this
func IsClosingErr(err error) bool {
	return strings.Contains(err.Error(), "use of closed network connection")
}