package client

import (
	"bytes"
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

type TestClient struct {
	queue                 chan *request
	streamIds             chan int16
	pendingOperations     *sync.Map
	pendingOperationsLock *sync.RWMutex
	requestTimeout        time.Duration
	waitGroup             *sync.WaitGroup
	cancelFunc            context.CancelFunc
	context               context.Context
	stateLock             *sync.RWMutex
	closed                bool
	connection            net.Conn
	eventsQueue           chan *frame.Frame
}

type request struct {
	buffer          []byte
	responseChannel chan *frame.Frame
}

func newRequest(buffer []byte) *request {
	return &request{
		buffer:          buffer,
		responseChannel: make(chan *frame.Frame, 1),
	}
}

const (
	numberOfStreamIds = int16(2048)
	eventQueueLength  = 2048
)

func NewTestClient(ctx context.Context, address string) (*TestClient, error) {
	streamIdsQueue := make(chan int16, numberOfStreamIds)
	for i := int16(0); i < numberOfStreamIds; i++ {
		streamIdsQueue <- i
	}

	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("could not open connection: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	client := &TestClient{
		queue:                 make(chan *request, numberOfStreamIds),
		streamIds:             streamIdsQueue,
		pendingOperations:     &sync.Map{},
		pendingOperationsLock: &sync.RWMutex{},
		requestTimeout:        2 * time.Second,
		waitGroup:             &sync.WaitGroup{},
		cancelFunc:            cancel,
		context:               ctx,
		stateLock:             &sync.RWMutex{},
		connection:            conn,
		eventsQueue:           make(chan *frame.Frame, eventQueueLength),
	}

	client.waitGroup.Add(1)
	go func() {
		defer client.waitGroup.Done()
		defer client.shutdownInternal()
		for client.context.Err() == nil {
			select {
			case req := <-client.queue:
				_, err := conn.Write(req.buffer)
				if errors.Is(err, io.EOF) {
					return
				} else if err != nil {
					if client.context.Err() == nil &&
						!strings.Contains(err.Error(), "use of closed network connection") &&
						!strings.Contains(err.Error(), "An existing connection was forcibly closed by the remote host") &&
						!strings.Contains(err.Error(), "connection reset by peer") {
						log.Errorf("[TestClient] error while writing to test client connection: %v", err)
					} else {
						log.Infof("[TestClient] error while writing to test client connection: %v", err)
					}
					return
				}
			case <-client.context.Done():
				return
			}
		}
	}()

	client.waitGroup.Add(1)
	go func() {
		defer client.waitGroup.Done()
		defer client.shutdownInternal()
		codec := frame.NewCodec()
		for client.context.Err() == nil {
			parsedFrame, err := codec.DecodeFrame(conn)
			if errors.Is(err, io.EOF) {
				log.Infof("[TestClient] EOF in test client connection")
				break
			} else if err != nil {
				if client.context.Err() == nil &&
					!strings.Contains(err.Error(), "use of closed network connection") &&
					!strings.Contains(err.Error(), "An existing connection was forcibly closed by the remote host") &&
					!strings.Contains(err.Error(), "connection reset by peer") {
					log.Errorf("[TestClient] error while reading from test client connection: %v", err)
				} else {
					log.Infof("[TestClient] error while reading from test client connection: %v", err)
				}
				break
			}

			log.Infof("[TestClient] received response: %v", parsedFrame.Body.Message)

			if parsedFrame.Body.Message.GetOpCode() == primitive.OpCodeEvent {
				select {
				case client.eventsQueue <- parsedFrame:
				default:
					log.Warnf("[TestClient] events queue is full, discarding event message...")
				}
				continue
			}

			client.pendingOperationsLock.RLock()
			respChan, ok := client.pendingOperations.LoadAndDelete(parsedFrame.Header.StreamId)
			if ok {
				respChan.(chan *frame.Frame) <- parsedFrame
			}
			client.pendingOperationsLock.RUnlock()

			if _, protocolErrorOccured := parsedFrame.Body.Message.(*message.ProtocolError); protocolErrorOccured {
				log.Errorf("[TestClient] Protocol error in test client connection, closing: %v", parsedFrame.Body.Message)
				break
			}

			client.ReturnStreamId(parsedFrame.Header.StreamId)
			if !ok {
				log.Warnf("[TestClient] could not find response channel for streamid %d, skipping", parsedFrame.Header.StreamId)
			}
		}
	}()

	return client, nil
}

func (testClient *TestClient) isClosed() bool {
	testClient.stateLock.RLock()
	defer testClient.stateLock.RUnlock()
	return testClient.closed
}

func (testClient *TestClient) PerformHandshake(
	ctx context.Context, version primitive.ProtocolVersion, useAuth bool, username string, password string) error {
	response, _, err := testClient.SendMessage(ctx, version, message.NewStartup())
	if err != nil {
		return fmt.Errorf("could not send startup frame: %w", err)
	}

	if useAuth {
		parsedAuthenticateResponse, ok := response.Body.Message.(*message.Authenticate)
		if !ok {
			return fmt.Errorf("expected authenticate but got %02x", response.Body.Message.GetOpCode())
		}

		authenticator := NewDsePlainTextAuthenticator(username, password)
		initialResponse, err := authenticator.InitialResponse(parsedAuthenticateResponse.Authenticator)
		if err != nil {
			return fmt.Errorf("could not create initial response token: %w", err)
		}

		response, _, err = testClient.SendMessage(ctx, version, &message.AuthResponse{Token: initialResponse})
		if err != nil {
			return fmt.Errorf("could not send auth response: %w", err)
		}

		if response.Body.Message.GetOpCode() != primitive.OpCodeAuthSuccess {
			return fmt.Errorf("expected auth success but received %v", response.Body.Message)
		}

		return nil
	}

	if response.Body.Message.GetOpCode() != primitive.OpCodeReady {
		return fmt.Errorf("expected ready but received %v", response.Body.Message)
	}

	return nil
}

func (testClient *TestClient) PerformDefaultHandshake(ctx context.Context, version primitive.ProtocolVersion, useAuth bool) error {
	return testClient.PerformHandshake(ctx, version, useAuth, "cassandra", "cassandra")
}

func (testClient *TestClient) Shutdown() error {
	err := testClient.shutdownInternal()
	if err != nil {
		testClient.waitGroup.Wait()
	}
	return err
}

func (testClient *TestClient) shutdownInternal() error {
	if testClient.isClosed() {
		return nil
	}

	testClient.stateLock.Lock()
	defer testClient.stateLock.Unlock()
	if !testClient.closed {
		testClient.closed = true
		testClient.cancelFunc()
		err := testClient.connection.Close()

		testClient.pendingOperationsLock.Lock()
		testClient.pendingOperations.Range(func(key, value interface{}) bool {
			close(value.(chan *frame.Frame))
			testClient.pendingOperations.Delete(key)
			return true
		})
		testClient.pendingOperationsLock.Unlock()

		close(testClient.eventsQueue)

		if err != nil {
			return fmt.Errorf("could not close connection: %w", err)
		}
	}

	return nil
}

func (testClient *TestClient) BorrowStreamId() (int16, error) {
	select {
	case id := <-testClient.streamIds:
		return id, nil
	default:
		return 0, errors.New("no streamIds available")
	}
}

func (testClient *TestClient) ReturnStreamId(streamId int16) {
	testClient.streamIds <- streamId
}

func (testClient *TestClient) SendRawRequest(ctx context.Context, streamId int16, reqBuf []byte) (*frame.Frame, error) {
	req := newRequest(reqBuf)

	testClient.pendingOperationsLock.RLock()
	if testClient.closed {
		testClient.pendingOperationsLock.RUnlock()
		return nil, errors.New("response channel closed")
	}

	testClient.pendingOperationsLock.RUnlock()

	if _, ok := testClient.pendingOperations.Load(streamId); ok {
		return nil, errors.New("stream id already in use")
	}
	testClient.pendingOperations.Store(streamId, req.responseChannel)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case testClient.queue <- req:
	}

	var response *frame.Frame = nil
	var ok bool
	var timedOut bool
	var canceled bool
	select {
	case response, ok = <-req.responseChannel:
	case <-time.After(testClient.requestTimeout):
		timedOut = true
	case <-ctx.Done():
		canceled = true
	}

	testClient.pendingOperations.Delete(streamId)

	if canceled {
		return nil, ctx.Err()
	}

	if timedOut {
		return nil, errors.New("request timed out at client level")
	}

	if !ok {
		return nil, errors.New("response channel closed")
	}

	return response, nil
}

func (testClient *TestClient) SendRequest(ctx context.Context, request *frame.Frame) (*frame.Frame, int16, error) {
	streamId, err := testClient.BorrowStreamId()
	if err != nil {
		return nil, streamId, err
	}

	request.Header.StreamId = streamId

	buf := &bytes.Buffer{}
	err = frame.NewCodec().EncodeFrame(request, buf)
	if err != nil {
		return nil, streamId, fmt.Errorf("could not encode request: %w", err)
	}
	response, err := testClient.SendRawRequest(ctx, streamId, buf.Bytes())
	return response, streamId, err
}

func (testClient *TestClient) SendMessage(
	ctx context.Context, protocolVersion primitive.ProtocolVersion, message message.Message) (*frame.Frame, int16, error) {
	streamId, err := testClient.BorrowStreamId()
	if err != nil {
		return nil, streamId, err
	}

	reqFrame := frame.NewFrame(protocolVersion, streamId, message)

	buf := &bytes.Buffer{}
	err = frame.NewCodec().EncodeFrame(reqFrame, buf)
	if err != nil {
		return nil, streamId, fmt.Errorf("could not encode request: %w", err)
	}

	response, err := testClient.SendRawRequest(ctx, streamId, buf.Bytes())
	return response, streamId, err
}

func (testClient *TestClient) GetEventMessage(timeout time.Duration) (*frame.Frame, error) {
	select {
	case eventMsg, ok := <-testClient.eventsQueue:
		if !ok {
			return nil, errors.New("channel closed")
		}
		return eventMsg, nil
	case <-time.After(timeout):
		return nil, errors.New("timeout retrieving event message")
	}
}
