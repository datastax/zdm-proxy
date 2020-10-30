package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/message"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"sync"
	"time"
)

type TestClient struct {
	queue                 chan *request
	streamIds             chan int16
	pendingOperations     map[int16]chan *frame.Frame
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

func NewTestClient(address string) (*TestClient, error) {
	streamIdsQueue := make(chan int16, numberOfStreamIds)
	for i := int16(0); i < numberOfStreamIds; i++ {
		streamIdsQueue <- i
	}

	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("could not open connection: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	client := &TestClient{
		queue:                 make(chan *request, numberOfStreamIds),
		streamIds:             streamIdsQueue,
		pendingOperations:     make(map[int16]chan *frame.Frame),
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
		for {
			select {
			case req := <-client.queue:
				_, err := conn.Write(req.buffer)
				if errors.Is(err, io.EOF) {
					return
				} else if err != nil {
					log.Errorf("[TestClient] error in test client connection: %v", err)
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
		for {
			parsedFrame, err := codec.DecodeFrame(conn)
			if errors.Is(err, io.EOF) {
				log.Infof("[TestClient] EOF in test client connection")
				break
			} else if err != nil {
				log.Errorf("[TestClient] error while reading from test client connection: %v", err)
				break
			}

			log.Infof("[TestClient] received response: %v", parsedFrame.Body.Message)

			if parsedFrame.Body.Message.GetOpCode() == cassandraprotocol.OpCodeEvent {
				select {
				case client.eventsQueue <- parsedFrame:
				default:
					log.Warnf("[TestClient] events queue is full, discarding event message...")
				}
				continue
			}

			client.pendingOperationsLock.RLock()
			respChan, ok := client.pendingOperations[parsedFrame.Header.StreamId]
			client.pendingOperationsLock.RUnlock()

			if !ok {
				log.Warnf("[TestClient] could not find response channel for streamid %d, skipping", parsedFrame.Header.StreamId)
				client.ReturnStreamId(parsedFrame.Header.StreamId)
				continue
			}
			respChan <- parsedFrame
			client.ReturnStreamId(parsedFrame.Header.StreamId)
		}

		client.pendingOperationsLock.Lock()
		for streamId, respChan := range client.pendingOperations {
			close(respChan)
			delete(client.pendingOperations, streamId)
		}
		client.pendingOperationsLock.Unlock()
	}()

	return client, nil
}

func (testClient *TestClient) isClosed() bool {
	testClient.stateLock.RLock()
	defer testClient.stateLock.RUnlock()
	return testClient.closed
}

func (testClient *TestClient) PerformHandshake(
	version cassandraprotocol.ProtocolVersion, useAuth bool, username string, password string) error {
	response, _, err := testClient.SendMessage(version, message.NewStartup())
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

		response, _, err = testClient.SendMessage(version, &message.AuthResponse{Token: initialResponse})
		if err != nil {
			return fmt.Errorf("could not send auth response: %w", err)
		}

		if response.Body.Message.GetOpCode() != cassandraprotocol.OpCodeAuthSuccess {
			return fmt.Errorf("expected auth success but received %v", response.Body.Message)
		}

		return nil
	}

	if response.Body.Message.GetOpCode() != cassandraprotocol.OpCodeReady {
		return fmt.Errorf("expected ready but received %v", response.Body.Message)
	}

	return nil
}

func (testClient *TestClient) PerformDefaultHandshake(version cassandraprotocol.ProtocolVersion, useAuth bool) error {
	return testClient.PerformHandshake(version, useAuth, "cassandra", "cassandra")
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

		testClient.pendingOperationsLock.RLock()
		for _, respChan := range testClient.pendingOperations {
			close(respChan)
		}
		testClient.pendingOperationsLock.RUnlock()

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

func (testClient *TestClient) SendRawRequest(streamId int16, reqBuf []byte) (*frame.Frame, error) {
	req := newRequest(reqBuf)

	testClient.pendingOperationsLock.Lock()
	if _, ok := testClient.pendingOperations[streamId]; ok {
		testClient.pendingOperationsLock.Unlock()
		return nil, errors.New("stream id already in use")
	}

	testClient.pendingOperations[streamId] = req.responseChannel
	testClient.pendingOperationsLock.Unlock()

	testClient.queue <- req

	var response *frame.Frame = nil
	var ok bool
	var timedOut bool
	select {
	case response, ok = <-req.responseChannel:
	case <-time.After(testClient.requestTimeout):
		timedOut = true
	}

	testClient.pendingOperationsLock.Lock()
	delete(testClient.pendingOperations, streamId)
	testClient.pendingOperationsLock.Unlock()

	if timedOut {
		return nil, errors.New("request timed out at client level")
	}

	if !ok {
		return nil, errors.New("response channel closed")
	}

	return response, nil
}

func (testClient *TestClient) SendRequest(request *frame.Frame) (*frame.Frame, int16, error) {
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
	response, err := testClient.SendRawRequest(streamId, buf.Bytes())
	return response, streamId, err
}

func (testClient *TestClient) SendMessage(
	protocolVersion cassandraprotocol.ProtocolVersion, message message.Message) (*frame.Frame, int16, error) {
	streamId, err := testClient.BorrowStreamId()
	if err != nil {
		return nil, streamId, err
	}

	reqFrame, err := frame.NewRequestFrame(protocolVersion, streamId, false, nil, message)
	if err != nil {
		return nil, streamId, err
	}

	buf := &bytes.Buffer{}
	err = frame.NewCodec().EncodeFrame(reqFrame, buf)
	if err != nil {
		return nil, streamId, fmt.Errorf("could not encode request: %w", err)
	}

	response, err := testClient.SendRawRequest(streamId, buf.Bytes())
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
