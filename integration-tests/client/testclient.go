package client

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/riptano/cloud-gate/integration-tests/cql"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"sync"
	"time"
)

type TestClient struct {
	queue                 chan *request
	streamIds             chan uint16
	pendingOperations     map[uint16]chan *cql.Frame
	pendingOperationsLock *sync.RWMutex
	requestTimeout        time.Duration
	waitGroup             *sync.WaitGroup
	cancelFunc            context.CancelFunc
	context               context.Context
	stateLock             *sync.RWMutex
	closed				  bool
	connection			  net.Conn
}

type request struct {
	buffer          []byte
	responseChannel chan *cql.Frame
}

func newRequest(buffer []byte) *request {
	return &request{
		buffer:          buffer,
		responseChannel: make(chan *cql.Frame, 1),
	}
}

func NewTestClient(address string, numberOfStreamIds int) (*TestClient, error) {
	streamIdsQueue := make(chan uint16, numberOfStreamIds)
	for i := 0; i < numberOfStreamIds; i++ {
		streamIdsQueue <- uint16(i)
	}

	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	client := &TestClient{
		queue:                 make(chan *request, numberOfStreamIds),
		streamIds:             streamIdsQueue,
		pendingOperations:     make(map[uint16]chan *cql.Frame),
		pendingOperationsLock: &sync.RWMutex{},
		requestTimeout:        5 * time.Second,
		waitGroup:             &sync.WaitGroup{},
		cancelFunc:            cancel,
		context:               ctx,
		stateLock: 			   &sync.RWMutex{},
		connection: 		   conn,
	}

	client.waitGroup.Add(1)
	go func() {
		defer client.waitGroup.Done()
		defer client.shutdownInternal()
		for {
			select {
			case req := <-client.queue:
				_, err := conn.Write(req.buffer)
				if err == io.EOF {
					return
				} else if err != nil {
					log.Errorf("error in test client connection: ", err.Error())
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
		for {
			frame, err := readFrame(conn)
			if err == io.EOF {
				log.Infof("EOF in test client connection")
				break
			} else if err != nil {
				log.Errorf("error while reading from test client connection: ", err.Error())
				break
			}

			parsedFrame, err := cql.ParseFrame(frame)
			if err != nil {
				log.Errorf("error while parsing frame from test client connection: ", err.Error())
				break
			}

			client.pendingOperationsLock.RLock()
			respChan, ok := client.pendingOperations[parsedFrame.StreamId]
			client.pendingOperationsLock.RUnlock()

			// TODO on event responses, do not free streamid
			if !ok {
				log.Warnf("could not find response channel for streamid %d, skipping", parsedFrame.StreamId)
				client.ReturnStreamId(parsedFrame.StreamId)
				continue
			}
			respChan <- parsedFrame
			client.ReturnStreamId(parsedFrame.StreamId)
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

func readFrame(connection net.Conn) ([]byte, error) {
	version := make([]byte, 1)
	_, err := io.ReadFull(connection, version)
	if err != nil {
		return nil, err
	}
	data := make([]byte, cql.GetHeaderLength(version[0] & 0x7F))
	_, err = io.ReadFull(connection, data[1:])
	if err != nil {
		return nil, err
	}
	data[0] = version[0]
	bodyLen := binary.BigEndian.Uint32(data[len(data)-4:])
	rest := make([]byte, int(bodyLen))
	bytesRead, err := io.ReadFull(connection, rest)
	if err != nil {
		return nil, err
	}
	if uint32(bytesRead) != bodyLen {
		return nil, errors.New("failed to read body")
	}
	data = append(data, rest[:bytesRead]...)
	return data, nil
}

func (testClient *TestClient) isClosed() bool {
	testClient.stateLock.RLock()
	defer testClient.stateLock.RUnlock()
	return testClient.closed
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
		if err != nil {
			return err
		}
	}

	return nil
}

func (testClient *TestClient) BorrowStreamId() (uint16, error) {
	select {
	case id := <-testClient.streamIds:
		return id, nil
	default:
		return 0, errors.New("no streamIds available")
	}
}

func (testClient *TestClient) ReturnStreamId(streamId uint16) {
	testClient.streamIds <- streamId
}

func (testClient *TestClient) SendRawRequest(streamId uint16, reqBuf []byte) (*cql.Frame, error) {
	if cql.Uses2BytesStreamIds(reqBuf[0]) {
		binary.BigEndian.PutUint16(reqBuf[2:4], streamId)
	} else {
		reqBuf[2] = byte(streamId)
	}

	req := newRequest(reqBuf)

	testClient.pendingOperationsLock.Lock()
	if _, ok := testClient.pendingOperations[streamId]; ok {
		testClient.pendingOperationsLock.Unlock()
		return nil, errors.New("stream id already in use")
	}

	testClient.pendingOperations[streamId] = req.responseChannel
	testClient.pendingOperationsLock.Unlock()

	testClient.queue <- req

	var response *cql.Frame = nil
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
		return nil, errors.New("timed out")
	}

	if !ok {
		return nil, errors.New("response channel closed")
	}

	return response, nil
}

func (testClient *TestClient) SendRequest(request []byte) (*cql.Frame, error) {
	streamId, err := testClient.BorrowStreamId()
	if err != nil {
		return nil, err
	}

	err = cql.SetStreamId(request[0] & 0x7F, request, streamId)
	if err != nil {
		return nil, err
	}
	return testClient.SendRawRequest(streamId, request)
}