package integration_tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"

	"github.com/datastax/zdm-proxy/integration-tests/setup"
)

// TestWriteCoalescerHandlesWrittenFalse tests that the write coalescer correctly handles
// the case when the segment writer returns written=false, which happens when a frame
// cannot fit in the current segment payload buffer and needs to be written later.
func TestWriteCoalescerHandlesWrittenFalse(t *testing.T) {
	// Create a config with very small write buffer sizes to force the written=false condition
	conf := setup.NewTestConfig("127.0.1.1", "127.0.1.2")

	// Set extremely small buffer sizes and reduce workers to trigger written=false more frequently
	conf.RequestWriteBufferSizeBytes = 256 // Extremely small buffer to force frequent flushes
	conf.ResponseWriteBufferSizeBytes = 256
	conf.RequestResponseMaxWorkers = 2 // Very few workers to increase contention
	conf.WriteMaxWorkers = 2
	conf.ReadMaxWorkers = 2

	testSetup, err := setup.NewCqlServerTestSetup(t, conf, false, false, false)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	// Create request handlers that capture all requests and return successful responses
	originRequestHandler := NewRequestCapturingHandler()
	targetRequestHandler := NewRequestCapturingHandler()

	testSetup.Origin.CqlServer.RequestHandlers = []client.RequestHandler{
		originRequestHandler.HandleRequest,
		client.NewDriverConnectionInitializationHandler("origin", "dc1", func(_ string) {}),
	}
	testSetup.Target.CqlServer.RequestHandlers = []client.RequestHandler{
		targetRequestHandler.HandleRequest,
		client.NewDriverConnectionInitializationHandler("target", "dc2", func(_ string) {}),
	}

	err = testSetup.Start(nil, false, primitive.ProtocolVersion5)
	require.Nil(t, err)

	proxy, err := setup.NewProxyInstanceWithConfig(conf)
	require.Nil(t, err)
	require.NotNil(t, proxy)
	defer proxy.Shutdown()

	testSetup.Client.CqlClient.ReadTimeout = 5 * time.Second // Short timeout to fail fast and expose bugs quickly
	cqlConn, err := testSetup.Client.CqlClient.ConnectAndInit(context.Background(), primitive.ProtocolVersion5, client.ManagedStreamId)
	require.Nil(t, err, "client connection failed: %v", err)
	defer cqlConn.Close()

	// Spawn multiple goroutines that concurrently send INSERT queries
	// This should trigger the written=false condition and expose any race conditions
	numGoroutines := 5
	queriesPerGoroutine := 10
	var wg sync.WaitGroup
	errorsChan := make(chan error, numGoroutines*queriesPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		goroutineId := g
		go func() {
			defer wg.Done()

			for i := 0; i < queriesPerGoroutine; i++ {
				// Create queries with large payloads to exceed the small buffer
				largeValue := make([]byte, 400) // 400 bytes of data
				for j := range largeValue {
					largeValue[j] = byte('A' + (j % 26))
				}

				queryMsg := &message.Query{
					Query: fmt.Sprintf("INSERT INTO test.table (id, data) VALUES (%d, '%s')",
						goroutineId*queriesPerGoroutine+i, string(largeValue)),
					Options: &message.QueryOptions{
						Consistency: primitive.ConsistencyLevelOne,
					},
				}

				queryFrame := frame.NewFrame(primitive.ProtocolVersion5, int16((goroutineId*queriesPerGoroutine+i)%100+1), queryMsg)
				responseFrame, err := cqlConn.SendAndReceive(queryFrame)
				if err != nil {
					errorsChan <- fmt.Errorf("goroutine %d query %d failed: %v", goroutineId, i, err)
					return
				}
				if responseFrame == nil {
					errorsChan <- fmt.Errorf("goroutine %d query %d returned nil response", goroutineId, i)
					return
				}

				// Verify we got a successful response
				if _, ok := responseFrame.Body.Message.(*message.VoidResult); !ok {
					errorsChan <- fmt.Errorf("goroutine %d query %d did not return VoidResult", goroutineId, i)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errorsChan)

	// Check for errors from goroutines
	var errors []error
	for err := range errorsChan {
		errors = append(errors, err)
	}
	require.Empty(t, errors, "Encountered errors during concurrent writes: %v", errors)

	totalQueries := numGoroutines * queriesPerGoroutine

	// Verify that all queries were received by both origin and target
	originRequests := originRequestHandler.GetQueryRequests()
	targetRequests := targetRequestHandler.GetQueryRequests()

	require.GreaterOrEqual(t, len(originRequests), totalQueries,
		"origin should have received at least %d queries, got %d", totalQueries, len(originRequests))
	require.GreaterOrEqual(t, len(targetRequests), totalQueries,
		"target should have received at least %d queries, got %d", totalQueries, len(targetRequests))
}

// TestWriteCoalescerMultipleFramesInSegment tests that multiple frames can be written
// to a segment payload when they fit, and that leftover frames are properly handled.
func TestWriteCoalescerMultipleFramesInSegment(t *testing.T) {
	conf := setup.NewTestConfig("127.0.1.1", "127.0.1.2")

	// Set very small buffer sizes and reduce workers to maximize contention
	conf.RequestWriteBufferSizeBytes = 512 // Small buffer to force frequent flushes
	conf.ResponseWriteBufferSizeBytes = 512
	conf.RequestResponseMaxWorkers = 2
	conf.WriteMaxWorkers = 2
	conf.ReadMaxWorkers = 2

	testSetup, err := setup.NewCqlServerTestSetup(t, conf, false, false, false)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	originRequestHandler := NewRequestCapturingHandler()
	targetRequestHandler := NewRequestCapturingHandler()

	testSetup.Origin.CqlServer.RequestHandlers = []client.RequestHandler{
		originRequestHandler.HandleRequest,
		client.NewDriverConnectionInitializationHandler("origin", "dc1", func(_ string) {}),
	}
	testSetup.Target.CqlServer.RequestHandlers = []client.RequestHandler{
		targetRequestHandler.HandleRequest,
		client.NewDriverConnectionInitializationHandler("target", "dc2", func(_ string) {}),
	}

	err = testSetup.Start(nil, false, primitive.ProtocolVersion5)
	require.Nil(t, err)

	proxy, err := setup.NewProxyInstanceWithConfig(conf)
	require.Nil(t, err)
	require.NotNil(t, proxy)
	defer proxy.Shutdown()

	testSetup.Client.CqlClient.ReadTimeout = 5 * time.Second // Short timeout to fail fast and expose bugs quickly
	cqlConn, err := testSetup.Client.CqlClient.ConnectAndInit(context.Background(), primitive.ProtocolVersion5, client.ManagedStreamId)
	require.Nil(t, err, "client connection failed: %v", err)
	defer cqlConn.Close()

	// Spawn multiple goroutines sending bursts of queries concurrently
	numGoroutines := 8
	queriesPerGoroutine := 15
	var wg sync.WaitGroup
	errorsChan := make(chan error, numGoroutines*queriesPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		goroutineId := g
		go func() {
			defer wg.Done()

			for i := 0; i < queriesPerGoroutine; i++ {
				// Create moderately-sized INSERT queries with variable length data
				dataSize := 200 + (i * 10) // Variable size from 200 to 340 bytes
				largeValue := make([]byte, dataSize)
				for j := range largeValue {
					largeValue[j] = byte('A' + (j % 26))
				}

				queryMsg := &message.Query{
					Query: fmt.Sprintf("INSERT INTO test.table (id, data) VALUES (%d, '%s')",
						goroutineId*queriesPerGoroutine+i, string(largeValue)),
					Options: &message.QueryOptions{
						Consistency: primitive.ConsistencyLevelOne,
					},
				}

				queryFrame := frame.NewFrame(primitive.ProtocolVersion5, int16((goroutineId*queriesPerGoroutine+i)%100+1), queryMsg)
				responseFrame, err := cqlConn.SendAndReceive(queryFrame)
				if err != nil {
					errorsChan <- fmt.Errorf("goroutine %d query %d failed: %v", goroutineId, i, err)
					return
				}
				if responseFrame == nil {
					errorsChan <- fmt.Errorf("goroutine %d query %d returned nil response", goroutineId, i)
					return
				}

				// Verify we got a successful response
				if _, ok := responseFrame.Body.Message.(*message.VoidResult); !ok {
					errorsChan <- fmt.Errorf("goroutine %d query %d did not return VoidResult", goroutineId, i)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errorsChan)

	// Check for errors from goroutines
	var errors []error
	for err := range errorsChan {
		errors = append(errors, err)
	}
	require.Empty(t, errors, "Encountered errors during concurrent writes: %v", errors)

	totalQueries := numGoroutines * queriesPerGoroutine
	originRequests := originRequestHandler.GetQueryRequests()
	targetRequests := targetRequestHandler.GetQueryRequests()

	require.GreaterOrEqual(t, len(originRequests), totalQueries,
		"origin should have received at least %d queries, got %d", totalQueries, len(originRequests))
	require.GreaterOrEqual(t, len(targetRequests), totalQueries,
		"target should have received at least %d queries, got %d", totalQueries, len(targetRequests))
}

// RequestCapturingHandler captures all incoming requests for verification
type RequestCapturingHandler struct {
	lock     *sync.Mutex
	requests []*frame.Frame
}

func NewRequestCapturingHandler() *RequestCapturingHandler {
	return &RequestCapturingHandler{
		lock:     &sync.Mutex{},
		requests: make([]*frame.Frame, 0),
	}
}

func (recv *RequestCapturingHandler) HandleRequest(
	request *frame.Frame,
	_ *client.CqlServerConnection,
	_ client.RequestHandlerContext) (response *frame.Frame) {

	recv.lock.Lock()
	recv.requests = append(recv.requests, request)
	recv.lock.Unlock()

	// Return appropriate response based on request type
	switch msg := request.Body.Message.(type) {
	case *message.Query:
		// Let system table queries pass through to the next handler
		q := strings.ToLower(strings.TrimSpace(msg.Query))
		if strings.Contains(q, "system.local") || strings.Contains(q, "system.peers") {
			return nil // Let the system tables handler deal with it
		}
		// Return a void result for non-system queries
		return frame.NewFrame(
			request.Header.Version,
			request.Header.StreamId,
			&message.VoidResult{},
		)
	default:
		// For other request types, return nil (let other handlers deal with it)
		return nil
	}
}

func (recv *RequestCapturingHandler) GetQueryRequests() []*frame.Frame {
	recv.lock.Lock()
	defer recv.lock.Unlock()

	queries := make([]*frame.Frame, 0)
	for _, req := range recv.requests {
		if _, ok := req.Body.Message.(*message.Query); ok {
			queries = append(queries, req)
		}
	}
	return queries
}

func (recv *RequestCapturingHandler) GetAllRequests() []*frame.Frame {
	recv.lock.Lock()
	defer recv.lock.Unlock()

	result := make([]*frame.Frame, len(recv.requests))
	copy(result, recv.requests)
	return result
}

func (recv *RequestCapturingHandler) Clear() {
	recv.lock.Lock()
	defer recv.lock.Unlock()
	recv.requests = make([]*frame.Frame, 0)
}
