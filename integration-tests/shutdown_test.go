package integration_tests

import (
	"context"
	client2 "github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/integration-tests/simulacron"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestShutdownInFlightRequests(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetupWithSession(false, false)
	defer testSetup.Cleanup()

	config := setup.NewTestConfig(testSetup.Origin.GetInitialContactPoint(), testSetup.Target.GetInitialContactPoint())
	config.RequestTimeoutMs = 30000
	proxy, err := setup.NewProxyInstance(testSetup.Origin, testSetup.Target)
	require.Nil(t, err)
	shutdownProxyTriggered := false
	defer func() {
		if !shutdownProxyTriggered {
			proxy.Shutdown()
		}
	}()

	testClient, err := client2.NewCqlClient("127.0.0.1:14002", nil).
		ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 0)
	if err != nil {
		t.Fatalf("could not connect: %v", err)
	}
	defer testClient.Close()

	testSetup.Origin.Prime(
		simulacron.WhenQuery("SELECT * FROM test1", simulacron.NewWhenQueryOptions()).
			ThenSuccess().WithDelay(2 * time.Second))
	testSetup.Origin.Prime(
		simulacron.WhenQuery("SELECT * FROM test2", simulacron.NewWhenQueryOptions()).
			ThenSuccess().WithDelay(7 * time.Second))

	queryMsg1 := &message.Query{
		Query:   "SELECT * FROM test1",
		Options: nil,
	}

	queryMsg2 := &message.Query{
		Query:   "SELECT * FROM test2",
		Options: nil,
	}

	beginTimestamp := time.Now()

	reqFrame := frame.NewFrame(primitive.ProtocolVersion4, 2, queryMsg1)
	inflightRequest, err := testClient.Send(reqFrame)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	reqFrame2 := frame.NewFrame(primitive.ProtocolVersion4, 3, queryMsg2)
	inflightRequest2, err := testClient.Send(reqFrame2)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(1 * time.Second)

	shutdownComplete := make(chan bool)
	go func() {
		proxy.Shutdown()
		close(shutdownComplete)
	}()
	shutdownProxyTriggered = true

	select {
	case rsp := <-inflightRequest.Incoming():
		require.Equal(t, primitive.OpCodeResult, rsp.Header.OpCode)
	case <-time.After(10 * time.Second):
		t.Fatalf("test timed out after 10 seconds")
	}

	// 1 second instead of 2 just in case there is a time precision issue
	require.GreaterOrEqual(t, time.Now().Sub(beginTimestamp).Nanoseconds(), (1 * time.Second).Nanoseconds())

	select {
	case <-shutdownComplete:
		t.Fatalf("unexpected shutdown complete before 2nd request is done")
	default:
	}

	reqFrame3 := frame.NewFrame(primitive.ProtocolVersion4, 4, queryMsg1)
	inflightRequest3, err := testClient.Send(reqFrame3)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case rsp := <-inflightRequest3.Incoming():
		require.Equal(t, primitive.OpCodeError, rsp.Header.OpCode)
		_, ok := rsp.Body.Message.(*message.Overloaded)
		require.True(t, ok)
	case <-time.After(15 * time.Second):
		t.Fatalf("test timed out after 15 seconds")
	}

	select {
	case rsp := <-inflightRequest2.Incoming():
		require.Equal(t, primitive.OpCodeResult, rsp.Header.OpCode)
	case <-time.After(15 * time.Second):
		t.Fatalf("test timed out after 15 seconds")
	}

	// 4 seconds instead of 5 just in case there is a time precision issue
	require.GreaterOrEqual(t, time.Now().Sub(beginTimestamp).Nanoseconds(), (4 * time.Second).Nanoseconds())

	select {
	case <-shutdownComplete:
	case <-time.After(10 * time.Second):
		t.Fatalf("test timed out")
	}
}
