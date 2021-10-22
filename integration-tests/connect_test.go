package integration_tests

import (
	"bytes"
	"context"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/integration-tests/client"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/integration-tests/utils"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGoCqlConnect(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetup()
	require.Nil(t, err)
	defer testSetup.Cleanup()

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		t.Log("Unable to connect to proxy session.")
		t.Fatal(err)
	}
	defer proxy.Close()

	iter := proxy.Query("SELECT * FROM fakeks.faketb").Iter()
	result, err := iter.SliceMap()

	if err != nil {
		t.Fatal("query failed:", err)
	}

	require.Equal(t, 0, len(result))

	// simulacron generates fake response metadata when queries aren't primed
	require.Equal(t, "fake", iter.Columns()[0].Name)
}

func TestMaxClientsThreshold(t *testing.T) {
	maxClients := 10
	goCqlConnectionsPerHost := 1
	maxSessions := 5 // each session spawns 2 connections (1 control connection)

	testSetup, err := setup.NewSimulacronTestSetupWithSession(false, false)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	config := setup.NewTestConfig(testSetup.Origin.GetInitialContactPoint(), testSetup.Target.GetInitialContactPoint())
	config.MaxClientsThreshold = maxClients
	proxyInstance, err := setup.NewProxyInstanceWithConfig(config)
	require.Nil(t, err)
	defer proxyInstance.Shutdown()

	for i := 0; i < maxSessions + 1; i++ {
		// Connect to proxy as a "client"
		cluster := utils.NewCluster("127.0.0.1", "", "", 14002)
		cluster.NumConns = goCqlConnectionsPerHost
		session, err := cluster.CreateSession()

		if err != nil {
			if i == maxSessions {
				return
			}
			t.Log("Unable to connect to proxy.")
			t.Fatal(err)
		}
		defer session.Close()
	}

	t.Fatal("Expected failure in last session connection but it was successful.")
}

func TestProtocolVersionNegotiation(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetupWithSession(true, false)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	testClient, err := client.NewTestClient(context.Background(), "127.0.0.1:14002")
	require.True(t, err == nil, "testClient setup failed: %s", err)
	defer testClient.Shutdown()

	startup := message.NewStartup()
	f := frame.NewFrame(primitive.ProtocolVersion5, 0, startup)
	codec := frame.NewCodec()
	buf := bytes.Buffer{}
	codec.EncodeFrame(f, &buf)
	bytes := buf.Bytes()
	bytes[1] = 0
	rsp, err := testClient.SendRawRequest(context.Background(), 0, bytes)
	protocolErr, ok := rsp.Body.Message.(*message.ProtocolError)
	require.True(t, ok)
	require.Equal(t, "Invalid or unsupported protocol version", protocolErr.ErrorMessage)
	require.Equal(t, int16(0), rsp.Header.StreamId)
	require.Equal(t, primitive.ProtocolVersion4, rsp.Header.Version)
}

func TestProtocolVersionUnsupportedByProxy(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetupWithSession(true, false)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	testClient, err := client.NewTestClient(context.Background(), "127.0.0.1:14002")
	require.True(t, err == nil, "testClient setup failed: %s", err)
	defer testClient.Shutdown()

	startup := message.NewStartup()
	f := frame.NewFrame(primitive.ProtocolVersion2, 0, startup)
	codec := frame.NewCodec()
	buf := bytes.Buffer{}
	codec.EncodeFrame(f, &buf)
	encoded := buf.Bytes()
	encoded[0] = byte(primitive.ProtocolVersion(1))
	rsp, err := testClient.SendRawRequest(context.Background(), 0, encoded)
	protocolErr, ok := rsp.Body.Message.(*message.ProtocolError)
	require.True(t, ok)
	require.Equal(t, "Invalid or unsupported protocol version (1)", protocolErr.ErrorMessage)
	require.Equal(t, int16(0), rsp.Header.StreamId)
	require.Equal(t, primitive.ProtocolVersion4, rsp.Header.Version)
}