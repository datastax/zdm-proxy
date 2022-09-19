package integration_tests

import (
	"bytes"
	"context"
	client2 "github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/client"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/stretchr/testify/require"
	"sync/atomic"
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
	config.ProxyMaxClientConnections = maxClients
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

func TestRequestedProtocolVersionUnsupportedByProxy(t *testing.T) {
	tests := []struct{
		name            string
		requestVersion  primitive.ProtocolVersion
		expectedVersion primitive.ProtocolVersion
		errExpected     string
	}{
		{
			"request v5, response v4",
			primitive.ProtocolVersion5,
			primitive.ProtocolVersion4,
			"Invalid or unsupported protocol version (5)",
		},
		{
			"request v1, response v4",
			primitive.ProtocolVersion(0x1),
			primitive.ProtocolVersion4,
			"Invalid or unsupported protocol version (1)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := setup.NewTestConfig("127.0.1.1", "127.0.1.2")
			testSetup, err := setup.NewCqlServerTestSetup(cfg, false, false, false)
			require.Nil(t, err)
			defer testSetup.Cleanup()

			testSetup.Origin.CqlServer.RequestHandlers = []client2.RequestHandler{client2.NewDriverConnectionInitializationHandler("origin", "dc1", func(_ string) {})}
			testSetup.Target.CqlServer.RequestHandlers = []client2.RequestHandler{client2.NewDriverConnectionInitializationHandler("target", "dc1", func(_ string) {})}

			err = testSetup.Start(cfg, false, primitive.ProtocolVersion3)
			require.Nil(t, err)

			testClient, err := client.NewTestClient(context.Background(), "127.0.0.1:14002")
			require.Nil(t, err)

			encodedFrame, err := createFrameWithUnsupportedVersion(test.requestVersion, 0,false)
			require.Nil(t, err)
			rsp, err := testClient.SendRawRequest(context.Background(), 0, encodedFrame)
			require.Nil(t, err)
			protocolErr, ok := rsp.Body.Message.(*message.ProtocolError)
			require.True(t, ok)
			require.Equal(t, test.errExpected, protocolErr.ErrorMessage)
			require.Equal(t, int16(0), rsp.Header.StreamId)
			require.Equal(t, test.expectedVersion, rsp.Header.Version)
		})
	}
}

func TestReturnedProtocolVersionUnsupportedByProxy(t *testing.T) {
	type test struct{
		name            string
		requestVersion  primitive.ProtocolVersion
		returnedVersion primitive.ProtocolVersion
		expectedVersion primitive.ProtocolVersion
		errExpected     string
	}
	tests := []*test{
		{
			"DSE_V2 request, v5 returned, v4 expected",
			primitive.ProtocolVersionDse2,
			primitive.ProtocolVersion5,
			primitive.ProtocolVersion4,
			"Invalid or unsupported protocol version (5)",
		},
		{
			"DSE_V2 request, v1 returned, v4 expected",
			primitive.ProtocolVersionDse2,
			primitive.ProtocolVersion(0x01),
			primitive.ProtocolVersion4,
			"Invalid or unsupported protocol version (1)",
		},
	}

	runTestFunc := func(t *testing.T, test *test, cfg *config.Config ) {
		testSetup, err := setup.NewCqlServerTestSetup(cfg, false, false, false)
		require.Nil(t, err)
		defer testSetup.Cleanup()

		enableHandlers := atomic.Value{}
		enableHandlers.Store(false)

		rawHandler := func(request *frame.Frame, conn *client2.CqlServerConnection, ctx client2.RequestHandlerContext) (response []byte) {
			if enableHandlers.Load().(bool) && request.Header.Version == test.requestVersion {
				encodedFrame, err := createFrameWithUnsupportedVersion(test.returnedVersion, request.Header.StreamId, true)
				if err != nil {
					t.Logf("failed to encode response: %v", err)
				} else {
					return encodedFrame
				}
			}
			return nil
		}

		testSetup.Origin.CqlServer.RequestRawHandlers = []client2.RawRequestHandler{rawHandler}
		testSetup.Target.CqlServer.RequestRawHandlers = []client2.RawRequestHandler{rawHandler}

		err = testSetup.Start(cfg, false, primitive.ProtocolVersion4)
		require.Nil(t, err)

		testClient, err := client.NewTestClient(context.Background(), "127.0.0.1:14002")
		require.Nil(t, err)

		enableHandlers.Store(true)

		request := frame.NewFrame(test.requestVersion, 0, message.NewStartup())
		rsp, _, err := testClient.SendRequest(context.Background(), request)
		require.Nil(t, err)
		protocolErr, ok := rsp.Body.Message.(*message.ProtocolError)
		require.True(t, ok)
		require.Equal(t, test.errExpected, protocolErr.ErrorMessage)
		require.Equal(t, int16(0), rsp.Header.StreamId)
		require.Equal(t, test.expectedVersion, rsp.Header.Version)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run("no async reads", func(t *testing.T) {
				cfg := setup.NewTestConfig("127.0.1.1", "127.0.1.2")
				cfg.DualReadsEnabled = false
				cfg.AsyncReadsOnSecondary = false
				runTestFunc(t, test, cfg)
			})
			t.Run("async reads", func(t *testing.T) {
				cfg := setup.NewTestConfig("127.0.1.1", "127.0.1.2")
				cfg.DualReadsEnabled = true
				cfg.AsyncReadsOnSecondary = true
				runTestFunc(t, test, cfg)
			})
		})
	}
}

func createFrameWithUnsupportedVersion(version primitive.ProtocolVersion, streamId int16, isResponse bool) ([]byte, error) {
	mostSimilarVersion := primitive.ProtocolVersion4
	if version > primitive.ProtocolVersionDse2 {
		mostSimilarVersion = primitive.ProtocolVersionDse2
	} else if version < primitive.ProtocolVersion2 {
		mostSimilarVersion = primitive.ProtocolVersion2
	}

	var msg message.Message
	if isResponse {
		msg = &message.Ready{}
	} else {
		msg = message.NewStartup()
	}
	f := frame.NewFrame(mostSimilarVersion, streamId, msg)
	codec := frame.NewCodec()
	buf := bytes.Buffer{}
	err := codec.EncodeFrame(f, &buf)
	if err != nil {
		return nil, err
	}
	encoded := buf.Bytes()
	encoded[0] = byte(version)
	if isResponse {
		encoded[0] |= 0b1000_0000
	}
	encoded[1] = 0
	return encoded, nil
}