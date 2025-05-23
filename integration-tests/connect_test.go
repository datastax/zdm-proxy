package integration_tests

import (
	"bufio"
	"bytes"
	"context"
	cqlClient "github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/client"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/simulacron"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/rs/zerolog"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

func TestGoCqlConnect(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetup(t)
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

func TestCannotConnectWithoutControlConnection(t *testing.T) {
	c := setup.NewTestConfig("", "")
	testSetup, err := setup.NewSimulacronTestSetupWithSessionAndNodesAndConfig(t, true, false, 1, c, nil)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	// try to force a scenario where the control connection is reconnecting (cqlConn is nil)
	// when a new client handler is being created
	// controlConn.Open() triggers a cqlConn.Close() on the existing connection (and making it nil) before it opens a new one
	go func() {
		_, err := testSetup.Proxy.GetOriginControlConn().Open(false, context.Background())
		if err != nil {
			t.Logf("err while opening cc in the background: %v", err)
		}
	}()

	for i := 0; i < 1000; i++ {
		// connect to proxy as a "client"
		client := cqlClient.NewCqlClient("127.0.0.1:14002", nil)
		conn, err := client.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 0)
		require.Nil(t, err)
		_ = conn.Close()
	}
}

// Simulacron-based test to make sure that we can handle invalid protocol error and downgrade
// used protocol on control connection. ORIGIN and TARGET are using the same C* version
func TestControlConnectionProtocolVersionNegotiation(t *testing.T) {
	tests := []struct {
		name                          string
		clusterVersion                string
		controlConnMaxProtocolVersion string
		negotiatedProtocolVersion     primitive.ProtocolVersion
	}{
		{
			name:                          "Cluster2.1_MaxCCProtoVer4_NegotiatedProtoVer3",
			clusterVersion:                "2.1",
			controlConnMaxProtocolVersion: "4",
			negotiatedProtocolVersion:     primitive.ProtocolVersion3, // protocol downgraded to V3, V4 is not supported
		},
		{
			name:                          "Cluster3.0_MaxCCProtoVer4_NegotiatedProtoVer4",
			clusterVersion:                "3.0",
			controlConnMaxProtocolVersion: "4",
			negotiatedProtocolVersion:     primitive.ProtocolVersion4, // make sure that protocol negotiation does not fail if it is not actually needed
		},
		{
			name:                          "Cluster3.0_MaxCCProtoVer3_NegotiatedProtoVer3",
			clusterVersion:                "3.0",
			controlConnMaxProtocolVersion: "3",
			negotiatedProtocolVersion:     primitive.ProtocolVersion3, // protocol V3 applied as it is the maximum configured
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := setup.NewTestConfig("", "")
			c.ControlConnMaxProtocolVersion = tt.controlConnMaxProtocolVersion
			testSetup, err := setup.NewSimulacronTestSetupWithSessionAndNodesAndConfig(t, true, false, 1, c,
				&simulacron.ClusterVersion{tt.clusterVersion, tt.clusterVersion})
			require.Nil(t, err)
			defer testSetup.Cleanup()

			query := "SELECT * FROM test"
			expectedRows := simulacron.NewRowsResult(
				map[string]simulacron.DataType{
					"company": simulacron.DataTypeText,
				}).WithRow(map[string]interface{}{
				"company": "TBD",
			})

			err = testSetup.Origin.Prime(simulacron.WhenQuery(
				query,
				simulacron.NewWhenQueryOptions()).
				ThenRowsSuccess(expectedRows))
			require.Nil(t, err)

			// Connect to proxy as a "client"
			client := cqlClient.NewCqlClient("127.0.0.1:14002", nil)
			cqlClientConn, err := client.ConnectAndInit(context.Background(), tt.negotiatedProtocolVersion, 0)
			require.Nil(t, err)
			defer cqlClientConn.Close()

			cqlConn, _ := testSetup.Proxy.GetOriginControlConn().GetConnAndContactPoint()
			negotiatedProto := cqlConn.GetProtocolVersion()
			require.Equal(t, tt.negotiatedProtocolVersion, negotiatedProto)

			queryMsg := &message.Query{
				Query:   "SELECT * FROM test",
				Options: &message.QueryOptions{Consistency: primitive.ConsistencyLevelOne},
			}
			rsp, err := cqlClientConn.SendAndReceive(frame.NewFrame(primitive.ProtocolVersion3, 0, queryMsg))
			if err != nil {
				t.Fatal("query failed:", err)
			}

			require.Equal(t, 1, len(rsp.Body.Message.(*message.RowsResult).Data))
		})
	}
}

func TestMaxClientsThreshold(t *testing.T) {
	maxClients := 10
	goCqlConnectionsPerHost := 1
	maxSessions := 5 // each session spawns 2 connections (1 control connection)

	testSetup, err := setup.NewSimulacronTestSetupWithSession(t, false, false)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	config := setup.NewTestConfig(testSetup.Origin.GetInitialContactPoint(), testSetup.Target.GetInitialContactPoint())
	config.ProxyMaxClientConnections = maxClients
	proxyInstance, err := setup.NewProxyInstanceWithConfig(config)
	require.Nil(t, err)
	defer proxyInstance.Shutdown()

	for i := 0; i < maxSessions+1; i++ {
		// Connect to proxy as a "client"
		cluster := utils.NewCluster("127.0.0.1", "", "", 14002)
		cluster.NumConns = goCqlConnectionsPerHost
		cluster.ProtoVersion = 4 // prevent temporary connection for proto version discovery
		session, err := cluster.CreateSession()

		if err != nil {
			if i == maxSessions {
				return
			}
			require.FailNow(t, "Unable to connect to proxy: %v.", err)
		}
		//goland:noinspection GoDeferInLoop
		defer session.Close()
	}

	require.FailNow(t, "Expected failure in last session connection but it was successful.")
}

func TestRequestedProtocolVersionUnsupportedByProxy(t *testing.T) {
	tests := []struct {
		name              string
		requestVersion    primitive.ProtocolVersion
		negotiatedVersion string
		expectedVersion   primitive.ProtocolVersion
		errExpected       string
	}{
		{
			"request v5, response v4",
			primitive.ProtocolVersion5,
			"4",
			primitive.ProtocolVersion4,
			"Invalid or unsupported protocol version (5)",
		},
		{
			"request v1, response v4",
			primitive.ProtocolVersion(0x1),
			"4",
			primitive.ProtocolVersion4,
			"Invalid or unsupported protocol version (1)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			oldLevel := log.GetLevel()
			oldZeroLogLevel := zerolog.GlobalLevel()
			log.SetLevel(log.TraceLevel)
			defer log.SetLevel(oldLevel)
			zerolog.SetGlobalLevel(zerolog.TraceLevel)
			defer zerolog.SetGlobalLevel(oldZeroLogLevel)

			cfg := setup.NewTestConfig("127.0.1.1", "127.0.1.2")
			cfg.ControlConnMaxProtocolVersion = test.negotiatedVersion
			cfg.LogLevel = "TRACE" // saw 1 test failure here once but logs didn't show enough info
			testSetup, err := setup.NewCqlServerTestSetup(t, cfg, false, false, false)
			require.Nil(t, err)
			defer testSetup.Cleanup()

			testSetup.Origin.CqlServer.RequestHandlers = []cqlClient.RequestHandler{cqlClient.NewDriverConnectionInitializationHandler("origin", "dc1", func(_ string) {})}
			testSetup.Target.CqlServer.RequestHandlers = []cqlClient.RequestHandler{cqlClient.NewDriverConnectionInitializationHandler("target", "dc1", func(_ string) {})}

			err = testSetup.Start(cfg, false, primitive.ProtocolVersion3)
			require.Nil(t, err)

			testClient, err := client.NewTestClient(context.Background(), "127.0.0.1:14002")
			require.Nil(t, err)

			encodedFrame, err := createFrameWithUnsupportedVersion(test.requestVersion, 0, false)
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
	type test struct {
		name              string
		requestVersion    primitive.ProtocolVersion
		negotiatedVersion string
		returnedVersion   primitive.ProtocolVersion
		expectedVersion   primitive.ProtocolVersion
		errExpected       string
	}
	tests := []*test{
		{
			"DSE_V2 request, v5 returned, v4 expected",
			primitive.ProtocolVersionDse2,
			"4",
			primitive.ProtocolVersion5,
			primitive.ProtocolVersion4,
			"Invalid or unsupported protocol version (5)",
		},
		{
			"DSE_V2 request, v1 returned, v4 expected",
			primitive.ProtocolVersionDse2,
			"4",
			primitive.ProtocolVersion(0x01),
			primitive.ProtocolVersion4,
			"Invalid or unsupported protocol version (1)",
		},
	}

	runTestFunc := func(t *testing.T, test *test, cfg *config.Config) {
		cfg.ControlConnMaxProtocolVersion = test.negotiatedVersion // simulate what version was negotiated on control connection
		testSetup, err := setup.NewCqlServerTestSetup(t, cfg, false, false, false)
		require.Nil(t, err)
		defer testSetup.Cleanup()

		enableHandlers := atomic.Value{}
		enableHandlers.Store(false)

		rawHandler := func(request *frame.Frame, conn *cqlClient.CqlServerConnection, ctx cqlClient.RequestHandlerContext) (response []byte) {
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

		testSetup.Origin.CqlServer.RequestRawHandlers = []cqlClient.RawRequestHandler{rawHandler}
		testSetup.Target.CqlServer.RequestRawHandlers = []cqlClient.RawRequestHandler{rawHandler}

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
				cfg.ReadMode = config.ReadModePrimaryOnly
				runTestFunc(t, test, cfg)
			})
			t.Run("async reads", func(t *testing.T) {
				cfg := setup.NewTestConfig("127.0.1.1", "127.0.1.2")
				cfg.ReadMode = config.ReadModeDualAsyncOnSecondary
				runTestFunc(t, test, cfg)
			})
		})
	}
}

func createFrameWithUnsupportedVersion(version primitive.ProtocolVersion, streamId int16, isResponse bool) ([]byte, error) {
	mostSimilarVersion := version
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

func TestHandlingOfInternalHeartbeat(t *testing.T) {
	oldLevel := log.GetLevel()
	log.SetLevel(log.TraceLevel)
	defer log.SetLevel(oldLevel)

	var buff bytes.Buffer
	buffWriter := bufio.NewWriter(&buff)
	log.SetOutput(buffWriter)

	c := setup.NewTestConfig("", "")
	c.HeartbeatIntervalMs = 500
	testSetup, err := setup.NewSimulacronTestSetupWithSessionAndNodesAndConfig(t, true, false, 1, c, nil)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	query := "SELECT * FROM test"
	expectedRows := simulacron.NewRowsResult(
		map[string]simulacron.DataType{
			"company": simulacron.DataTypeText,
		}).WithRow(map[string]interface{}{
		"company": "TBD",
	})

	err = testSetup.Origin.Prime(simulacron.WhenQuery(
		query,
		simulacron.NewWhenQueryOptions()).
		ThenRowsSuccess(expectedRows))
	require.Nil(t, err)

	// Connect to proxy as a "client"
	proxyClient := cqlClient.NewCqlClient("127.0.0.1:14002", nil)
	cqlClientConn, err := proxyClient.ConnectAndInit(context.Background(), primitive.ProtocolVersion4, 0)
	require.Nil(t, err)
	defer cqlClientConn.Close()

	queryMsg := &message.Query{
		Query:   query,
		Options: nil,
	}

	_, err = cqlClientConn.SendAndReceive(frame.NewFrame(primitive.ProtocolVersion4, 0, queryMsg))
	require.Nil(t, err)

	// sleep longer than heartbeat interval
	time.Sleep(550 * time.Millisecond)

	_, err = cqlClientConn.SendAndReceive(frame.NewFrame(primitive.ProtocolVersion4, 0, queryMsg))
	require.Nil(t, err)

	err = buffWriter.Flush()
	require.Nil(t, err)
	allLogs := buff.String()
	require.Contains(t, allLogs, "Sending heartbeat to cluster TARGET")
	require.Contains(t, allLogs, "Received internal response from TARGET")
	// below logs would indicate we have issues sending heartbeat messages or handling response
	require.NotContains(t, allLogs, "negative stream id")
	require.NotContains(t, allLogs, "Could not find request context for stream id")
}
