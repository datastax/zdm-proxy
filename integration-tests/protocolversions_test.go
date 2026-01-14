package integration_tests

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"

	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/datastax/zdm-proxy/integration-tests/setup"
)

// Test that proxy can establish connectivity with ORIGIN and TARGET
// clusters that support different set of protocol versions. Verify also that
// client driver can connect and successfully insert or query data.
func TestProtocolNegotiationDifferentClusters(t *testing.T) {
	tests := []struct {
		name                   string
		proxyMaxProtoVer       string
		proxyOriginContConnVer primitive.ProtocolVersion
		proxyTargetContConnVer primitive.ProtocolVersion
		originProtoVer         []primitive.ProtocolVersion
		targetProtoVer         []primitive.ProtocolVersion
		clientProtoVer         primitive.ProtocolVersion
		failClientConnect      bool
		failProxyStartup       bool
	}{
		{
			name:                   "OriginV2_TargetV2_ClientV2",
			proxyMaxProtoVer:       "",
			proxyOriginContConnVer: primitive.ProtocolVersion2,
			proxyTargetContConnVer: primitive.ProtocolVersion2,
			originProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion2},
			targetProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion2},
			clientProtoVer:         primitive.ProtocolVersion2,
		},
		{
			name:                   "OriginV23_TargetV345_ClientV3",
			proxyMaxProtoVer:       "",
			proxyOriginContConnVer: primitive.ProtocolVersion3,
			proxyTargetContConnVer: primitive.ProtocolVersion5,
			originProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion2, primitive.ProtocolVersion3},
			targetProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4, primitive.ProtocolVersion5},
			clientProtoVer:         primitive.ProtocolVersion3,
		},
		{
			name:                   "OriginV2_TargetV2_ClientV2_ProxyControlConnNegotiation",
			proxyMaxProtoVer:       "",
			proxyOriginContConnVer: primitive.ProtocolVersion2,
			proxyTargetContConnVer: primitive.ProtocolVersion2,
			originProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion2},
			targetProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion2},
			clientProtoVer:         primitive.ProtocolVersion2,
		},
		{
			name:                   "OriginV2_TargetV23_ClientV2",
			proxyMaxProtoVer:       "",
			proxyOriginContConnVer: primitive.ProtocolVersion2,
			proxyTargetContConnVer: primitive.ProtocolVersion3,
			originProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion2},
			targetProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion2, primitive.ProtocolVersion3},
			clientProtoVer:         primitive.ProtocolVersion2,
		},
		{
			name:                   "OriginV23_TargetV2_ClientV2",
			proxyMaxProtoVer:       "",
			proxyOriginContConnVer: primitive.ProtocolVersion3,
			proxyTargetContConnVer: primitive.ProtocolVersion2,
			originProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion2, primitive.ProtocolVersion3},
			targetProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion2},
			clientProtoVer:         primitive.ProtocolVersion2,
		},
		{
			// most common setup with OSS Cassandra
			name:                   "OriginV345_TargetV345_ClientV4",
			proxyMaxProtoVer:       "",
			proxyOriginContConnVer: primitive.ProtocolVersion5,
			proxyTargetContConnVer: primitive.ProtocolVersion5,
			originProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4, primitive.ProtocolVersion5},
			targetProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4, primitive.ProtocolVersion5},
			clientProtoVer:         primitive.ProtocolVersion4,
		},
		{
			name:                   "OriginV345_TargetV345_ClientV5",
			proxyMaxProtoVer:       "",
			proxyOriginContConnVer: primitive.ProtocolVersion5,
			proxyTargetContConnVer: primitive.ProtocolVersion5,
			originProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4, primitive.ProtocolVersion5},
			targetProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4, primitive.ProtocolVersion5},
			clientProtoVer:         primitive.ProtocolVersion5,
		},
		{
			// most common setup with DSE
			name:                   "OriginV345_TargetV34Dse1Dse2_ClientV4",
			proxyMaxProtoVer:       "",
			proxyOriginContConnVer: primitive.ProtocolVersion5,
			proxyTargetContConnVer: primitive.ProtocolVersionDse2,
			originProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4, primitive.ProtocolVersion5},
			targetProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4, primitive.ProtocolVersionDse1, primitive.ProtocolVersionDse2},
			clientProtoVer:         primitive.ProtocolVersion4,
		},
		{
			name:                   "OriginV234Dse1Dse2_TargetV345_ClientV4",
			proxyMaxProtoVer:       "",
			proxyOriginContConnVer: primitive.ProtocolVersionDse2,
			proxyTargetContConnVer: primitive.ProtocolVersion5,
			originProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion2, primitive.ProtocolVersion3, primitive.ProtocolVersion4, primitive.ProtocolVersionDse1, primitive.ProtocolVersionDse2},
			targetProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4, primitive.ProtocolVersion5},
			clientProtoVer:         primitive.ProtocolVersion4,
		},
		{
			name:                   "OriginV2_TargetV345_FailClient",
			proxyMaxProtoVer:       "",
			proxyOriginContConnVer: primitive.ProtocolVersion2,
			proxyTargetContConnVer: primitive.ProtocolVersion5,
			originProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion2},
			targetProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion3, primitive.ProtocolVersion4, primitive.ProtocolVersion5},
			clientProtoVer:         primitive.ProtocolVersion2,
			// client connection should fail as there is no common protocol version between origin and target
			failClientConnect: true,
		}, {
			name:                   "OriginV3_TargetV3_Too_Low_Proto_Configured",
			proxyMaxProtoVer:       "2",
			proxyOriginContConnVer: primitive.ProtocolVersion3,
			proxyTargetContConnVer: primitive.ProtocolVersion3,
			originProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion3},
			targetProtoVer:         []primitive.ProtocolVersion{primitive.ProtocolVersion3},
			clientProtoVer:         primitive.ProtocolVersion2,
			// fail proxy control connection, because configured protocol version is too low
			failProxyStartup: true,
		},
	}

	originAddress := "127.0.1.1"
	targetAddress := "127.0.1.2"
	serverConf := setup.NewTestConfig(originAddress, targetAddress)
	proxyConf := setup.NewTestConfig(originAddress, targetAddress)
	log.SetLevel(log.TraceLevel)

	queryInsert := &message.Query{
		Query: "INSERT INTO test_ks.test(key, value) VALUES(1, '1')", // use INSERT to route request to both clusters
	}
	querySelect := &message.Query{
		Query: "SELECT * FROM test_ks.test",
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.proxyMaxProtoVer != "" {
				proxyConf.ControlConnMaxProtocolVersion = test.proxyMaxProtoVer
			}

			testSetup, err := setup.NewCqlServerTestSetup(t, serverConf, false, false, false)
			require.Nil(t, err)
			defer testSetup.Cleanup()

			originRequestHandler := NewProtocolNegotiationRequestHandler("origin", "dc1", originAddress, test.originProtoVer)
			targetRequestHandler := NewProtocolNegotiationRequestHandler("target", "dc1", targetAddress, test.targetProtoVer)

			testSetup.Origin.CqlServer.RequestHandlers = []client.RequestHandler{
				originRequestHandler.HandleRequest,
				client.NewDriverConnectionInitializationHandler("origin", "dc1", func(_ string) {}),
			}
			testSetup.Target.CqlServer.RequestHandlers = []client.RequestHandler{
				targetRequestHandler.HandleRequest,
				client.NewDriverConnectionInitializationHandler("target", "dc1", func(_ string) {}),
			}

			err = testSetup.Start(nil, false, test.clientProtoVer)
			require.Nil(t, err)

			proxy, err := setup.NewProxyInstanceWithConfig(proxyConf) // starts the proxy
			if proxy != nil {
				defer proxy.Shutdown()
			}
			if test.failProxyStartup {
				require.NotNil(t, err)
				return
			} else {
				require.Nil(t, err)
			}

			cqlConn, err := testSetup.Client.CqlClient.ConnectAndInit(context.Background(), test.clientProtoVer, 0)
			if test.failClientConnect {
				require.NotNil(t, err)
				return
			}
			require.Nil(t, err)
			defer cqlConn.Close()

			response, err := cqlConn.SendAndReceive(frame.NewFrame(test.clientProtoVer, 0, queryInsert))
			require.Nil(t, err)
			require.IsType(t, &message.VoidResult{}, response.Body.Message)

			response, err = cqlConn.SendAndReceive(frame.NewFrame(test.clientProtoVer, 0, querySelect))
			require.Nil(t, err)
			resultSet := response.Body.Message.(*message.RowsResult).Data
			require.Equal(t, 1, len(resultSet))

			proxyCqlConn, _ := proxy.GetOriginControlConn().GetConnAndContactPoint()
			require.Equal(t, test.proxyOriginContConnVer, proxyCqlConn.GetProtocolVersion())
			proxyCqlConn, _ = proxy.GetTargetControlConn().GetConnAndContactPoint()
			require.Equal(t, test.proxyTargetContConnVer, proxyCqlConn.GetProtocolVersion())
		})
	}
}

// Test that proxy blocks protocol versions when configured to do so
func TestProtocolNegotiationBlockedVersions(t *testing.T) {
	tests := []struct {
		name              string
		clusterProtoVers  []primitive.ProtocolVersion
		blockedProtoVers  string
		clientProtoVer    primitive.ProtocolVersion
		failClientConnect bool
	}{
		{
			name:              "ClusterV2_BlockedV2_ClientFail",
			clusterProtoVers:  []primitive.ProtocolVersion{primitive.ProtocolVersion2},
			blockedProtoVers:  "v2",
			failClientConnect: true,
		},
		{
			name:             "ClusterV2V3V4_BlockedV2_ClientV4",
			clusterProtoVers: []primitive.ProtocolVersion{0x2, 0x3, 0x4},
			blockedProtoVers: "v2",
			clientProtoVer:   0x4,
		},
		{
			name:             "ClusterV2V3V4V5_BlockedV5_ClientV4",
			clusterProtoVers: []primitive.ProtocolVersion{0x2, 0x3, 0x4, 0x5},
			blockedProtoVers: "v5",
			clientProtoVer:   0x4,
		},
		{
			name:             "ClusterV2V3V4V5_BlockedV4V5_ClientV3",
			clusterProtoVers: []primitive.ProtocolVersion{0x2, 0x3, 0x4, 0x5},
			blockedProtoVers: "v4,v5",
			clientProtoVer:   0x3,
		},
		{
			name:              "ClusterV2V3V4V5_BlockedV2V3V4V5_ClientFail",
			clusterProtoVers:  []primitive.ProtocolVersion{0x2, 0x3, 0x4, 0x5},
			blockedProtoVers:  "2,3,4,5",
			failClientConnect: true,
		},
		{
			name:             "ClusterV2V3V4DseV1DseV2_BlockedV4V5DseV1_ClientDseV2",
			clusterProtoVers: []primitive.ProtocolVersion{0x2, 0x3, 0x4, primitive.ProtocolVersionDse1, primitive.ProtocolVersionDse2},
			blockedProtoVers: "V4,V5,DseV1",
			clientProtoVer:   primitive.ProtocolVersionDse2,
		},
		{
			name:             "ClusterV2V3V4DseV1_BlockedV5_ClientDseV1",
			clusterProtoVers: []primitive.ProtocolVersion{0x2, 0x3, 0x4, primitive.ProtocolVersionDse1},
			blockedProtoVers: "V5",
			clientProtoVer:   primitive.ProtocolVersionDse1,
		},
		{
			name:             "ClusterV2V3V4DseV1_BlockedDseV1_ClientV4",
			clusterProtoVers: []primitive.ProtocolVersion{0x2, 0x3, 0x4, primitive.ProtocolVersionDse1},
			blockedProtoVers: "dsev1",
			clientProtoVer:   0x4,
		},
	}

	originAddress := "127.0.1.1"
	targetAddress := "127.0.1.2"
	serverConf := setup.NewTestConfig(originAddress, targetAddress)
	proxyConf := setup.NewTestConfig(originAddress, targetAddress)
	log.SetLevel(log.TraceLevel)

	queryInsert := &message.Query{
		Query: "INSERT INTO test_ks.test(key, value) VALUES(1, '1')", // use INSERT to route request to both clusters
	}
	querySelect := &message.Query{
		Query: "SELECT * FROM test_ks.test",
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proxyConf.BlockedProtocolVersions = test.blockedProtoVers

			testSetup, err := setup.NewCqlServerTestSetup(t, serverConf, false, false, false)
			require.Nil(t, err)
			defer testSetup.Cleanup()

			originRequestHandler := NewProtocolNegotiationRequestHandler("origin", "dc1", originAddress, test.clusterProtoVers)
			targetRequestHandler := NewProtocolNegotiationRequestHandler("target", "dc1", targetAddress, test.clusterProtoVers)

			testSetup.Origin.CqlServer.RequestHandlers = []client.RequestHandler{
				originRequestHandler.HandleRequest,
				client.NewDriverConnectionInitializationHandler("origin", "dc1", func(_ string) {}),
			}
			testSetup.Target.CqlServer.RequestHandlers = []client.RequestHandler{
				targetRequestHandler.HandleRequest,
				client.NewDriverConnectionInitializationHandler("target", "dc1", func(_ string) {}),
			}

			err = testSetup.Start(nil, false, 0)
			require.Nil(t, err)

			proxy, err := setup.NewProxyInstanceWithConfig(proxyConf) // starts the proxy
			if proxy != nil {
				defer proxy.Shutdown()
			}
			require.Nil(t, err)

			cqlConn, clientProtoVer, err := connectWithNegotiation(testSetup.Client.CqlClient, context.Background())
			if cqlConn != nil {
				defer cqlConn.Close()
			}
			if test.failClientConnect {
				require.NotNil(t, err)
				return
			}
			require.Nil(t, err)

			require.Equal(t, test.clientProtoVer, clientProtoVer)

			response, err := cqlConn.SendAndReceive(frame.NewFrame(test.clientProtoVer, 0, queryInsert))
			require.Nil(t, err)
			require.IsType(t, &message.VoidResult{}, response.Body.Message)

			response, err = cqlConn.SendAndReceive(frame.NewFrame(test.clientProtoVer, 0, querySelect))
			require.Nil(t, err)
			resultSet := response.Body.Message.(*message.RowsResult).Data
			require.Equal(t, 1, len(resultSet))
		})
	}
}

func connectWithNegotiation(cqlClient *client.CqlClient, ctx context.Context) (*client.CqlClientConnection, primitive.ProtocolVersion, error) {
	orderedProtoVersions := []primitive.ProtocolVersion{
		primitive.ProtocolVersionDse2, primitive.ProtocolVersionDse1, primitive.ProtocolVersion5,
		primitive.ProtocolVersion4, primitive.ProtocolVersion3, primitive.ProtocolVersion2}

	for _, protoVersion := range orderedProtoVersions {
		conn, err := cqlClient.ConnectAndInit(ctx, protoVersion, 0)
		if err != nil {
			if conn != nil {
				conn.Close()
			}
			if strings.Contains(strings.ToLower(err.Error()), "handler closed") {
				continue
			}
			return nil, 0, fmt.Errorf("negotiate error: %w", err)
		}
		return conn, protoVersion, nil
	}
	return nil, 0, errors.New("all protocol versions failed")
}

type ProtocolNegotiationRequestHandler struct {
	cluster          string
	datacenter       string
	peerIP           string
	protocolVersions []primitive.ProtocolVersion // accepted protocol versions
	// store negotiated protocol versions by socket port number
	// protocol version negotiated by proxy on control connections can be different from the one
	// used by client driver with ORIGIN and TARGET nodes. In the scenario 'OriginV2_TargetV23_ClientV2', proxy
	// will establish control connection with ORIGIN using version 2, and TARGET with version 3.
	// Protocol version applied on client connections with TARGET will be different - V2.
	negotiatedProtoVer map[int]primitive.ProtocolVersion // negotiated protocol version on different sockets
}

func NewProtocolNegotiationRequestHandler(cluster string, datacenter string, peerIP string,
	protocolVersion []primitive.ProtocolVersion) *ProtocolNegotiationRequestHandler {
	return &ProtocolNegotiationRequestHandler{
		cluster:            cluster,
		datacenter:         datacenter,
		peerIP:             peerIP,
		protocolVersions:   protocolVersion,
		negotiatedProtoVer: make(map[int]primitive.ProtocolVersion),
	}
}

func (recv *ProtocolNegotiationRequestHandler) HandleRequest(
	request *frame.Frame,
	conn *client.CqlServerConnection,
	ctx client.RequestHandlerContext) (response *frame.Frame) {
	port := conn.RemoteAddr().(*net.TCPAddr).Port
	negotiatedProtoVer := recv.negotiatedProtoVer[port]
	if !slices.Contains(recv.protocolVersions, request.Header.Version) || (negotiatedProtoVer != 0 && negotiatedProtoVer != request.Header.Version) {
		// server does not support given protocol version, or it was not the one negotiated
		return frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.ProtocolError{
			ErrorMessage: fmt.Sprintf("Invalid or unsupported protocol version (%d)", request.Header.Version),
		})
	}
	switch request.Body.Message.GetOpCode() {
	case primitive.OpCodeStartup:
		recv.negotiatedProtoVer[port] = request.Header.Version
		return frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.Ready{})
	case primitive.OpCodeRegister:
		return frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.Ready{})
	case primitive.OpCodeQuery:
		query := request.Body.Message.(*message.Query)
		switch query.Query {
		case "SELECT * FROM system.local":
			// C* 2.0.0 does not store local endpoint details in system.local table
			sysLocRow := systemLocalRow(recv.cluster, recv.datacenter, "Murmur3Partitioner", nil, request.Header.Version)
			metadata := &message.RowsMetadata{
				ColumnCount: int32(len(systemLocalColumns)),
				Columns:     systemLocalColumns,
			}
			if negotiatedProtoVer == primitive.ProtocolVersion2 {
				metadata = &message.RowsMetadata{
					ColumnCount: int32(len(systemLocalColumnsProtocolV2)),
					Columns:     systemLocalColumnsProtocolV2,
				}
			}
			sysLocMsg := &message.RowsResult{
				Metadata: metadata,
				Data:     message.RowSet{sysLocRow},
			}
			return frame.NewFrame(request.Header.Version, request.Header.StreamId, sysLocMsg)
		case "SELECT * FROM system.peers":
			var sysPeerRows message.RowSet
			if len(recv.peerIP) > 0 {
				sysPeerRows = append(sysPeerRows, systemPeersRow(
					recv.datacenter,
					&net.TCPAddr{IP: net.ParseIP(recv.peerIP), Port: 9042},
					negotiatedProtoVer,
				))
			}
			sysPeeMsg := &message.RowsResult{
				Metadata: &message.RowsMetadata{
					ColumnCount: int32(len(systemPeersColumns)),
					Columns:     systemPeersColumns,
				},
				Data: sysPeerRows,
			}
			return frame.NewFrame(request.Header.Version, request.Header.StreamId, sysPeeMsg)
		case "SELECT * FROM test_ks.test":
			qryMsg := &message.RowsResult{
				Metadata: &message.RowsMetadata{
					ColumnCount: 2,
					Columns: []*message.ColumnMetadata{
						{Keyspace: "test_ks", Table: "test", Name: "key", Type: datatype.Varchar},
						{Keyspace: "test_ks", Table: "test", Name: "value", Type: datatype.Uuid},
					},
				},
				Data: message.RowSet{
					message.Row{keyValue, hostIdValue},
				},
			}
			return frame.NewFrame(request.Header.Version, request.Header.StreamId, qryMsg)
		case "INSERT INTO test_ks.test(key, value) VALUES(1, '1')":
			return frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.VoidResult{})
		}
	}
	return nil
}
