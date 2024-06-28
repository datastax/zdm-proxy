package integration_tests

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
)

func TestProtocolV2Connect(t *testing.T) {
	originAddress := "127.0.0.2"
	targetAddress := "127.0.0.3"

	serverConf := setup.NewTestConfig(originAddress, targetAddress)
	proxyConf := setup.NewTestConfig(originAddress, targetAddress)
	proxyConf.ControlConnMaxProtocolVersion = "3" // simulate protocol downgrade to V2

	testSetup, err := setup.NewCqlServerTestSetup(t, serverConf, false, false, false)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	originRequestHandler := NewProtocolV2RequestHandler("origin", "dc1", "127.0.0.4")
	targetRequestHandler := NewProtocolV2RequestHandler("target", "dc1", "127.0.0.5")

	testSetup.Origin.CqlServer.RequestHandlers = []client.RequestHandler{
		originRequestHandler.HandleRequest,
		client.NewDriverConnectionInitializationHandler("origin", "dc1", func(_ string) {}),
	}
	testSetup.Target.CqlServer.RequestHandlers = []client.RequestHandler{
		targetRequestHandler.HandleRequest,
		client.NewDriverConnectionInitializationHandler("target", "dc1", func(_ string) {}),
	}

	err = testSetup.Start(nil, false, primitive.ProtocolVersion2)
	require.Nil(t, err)

	proxy, err := setup.NewProxyInstanceWithConfig(proxyConf) // starts the proxy
	if proxy != nil {
		defer proxy.Shutdown()
	}
	require.Nil(t, err)
}

func TestProtocolV2Query(t *testing.T) {
	originAddress := "127.0.0.2"
	targetAddress := "127.0.0.3"

	serverConf := setup.NewTestConfig(originAddress, targetAddress)
	proxyConf := setup.NewTestConfig(originAddress, targetAddress)
	proxyConf.ControlConnMaxProtocolVersion = "2"

	testSetup, err := setup.NewCqlServerTestSetup(t, serverConf, false, false, false)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	originRequestHandler := NewProtocolV2RequestHandler("origin", "dc1", "")
	targetRequestHandler := NewProtocolV2RequestHandler("target", "dc1", "")

	testSetup.Origin.CqlServer.RequestHandlers = []client.RequestHandler{
		originRequestHandler.HandleRequest,
		client.NewDriverConnectionInitializationHandler("origin", "dc1", func(_ string) {}),
	}
	testSetup.Target.CqlServer.RequestHandlers = []client.RequestHandler{
		targetRequestHandler.HandleRequest,
		client.NewDriverConnectionInitializationHandler("target", "dc1", func(_ string) {}),
	}

	err = testSetup.Start(nil, false, primitive.ProtocolVersion2)
	require.Nil(t, err)

	proxy, err := setup.NewProxyInstanceWithConfig(proxyConf) // starts the proxy
	if proxy != nil {
		defer proxy.Shutdown()
	}
	require.Nil(t, err)

	cqlConn, err := testSetup.Client.CqlClient.Connect(context.Background())
	query := &message.Query{
		Query:   "SELECT * FROM fakeks.faketb",
		Options: &message.QueryOptions{Consistency: primitive.ConsistencyLevelOne},
	}

	response, err := cqlConn.SendAndReceive(frame.NewFrame(primitive.ProtocolVersion2, 0, query))
	resultSet := response.Body.Message.(*message.RowsResult).Data
	require.Equal(t, 1, len(resultSet))
}

type ProtocolV2RequestHandler struct {
	cluster    string
	datacenter string
	peerIP     string
}

func NewProtocolV2RequestHandler(cluster string, datacenter string, peerIP string) *ProtocolV2RequestHandler {
	return &ProtocolV2RequestHandler{
		cluster:    cluster,
		datacenter: datacenter,
		peerIP:     peerIP,
	}
}

func (recv *ProtocolV2RequestHandler) HandleRequest(
	request *frame.Frame,
	conn *client.CqlServerConnection,
	ctx client.RequestHandlerContext) (response *frame.Frame) {
	switch request.Body.Message.GetOpCode() {
	case primitive.OpCodeStartup:
		if request.Header.Version != primitive.ProtocolVersion2 {
			return frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.ProtocolError{
				ErrorMessage: fmt.Sprintf("Invalid or unsupported protocol version (%d)", request.Header.Version),
			})
		}
		return frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.Ready{})
	case primitive.OpCodeRegister:
		return frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.Ready{})
	case primitive.OpCodeQuery:
		query := request.Body.Message.(*message.Query)
		switch query.Query {
		case "SELECT * FROM system.local":
			// C* 2.0.0 does not store local endpoint details in system.local table
			sysLocRow := systemLocalRow(recv.cluster, recv.datacenter, "Murmur3Partitioner", nil, request.Header.Version)
			sysLocMsg := &message.RowsResult{
				Metadata: &message.RowsMetadata{
					ColumnCount: int32(len(systemLocalColumnsProtocolV2)),
					Columns:     systemLocalColumnsProtocolV2,
				},
				Data: message.RowSet{sysLocRow},
			}
			return frame.NewFrame(request.Header.Version, request.Header.StreamId, sysLocMsg)
		case "SELECT * FROM system.peers":
			var sysPeerRows message.RowSet
			if len(recv.peerIP) > 0 {
				sysPeerRows = append(sysPeerRows, systemPeersRow(
					recv.datacenter,
					&net.TCPAddr{IP: net.ParseIP(recv.peerIP), Port: 9042},
					primitive.ProtocolVersion2,
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
		case "SELECT * FROM fakeks.faketb":
			sysLocMsg := &message.RowsResult{
				Metadata: &message.RowsMetadata{
					ColumnCount: 2,
					Columns: []*message.ColumnMetadata{
						{Keyspace: "fakeks", Table: "faketb", Name: "key", Type: datatype.Varchar},
						{Keyspace: "fakeks", Table: "faketb", Name: "value", Type: datatype.Uuid},
					},
				},
				Data: message.RowSet{
					message.Row{keyValue, hostIdValue},
				},
			}
			return frame.NewFrame(request.Header.Version, request.Header.StreamId, sysLocMsg)
		}
	}
	return nil
}
