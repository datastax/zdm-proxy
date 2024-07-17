package integration_tests

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// Test sending more concurrent, async request than allowed stream IDs.
// Origin and target clusters are stubbed and will return protocol error
// if we notice greater stream ID value than expected. We cannot easily test
// exceeding 127 stream IDs allowed in protocol V2, because clients will
// fail serializing the frame
func TestMaxStreamIds(t *testing.T) {
	originAddress := "127.0.1.1"
	targetAddress := "127.0.1.2"
	originProtoVer := primitive.ProtocolVersion2
	targetProtoVer := primitive.ProtocolVersion2
	requestCount := 20
	maxStreamIdsConf := 10
	maxStreamIdsExpected := 10
	serverConf := setup.NewTestConfig(originAddress, targetAddress)
	proxyConf := setup.NewTestConfig(originAddress, targetAddress)

	queryInsert := &message.Query{
		Query: "INSERT INTO test_ks.test(key, value) VALUES(1, '1')", // use INSERT to route request to both clusters
	}

	buffer := utils.CreateLogHooks(log.WarnLevel, log.ErrorLevel)
	defer log.StandardLogger().ReplaceHooks(make(log.LevelHooks))

	testSetup, err := setup.NewCqlServerTestSetup(t, serverConf, false, false, false)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	originRequestHandler := NewMaxStreamIdsRequestHandler("origin", "dc1", originAddress, maxStreamIdsExpected)
	targetRequestHandler := NewProtocolNegotiationRequestHandler("target", "dc1", targetAddress, []primitive.ProtocolVersion{targetProtoVer})

	testSetup.Origin.CqlServer.RequestHandlers = []client.RequestHandler{
		originRequestHandler.HandleRequest,
		client.NewDriverConnectionInitializationHandler("origin", "dc1", func(_ string) {}),
	}
	testSetup.Target.CqlServer.RequestHandlers = []client.RequestHandler{
		targetRequestHandler.HandleRequest,
		client.NewDriverConnectionInitializationHandler("target", "dc1", func(_ string) {}),
	}

	err = testSetup.Start(nil, false, originProtoVer)
	require.Nil(t, err)

	proxyConf.ProxyMaxStreamIds = maxStreamIdsConf
	proxy, err := setup.NewProxyInstanceWithConfig(proxyConf) // starts the proxy
	if proxy != nil {
		defer proxy.Shutdown()
	}
	require.Nil(t, err)

	testSetup.Client.CqlClient.MaxInFlight = 127 // set to 127, otherwise we fail to serialize in protocol
	cqlConn, err := testSetup.Client.CqlClient.ConnectAndInit(context.Background(), originProtoVer, 0)
	require.Nil(t, err)
	defer cqlConn.Close()

	remainingRequests := requestCount

	for j := 0; j < 10; j++ {
		var responses []client.InFlightRequest
		for i := 0; i < remainingRequests; i++ {
			inFlightReq, err := cqlConn.Send(frame.NewFrame(originProtoVer, 0, queryInsert))
			require.Nil(t, err)
			responses = append(responses, inFlightReq)
		}

		for _, response := range responses {
			select {
			case msg := <-response.Incoming():
				if response.Err() != nil {
					t.Fatalf(response.Err().Error())
				}
				switch msg.Body.Message.(type) {
				case *message.VoidResult:
					// expected, we have received successful response
					remainingRequests--
				case *message.Overloaded:
					// client received overloaded message due to insufficient stream ID pool, retry the request
				default:
					t.Fatalf(response.Err().Error())
				}
			}
		}

		if remainingRequests == 0 {
			break
		}
	}

	require.True(t, strings.Contains(buffer.String(), "no stream id available"))

	require.True(t, len(originRequestHandler.usedStreamIdsPerConn) >= 1)
	for _, idMap := range originRequestHandler.usedStreamIdsPerConn {
		maxId := int16(0)
		for streamId := range idMap {
			if streamId > maxId {
				maxId = streamId
			}
		}
		require.True(t, maxId < int16(maxStreamIdsExpected))
	}
}

type MaxStreamIdsRequestHandler struct {
	lock                 sync.Mutex
	cluster              string
	datacenter           string
	peerIP               string
	maxStreamIds         int
	usedStreamIdsPerConn map[int]map[int16]bool
}

func NewMaxStreamIdsRequestHandler(cluster string, datacenter string, peerIP string, maxStreamIds int) *MaxStreamIdsRequestHandler {
	return &MaxStreamIdsRequestHandler{
		cluster:              cluster,
		datacenter:           datacenter,
		peerIP:               peerIP,
		maxStreamIds:         maxStreamIds,
		usedStreamIdsPerConn: make(map[int]map[int16]bool),
	}
}

func (recv *MaxStreamIdsRequestHandler) HandleRequest(
	request *frame.Frame,
	conn *client.CqlServerConnection,
	ctx client.RequestHandlerContext) (response *frame.Frame) {
	port := conn.RemoteAddr().(*net.TCPAddr).Port

	switch request.Body.Message.GetOpCode() {
	case primitive.OpCodeStartup:
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
			if request.Header.Version == primitive.ProtocolVersion2 {
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
					request.Header.Version,
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
		case "INSERT INTO test_ks.test(key, value) VALUES(1, '1')":
			recv.lock.Lock()
			usedStreamIdsMap := recv.usedStreamIdsPerConn[port]
			if usedStreamIdsMap == nil {
				usedStreamIdsMap = make(map[int16]bool)
				recv.usedStreamIdsPerConn[port] = usedStreamIdsMap
			}
			usedStreamIdsMap[request.Header.StreamId] = true
			recv.lock.Unlock()

			time.Sleep(5 * time.Millisecond) // introduce some delay so that stream IDs are not released immediately

			if len(usedStreamIdsMap) > recv.maxStreamIds {
				return frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.ProtocolError{
					ErrorMessage: fmt.Sprintf("Too many stream IDs used (%d)", len(usedStreamIdsMap)),
				})
			}
			return frame.NewFrame(request.Header.Version, request.Header.StreamId, &message.VoidResult{})
		}
	}
	return nil
}
