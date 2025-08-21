package integration_tests

import (
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOptionsShouldComeFromTarget(t *testing.T) {

	conf := setup.NewTestConfig("127.0.1.1", "127.0.1.2")
	testSetup, err := setup.NewCqlServerTestSetup(t, conf, false, false, false)
	require.Nil(t, err)
	defer testSetup.Cleanup()
	testSetup.Origin.CqlServer.RequestHandlers = []client.RequestHandler{client.RegisterHandler, newOptionsHandler(map[string][]string{"FROM": {"origin"}}), client.HandshakeHandler, client.NewSystemTablesHandler("cluster2", "dc2")}
	testSetup.Target.CqlServer.RequestHandlers = []client.RequestHandler{client.RegisterHandler, newOptionsHandler(map[string][]string{"FROM": {"target"}}), client.HandshakeHandler, client.NewSystemTablesHandler("cluster1", "dc1")}

	err = testSetup.Start(conf, true, primitive.ProtocolVersion4)
	require.Nil(t, err)

	request := frame.NewFrame(primitive.ProtocolVersion4, client.ManagedStreamId, &message.Options{})
	response, err := testSetup.Client.CqlConnection.SendAndReceive(request)
	require.Nil(t, err)
	require.IsType(t, &message.Supported{}, response.Body.Message)
	supported := response.Body.Message.(*message.Supported)
	require.Equal(t, "target", supported.Options["FROM"][0])

}

func TestCommonCompressionAlgorithms(t *testing.T) {

	conf := setup.NewTestConfig("127.0.1.1", "127.0.1.2")
	testSetup, err := setup.NewCqlServerTestSetup(t, conf, false, false, false)
	require.Nil(t, err)
	defer testSetup.Cleanup()
	testSetup.Origin.CqlServer.RequestHandlers = []client.RequestHandler{client.RegisterHandler, newOptionsHandler(map[string][]string{"COMPRESSION": {"snappy"}}), client.HandshakeHandler, client.NewSystemTablesHandler("cluster2", "dc2")}
	testSetup.Target.CqlServer.RequestHandlers = []client.RequestHandler{client.RegisterHandler, newOptionsHandler(map[string][]string{"COMPRESSION": {"snappy", "lz4"}}), client.HandshakeHandler, client.NewSystemTablesHandler("cluster1", "dc1")}

	err = testSetup.Start(conf, true, primitive.ProtocolVersion4)
	require.Nil(t, err)

	request := frame.NewFrame(primitive.ProtocolVersion4, client.ManagedStreamId, &message.Options{})
	response, err := testSetup.Client.CqlConnection.SendAndReceive(request)
	require.Nil(t, err)
	require.IsType(t, &message.Supported{}, response.Body.Message)
	supported := response.Body.Message.(*message.Supported)
	require.Equal(t, []string{"snappy"}, supported.Options["COMPRESSION"])

}

func newOptionsHandler(options map[string][]string) client.RequestHandler {
	return func(
		request *frame.Frame,
		conn *client.CqlServerConnection,
		ctx client.RequestHandlerContext,
	) (response *frame.Frame) {
		if _, ok := request.Body.Message.(*message.Options); ok {
			response = frame.NewFrame(
				request.Header.Version,
				request.Header.StreamId,
				&message.Supported{Options: options},
			)
		}
		return
	}
}
