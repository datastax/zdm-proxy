package integration_tests

import (
	"context"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestOptionsShouldComeFromTarget(t *testing.T) {

	conf := setup.NewTestConfig("127.0.1.1", "127.0.1.2")
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	origin, target := createOriginAndTarget(conf)
	origin.RequestHandlers = []client.RequestHandler{newOptionsHandler("origin")}
	target.RequestHandlers = []client.RequestHandler{newOptionsHandler("target")}

	startOriginAndTarget(t, origin, target, ctx)
	startProxy(t, origin, target, conf, ctx)
	clientConn := startClient(t, origin, target, conf, ctx)

	request := frame.NewFrame(primitive.ProtocolVersion4, client.ManagedStreamId, &message.Options{})
	response, err := clientConn.SendAndReceive(request)
	require.Nil(t, err)
	require.IsType(t, &message.Supported{}, response.Body.Message)
	supported := response.Body.Message.(*message.Supported)
	require.Equal(t, "target", supported.Options["FROM"][0])

}

func newOptionsHandler(from string) client.RequestHandler {
	return func(
		request *frame.Frame,
		conn *client.CqlServerConnection,
		ctx client.RequestHandlerContext,
	) (response *frame.Frame) {
		if _, ok := request.Body.Message.(*message.Options); ok {
			response = frame.NewFrame(
				request.Header.Version,
				request.Header.StreamId,
				&message.Supported{Options: map[string][]string{"FROM": {from}}},
			)
		}
		return
	}
}
