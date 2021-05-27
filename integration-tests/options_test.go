package integration_tests

import (
	"context"
	"github.com/datastax/go-cassandra-native-protocol/client"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestOptionsShouldComeFromTarget(t *testing.T) {

	conf := setup.NewTestConfig("127.0.1.1", "127.0.1.2")

	origin, target := createOriginAndTarget(conf)
	defer origin.Close()
	defer target.Close()
	origin.RequestHandlers = []client.RequestHandler{newOptionsHandler("origin"), client.HandshakeHandler, client.NewSystemTablesHandler("cluster2", "dc2")}
	target.RequestHandlers = []client.RequestHandler{newOptionsHandler("target"), client.HandshakeHandler, client.NewSystemTablesHandler("cluster1", "dc1")}

	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	startOriginAndTarget(t, origin, target, ctx)
	startProxy(t, origin, target, conf, ctx, wg)
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
