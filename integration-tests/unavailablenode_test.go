package integration_tests

import (
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/message"
	"github.com/riptano/cloud-gate/integration-tests/client"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

// TestUnavailableNode tests if the proxy closes the client connection correctly when either cluster node connection is closed
func TestUnavailableNode(t *testing.T) {

	clusters := []string{"origin", "target", "both"}
	for _, clusterNotResponding := range clusters {

		t.Run(clusterNotResponding, func(t *testing.T) {

			simulacronSetup := setup.NewSimulacronTestSetup()
			defer simulacronSetup.Cleanup()

			testClient, err := client.NewTestClient("127.0.0.1:14002")
			require.True(t, err == nil, "testClient setup failed: %s", err)
			defer testClient.Shutdown()

			err = testClient.PerformDefaultHandshake(cassandraprotocol.ProtocolVersion4, false)
			require.True(t, err == nil, "No-auth handshake failed: %s", err)

			switch clusterNotResponding {
			case "origin":
				err := simulacronSetup.Origin.DropAllConnections()
				require.True(t, err == nil, "Error dropping connections of Origin: ", err)
			case "target":
				err := simulacronSetup.Target.DropAllConnections()
				require.True(t, err == nil, "Error dropping connections of Target: ", err)
			case "both":
				err := simulacronSetup.Origin.DropAllConnections()
				require.True(t, err == nil, "Error dropping connections of Origin: ", err)
				err = simulacronSetup.Target.DropAllConnections()
				require.True(t, err == nil, "Error dropping connections of Target: ", err)
			}

			// send query to check if connection has been closed
			query := &message.Query{
				Query:   "SELECT * FROM system.peers",
				Options: message.NewQueryOptions(),
			}
			response, _, err := testClient.SendMessage(cassandraprotocol.ProtocolVersion4, query)

			require.True(t, response == nil, "a response has been received")
			require.True(t, err != nil, "no error has been received, but the request should have failed")
			require.True(t, strings.Contains(err.Error(), "response channel closed"),
				"the connection should have been closed at client level, but it didn't, got: %v", err)

			// open new connection to verify that the same proxy instance continues working normally
			newTestClient, err := client.NewTestClient("127.0.0.1:14002")
			require.True(t, err == nil, "newTestClient setup failed: %s", err)
			defer newTestClient.Shutdown()

			err = newTestClient.PerformDefaultHandshake(cassandraprotocol.ProtocolVersion4, false)
			require.True(t, err == nil, "No-auth handshake failed: %s", err)

			// send same query on the new connection and this time it should succeed
			response, _, err = newTestClient.SendMessage(cassandraprotocol.ProtocolVersion4, query)
			require.True(t, err == nil, "Query failed: %v", err)

			require.Equal(
				t,
				cassandraprotocol.OpCodeResult,
				response.Body.Message.GetOpCode(),
				"expected result but got %v", response.Body.Message)
		})
	}
}