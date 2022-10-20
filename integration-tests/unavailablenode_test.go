package integration_tests

import (
	"context"
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/datastax/zdm-proxy/integration-tests/client"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

// TestUnavailableNode tests if the proxy closes the client connection correctly when either cluster node connection is closed
func TestUnavailableNode(t *testing.T) {
	clusters := []string{"origin", "target", "both"}
	for _, clusterNotResponding := range clusters {

		t.Run(clusterNotResponding, func(t *testing.T) {

			simulacronSetup, err := setup.NewSimulacronTestSetup(t)
			require.Nil(t, err)
			defer simulacronSetup.Cleanup()

			testClient, err := client.NewTestClient(context.Background(), "127.0.0.1:14002")
			require.True(t, err == nil, "testClient setup failed: %s", err)
			defer testClient.Shutdown()

			err = testClient.PerformDefaultHandshake(context.Background(), primitive.ProtocolVersion4, false)
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
				Query: "SELECT * FROM system.peers",
			}

			responsePtr := new(*frame.Frame)
			errPtr := new(error)
			utils.RequireWithRetries(t, func() (err error, fatal bool) {
				*responsePtr, _, *errPtr = testClient.SendMessage(context.Background(), primitive.ProtocolVersion4, query)
				if *responsePtr != nil {
					_, ok := (*responsePtr).Body.Message.(*message.Overloaded)
					if !ok {
						return fmt.Errorf("response should be nil or OVERLOADED (shutdown): %v", (*responsePtr).Body.Message), false
					}
				}
				return nil, false
			}, 25, 200 * time.Millisecond)

			response := *responsePtr
			err = *errPtr
			if response != nil {
				responseError, ok := response.Body.Message.(*message.Overloaded)
				require.True(t, ok, "response should be nil or OVERLOADED (shutdown): %v", response.Body.Message)
				require.NotNil(t, responseError, "response should be nil or OVERLOADED (shutdown): %v", response.Body.Message)
				require.Equal(t, "Shutting down, please retry on next host.", responseError.ErrorMessage)
			} else {
				require.NotNil(t, err, "no error has been received, but the request should have failed: %v")
				require.True(t, strings.Contains(err.Error(), "response channel closed"),
					"the connection should have been closed at client level, but it didn't, got: %v", err)
			}

			// open new connection to verify that the same proxy instance continues working normally
			newTestClient, err := client.NewTestClient(context.Background(), "127.0.0.1:14002")
			require.True(t, err == nil, "newTestClient setup failed: %s", err)
			defer newTestClient.Shutdown()

			err = newTestClient.PerformDefaultHandshake(context.Background(), primitive.ProtocolVersion4, false)
			require.True(t, err == nil, "No-auth handshake failed: %s", err)

			// send same query on the new connection and this time it should succeed
			response, _, err = newTestClient.SendMessage(context.Background(), primitive.ProtocolVersion4, query)
			require.True(t, err == nil, "Query failed: %v", err)

			require.Equal(
				t,
				primitive.OpCodeResult,
				response.Body.Message.GetOpCode(),
				"expected result but got %v", response.Body.Message)
		})
	}
}
