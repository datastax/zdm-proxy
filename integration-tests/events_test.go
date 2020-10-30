package integration_tests

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/message"
	"github.com/riptano/cloud-gate/integration-tests/client"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// TestSchemaEvents tests the event message handling
func TestSchemaEvents(t *testing.T) {
	if !env.UseCcm {
		t.Skip("Test requires CCM, set USE_CCM env variable to TRUE")
	}
	tests := []struct {
		name           string
		endpointSchemaChange string
		expectedEvent bool
	}{
		{
			name:                 "schema change on origin",
			endpointSchemaChange: fmt.Sprintf("%s:%d", source.GetInitialContactPoint(), 9042),
			expectedEvent:        true,
		},
		{
			name:                 "schema change on target",
			endpointSchemaChange: fmt.Sprintf("%s:%d", dest.GetInitialContactPoint(), 9042),
			expectedEvent:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proxyInstance := NewProxyInstanceForGlobalCcmClusters()
			defer proxyInstance.Shutdown()

			// test client that connects to the proxy
			testClientForEvents, err := client.NewTestClient("127.0.0.1:14002")
			require.True(t, err == nil, "unable to connect to test client: %v", err)
			defer testClientForEvents.Shutdown()

			// test client that connects to the C* node directly
			testClientForSchemaChange, err := client.NewTestClient(tt.endpointSchemaChange)
			require.True(t, err == nil, "unable to connect to test client: %v", err)
			defer testClientForSchemaChange.Shutdown()

			err = testClientForEvents.PerformDefaultHandshake(cassandraprotocol.ProtocolVersion4, false)
			require.True(t, err == nil, "could not perform handshake: %v", err)

			err = testClientForSchemaChange.PerformDefaultHandshake(cassandraprotocol.ProtocolVersion4, false)
			require.True(t, err == nil, "could not perform handshake: %v", err)

			// send REGISTER to proxy
			registerMsg := &message.Register{
				EventTypes: []cassandraprotocol.EventType{
					cassandraprotocol.EventTypeSchemaChange,
					cassandraprotocol.EventTypeStatusChange,
					cassandraprotocol.EventTypeTopologyChange},
			}

			response, _, err := testClientForEvents.SendMessage(cassandraprotocol.ProtocolVersion4, registerMsg)
			require.True(t, err == nil, "could not send register frame: %v", err)

			_, ok := response.Body.Message.(*message.Ready)
			require.True(t, ok, "expected ready but got %v", response.Body.Message)

			// send schema change to C* node directly bypassing proxy
			createKeyspaceMessage := &message.Query{
				Query:   fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS testks_%d " +
					"WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};", env.Rand.Uint64()),
				Options: message.NewQueryOptions(),
			}

			response, _, err = testClientForSchemaChange.SendMessage(cassandraprotocol.ProtocolVersion4, createKeyspaceMessage)
			require.True(t, err == nil, "could not send create keyspace request: %v", err)

			_, ok = response.Body.Message.(*message.SchemaChangeResult)
			require.True(t, ok, "expected schema change result but got %v", response.Body.Message)

			if tt.expectedEvent {
				response, err = testClientForEvents.GetEventMessage(5 * time.Second)
				require.True(t, err == nil, "could not get event message: %v", err)

				_, ok = response.Body.Message.(*message.SchemaChangeEvent)
				require.True(t, ok, "expected schema change event but was %v", response.Body.Message)

				response, err = testClientForEvents.GetEventMessage(200 * time.Millisecond)
				require.True(t, err != nil, "did not expect to receive a second event message: %v", response)
			} else {
				response, err = testClientForEvents.GetEventMessage(500 * time.Millisecond)
				require.True(t, err != nil, "did not expect to receive an event message: %v", response)
			}
		})
	}
}
