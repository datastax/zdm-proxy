package integration_tests

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/integration-tests/ccm"
	"github.com/riptano/cloud-gate/integration-tests/client"
	"github.com/riptano/cloud-gate/integration-tests/env"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// TestSchemaEvents tests the schema event message handling
func TestSchemaEvents(t *testing.T) {
	if !env.UseCcm {
		t.Skip("Test requires CCM, set USE_CCM env variable to TRUE")
	}

	tests := []struct {
		name                 string
		endpointSchemaChange string
		expectedEvent        bool
	}{
		{
			name:                 "schema change on origin",
			endpointSchemaChange: fmt.Sprintf("%s:%d", originCluster.GetInitialContactPoint(), 9042),
			expectedEvent:        true,
		},
		{
			name:                 "schema change on target",
			endpointSchemaChange: fmt.Sprintf("%s:%d", targetCluster.GetInitialContactPoint(), 9042),
			expectedEvent:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proxyInstance, err := NewProxyInstanceForGlobalCcmClusters()
			require.Nil(t, err)
			defer proxyInstance.Shutdown()

			// test client that connects to the proxy
			testClientForEvents, err := client.NewTestClient("127.0.0.1:14002")
			require.True(t, err == nil, "unable to connect to test client: %v", err)
			defer testClientForEvents.Shutdown()

			// test client that connects to the C* node directly
			testClientForSchemaChange, err := client.NewTestClient(tt.endpointSchemaChange)
			require.True(t, err == nil, "unable to connect to test client: %v", err)
			defer testClientForSchemaChange.Shutdown()

			err = testClientForEvents.PerformDefaultHandshake(primitive.ProtocolVersion4, false)
			require.True(t, err == nil, "could not perform handshake: %v", err)

			err = testClientForSchemaChange.PerformDefaultHandshake(primitive.ProtocolVersion4, false)
			require.True(t, err == nil, "could not perform handshake: %v", err)

			// send REGISTER to proxy
			registerMsg := &message.Register{
				EventTypes: []primitive.EventType{
					primitive.EventTypeSchemaChange,
					primitive.EventTypeStatusChange,
					primitive.EventTypeTopologyChange},
			}

			response, _, err := testClientForEvents.SendMessage(primitive.ProtocolVersion4, registerMsg)
			require.True(t, err == nil, "could not send register frame: %v", err)

			_, ok := response.Body.Message.(*message.Ready)
			require.True(t, ok, "expected ready but got %v", response.Body.Message)

			// send schema change to C* node directly bypassing proxy
			createKeyspaceMessage := &message.Query{
				Query: fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS testks_%d "+
					"WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};", env.Rand.Uint64()),
			}

			response, _, err = testClientForSchemaChange.SendMessage(primitive.ProtocolVersion4, createKeyspaceMessage)
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

// TestTopologyStatusEvents tests the topology and status events handling
func TestTopologyStatusEvents(t *testing.T) {
	if !env.UseCcm {
		t.Skip("Test requires CCM, set USE_CCM env variable to TRUE")
	}

	tempCcmSetup, err := setup.NewTemporaryCcmTestSetup(true, false)
	require.Nil(t, err)
	defer tempCcmSetup.Cleanup()

	tests := []struct {
		name                    string
		clusterToChangeTopology *ccm.Cluster
		expectedEvents          bool
	}{
		{
			name:                    "origin should not forward events",
			clusterToChangeTopology: tempCcmSetup.Origin,
			expectedEvents:          false,
		},
		{
			name:                    "target should forward events",
			clusterToChangeTopology: tempCcmSetup.Target,
			expectedEvents:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proxyInstance, err := setup.NewProxyInstance(tempCcmSetup.Origin, tempCcmSetup.Target)
			require.Nil(t, err)
			defer proxyInstance.Shutdown()

			testClientForEvents, err := client.NewTestClient("127.0.0.1:14002")
			require.True(t, err == nil, "unable to connect to test client: %v", err)
			defer testClientForEvents.Shutdown()

			err = testClientForEvents.PerformDefaultHandshake(primitive.ProtocolVersion4, false)
			require.True(t, err == nil, "could not perform handshake: %v", err)

			registerMsg := &message.Register{
				EventTypes: []primitive.EventType{
					primitive.EventTypeSchemaChange,
					primitive.EventTypeStatusChange,
					primitive.EventTypeTopologyChange},
			}

			response, _, err := testClientForEvents.SendMessage(primitive.ProtocolVersion4, registerMsg)
			require.True(t, err == nil, "could not send register frame: %v", err)

			_, ok := response.Body.Message.(*message.Ready)
			require.True(t, ok, "expected ready but got %v", response.Body.Message)

			clusterToAddNode := tt.clusterToChangeTopology
			nodeIndex := clusterToAddNode.GetNumberOfSeedNodes()
			err = clusterToAddNode.AddNode(nodeIndex)
			require.True(t, err == nil, "failed to add node: %v", err)
			defer clusterToAddNode.RemoveNode(nodeIndex)

			err = clusterToAddNode.StartNode(nodeIndex)
			require.True(t, err == nil, "failed to start node: %v", err)

			err = clusterToAddNode.StopNode(nodeIndex)
			require.True(t, err == nil, "failed to stop node: %v", err)

			if tt.expectedEvents {
				response, err = testClientForEvents.GetEventMessage(5 * time.Second)
				require.True(t, err == nil, "could not get event message: %v", err)

				topologyChangeEvent, ok := response.Body.Message.(*message.TopologyChangeEvent)
				require.True(t, ok, "expected topology change event but was %v", response.Body.Message)
				require.Equal(t, primitive.TopologyChangeTypeNewNode, topologyChangeEvent.ChangeType)

				response, err = testClientForEvents.GetEventMessage(5 * time.Second)
				require.True(t, err == nil, "could not get second event message: %v", err)

				statusChangeEvent, ok := response.Body.Message.(*message.StatusChangeEvent)
				require.True(t, ok, "expected status change event but was %v", response.Body.Message)
				require.Equal(t, primitive.StatusChangeTypeUp, statusChangeEvent.ChangeType)

				response, err = testClientForEvents.GetEventMessage(30 * time.Second)
				require.True(t, err == nil, "could not get third event message: %v", err)

				statusChangeEvent, ok = response.Body.Message.(*message.StatusChangeEvent)
				require.True(t, ok, "expected status change event but was %v", response.Body.Message)
				require.Equal(t, primitive.StatusChangeTypeDown, statusChangeEvent.ChangeType)

				response, err = testClientForEvents.GetEventMessage(200 * time.Millisecond)
				require.True(t, err != nil, "did not expect to receive a fourth event message: %v", response)
			} else {
				response, err = testClientForEvents.GetEventMessage(5000 * time.Millisecond)
				require.True(t, err != nil, "did not expect to receive an event message: %v", response)
			}
		})
	}
}
