package integration_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/require"

	"github.com/datastax/zdm-proxy/integration-tests/ccm"
	"github.com/datastax/zdm-proxy/integration-tests/client"
	"github.com/datastax/zdm-proxy/integration-tests/env"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
)

// TestSchemaEvents tests the schema event message handling
func TestSchemaEvents(t *testing.T) {
	originCluster, targetCluster, err := SetupOrGetGlobalCcmClusters(t)
	require.Nil(t, err)

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
			proxyInstance, err := NewProxyInstanceForGlobalCcmClusters(t)
			require.Nil(t, err)
			defer proxyInstance.Shutdown()

			// test client that connects to the proxy
			testClientForEvents, err := client.NewTestClient(context.Background(), "127.0.0.1:14002")
			require.True(t, err == nil, "unable to connect to test client: %v", err)
			defer testClientForEvents.Shutdown()

			// test client that connects to the C* node directly
			testClientForSchemaChange, err := client.NewTestClient(context.Background(), tt.endpointSchemaChange)
			require.True(t, err == nil, "unable to connect to test client: %v", err)
			defer testClientForSchemaChange.Shutdown()

			err = testClientForEvents.PerformDefaultHandshake(context.Background(), env.DefaultProtocolVersion, false)
			require.True(t, err == nil, "could not perform handshake: %v", err)

			err = testClientForSchemaChange.PerformDefaultHandshake(context.Background(), env.DefaultProtocolVersion, false)
			require.True(t, err == nil, "could not perform handshake: %v", err)

			// send REGISTER to proxy
			registerMsg := &message.Register{
				EventTypes: []primitive.EventType{
					primitive.EventTypeSchemaChange,
					primitive.EventTypeStatusChange,
					primitive.EventTypeTopologyChange},
			}

			response, _, err := testClientForEvents.SendMessage(context.Background(), env.DefaultProtocolVersion, registerMsg)
			require.True(t, err == nil, "could not send register frame: %v", err)

			_, ok := response.Body.Message.(*message.Ready)
			require.True(t, ok, "expected ready but got %v", response.Body.Message)

			// send schema change to C* node directly bypassing proxy
			createKeyspaceMessage := &message.Query{
				Query: fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS testks_%d "+
					"WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};", env.Rand.Uint64()),
			}

			response, _, err = testClientForSchemaChange.SendMessage(context.Background(), env.DefaultProtocolVersion, createKeyspaceMessage)
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
	tempCcmSetup, err := setup.NewTemporaryCcmTestSetup(t, true, false)
	require.Nil(t, err)
	defer tempCcmSetup.Cleanup()

	tests := []struct {
		name                    string
		clusterToChangeTopology *ccm.Cluster
	}{
		{
			name:                    "origin should not forward events",
			clusterToChangeTopology: tempCcmSetup.Origin,
		},
		{
			name:                    "target should not forward events",
			clusterToChangeTopology: tempCcmSetup.Target,
		},
	}

	for testIdx, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testConfig := setup.NewTestConfig(tempCcmSetup.Origin.GetInitialContactPoint(), tempCcmSetup.Target.GetInitialContactPoint())

			// Each test adds a node and removes it so if the node is not removed properly the proxy in the following tests
			// will try to use the previous test's node which will time out (because the node is being removed).
			// To avoid this we force the proxy to use the contact point only
			testConfig.TargetEnableHostAssignment = false
			testConfig.OriginEnableHostAssignment = false

			proxyInstance, err := setup.NewProxyInstanceWithConfig(testConfig)
			require.Nil(t, err)
			defer proxyInstance.Shutdown()

			testClientForEvents, err := client.NewTestClient(context.Background(), "127.0.0.1:14002")
			require.True(t, err == nil, "unable to connect to test client: %v", err)
			defer testClientForEvents.Shutdown()

			err = testClientForEvents.PerformDefaultHandshake(context.Background(), env.DefaultProtocolVersion, false)
			require.True(t, err == nil, "could not perform handshake: %v", err)

			registerMsg := &message.Register{
				EventTypes: []primitive.EventType{
					primitive.EventTypeSchemaChange,
					primitive.EventTypeStatusChange,
					primitive.EventTypeTopologyChange},
			}

			response, _, err := testClientForEvents.SendMessage(context.Background(), env.DefaultProtocolVersion, registerMsg)
			require.True(t, err == nil, "could not send register frame: %v", err)

			_, ok := response.Body.Message.(*message.Ready)
			require.True(t, ok, "expected ready but got %v", response.Body.Message)

			clusterToAddNode := tt.clusterToChangeTopology
			nodeIndex := clusterToAddNode.GetNumberOfSeedNodes() + testIdx
			err = clusterToAddNode.AddNode(nodeIndex)
			require.True(t, err == nil, "failed to add node: %v", err)
			defer clusterToAddNode.RemoveNode(nodeIndex)

			err = clusterToAddNode.StartNode(nodeIndex)
			require.True(t, err == nil, "failed to start node: %v", err)

			err = clusterToAddNode.StopNode(nodeIndex)
			require.True(t, err == nil, "failed to stop node: %v", err)

			response, err = testClientForEvents.GetEventMessage(5000 * time.Millisecond)
			require.True(t, err != nil, "did not expect to receive an event message: %v", response)
		})
	}
}
