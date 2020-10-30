package integration_tests

import (
	"fmt"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol"
	"github.com/datastax/go-cassandra-native-protocol/cassandraprotocol/message"
	"github.com/riptano/cloud-gate/integration-tests/client"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/integration-tests/simulacron"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPreparedIdProxyCacheMiss(t *testing.T) {

	simulacronSetup := setup.NewSimulacronTestSetup()
	defer simulacronSetup.Cleanup()

	testClient, err := client.NewTestClient("127.0.0.1:14002")
	require.True(t, err == nil, "testClient setup failed: %s", err)

	err = testClient.PerformDefaultHandshake(cassandraprotocol.ProtocolVersion4, false)
	require.True(t, err == nil, "No-auth handshake failed: %s", err)

	defer testClient.Shutdown()

	preparedId := []byte{143, 7, 36, 50, 225, 104, 157, 89, 199, 177, 239, 231, 82, 201, 142, 253}

	executeMsg := &message.Execute{
		QueryId:          preparedId,
		ResultMetadataId: nil,
		Options:          message.NewQueryOptions(),
	}
	response, requestStreamId, err := testClient.SendMessage(cassandraprotocol.ProtocolVersion4, executeMsg)
	require.True(t, err == nil, "execute request send failed: %s", err)
	require.True(t, response != nil, "response received was null")

	errorResponse, ok := response.Body.Message.(message.Error)
	require.True(t, ok, fmt.Sprintf("expected error result but got %02x", response.Body.Message.GetOpCode()))
	require.Equal(t, requestStreamId, response.Header.StreamId, "streamId does not match expected value.")
	require.True(t, err == nil, "Error response could not be parsed: %s", err)
	require.Equal(t, cassandraprotocol.ErrorCodeUnprepared, errorResponse.GetErrorCode(), "Error code received was not Unprepared.")
	require.Equal(t, "Prepared query with ID 8f072432e1689d59c7b1efe752c98efd not found "+
		"(either the query was not prepared on this host (maybe the host has been restarted?) "+
		"or you have prepared too many queries and it has been evicted from the internal cache)",
		errorResponse.GetErrorMessage(),
		"Unexpected error message.")

	unprepared, ok := errorResponse.(*message.Unprepared)
	require.True(t, ok, fmt.Sprintf("expected unprepared but got %T", errorResponse))
	require.Equal(t, preparedId, unprepared.Id, "Error body did not contain the expected preparedId.")

}

func TestPreparedIdPreparationMismatch(t *testing.T) {

	simulacronSetup := setup.NewSimulacronTestSetup()
	defer simulacronSetup.Cleanup()

	testClient, err := client.NewTestClient("127.0.0.1:14002")
	require.True(t, err == nil, "testClient setup failed: %s", err)

	err = testClient.PerformDefaultHandshake(cassandraprotocol.ProtocolVersion4, false)
	require.True(t, err == nil, "No-auth handshake failed: %s", err)

	defer testClient.Shutdown()

	tests := map[string]*simulacron.Cluster{
		"origin": simulacronSetup.Origin,
		"target": simulacronSetup.Target,
	}

	for name, cluster := range tests {
		t.Run(name, func(t *testing.T) {

			err := simulacronSetup.Origin.ClearPrimes()
			require.True(t, err == nil, "clear primes failed on origin: %s", err)

			err = simulacronSetup.Target.ClearPrimes()
			require.True(t, err == nil, "clear primes failed on target: %s", err)

			prepareMsg := &message.Prepare{
				Query:    "INSERT INTO ks1.table1 (c1, c2) VALUES (1, 2)",
				Keyspace: "",
			}

			response, requestStreamId, err := testClient.SendMessage(cassandraprotocol.ProtocolVersion4, prepareMsg)
			require.True(t, err == nil, "prepare request send failed: %s", err)

			preparedResponse, ok := response.Body.Message.(*message.PreparedResult)
			require.True(t, ok, "did not receive prepared result, got instead: %v", response.Body.Message)

			// clear primes only on selected cluster
			err = cluster.ClearPrimes()
			require.True(t, err == nil, "clear primes failed: %s", err)

			executeMsg := &message.Execute{
				QueryId:          preparedResponse.PreparedQueryId,
				ResultMetadataId: preparedResponse.ResultMetadataId,
				Options:          message.NewQueryOptions(),
			}

			response, requestStreamId, err = testClient.SendMessage(cassandraprotocol.ProtocolVersion4, executeMsg)
			require.True(t, err == nil, "execute request send failed: %s", err)

			errorResponse, ok := response.Body.Message.(message.Error)
			require.True(t, ok, fmt.Sprintf("expected error result but got %02x", response.Body.Message.GetOpCode()))
			require.Equal(t, requestStreamId, response.Header.StreamId, "streamId does not match expected value.")
			require.True(t, err == nil, "Error response could not be parsed: %s", err)
			require.Equal(t, cassandraprotocol.ErrorCodeUnprepared, errorResponse.GetErrorCode(), "Error code received was not Unprepared.")
			require.Equal(t, "No prepared statement with id: 5440fe1",
				errorResponse.GetErrorMessage(),
				"Unexpected error message.")

			unprepared, ok := errorResponse.(*message.Unprepared)
			require.True(t, ok, fmt.Sprintf("expected unprepared but got %T", errorResponse))
			require.Equal(t, preparedResponse.PreparedQueryId, unprepared.Id, "Error body did not contain the expected preparedId.")
		})
	}
}
