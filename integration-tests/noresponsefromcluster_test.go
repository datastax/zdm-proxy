package integration_tests

import (
	"context"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/riptano/cloud-gate/integration-tests/client"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/integration-tests/simulacron"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestAtLeastOneClusterReturnsNoResponse(t *testing.T) {

	simulacronSetup, err := setup.NewSimulacronTestSetup()
	require.Nil(t, err)
	defer simulacronSetup.Cleanup()

	testClient, err := client.NewTestClient(context.Background(), "127.0.0.1:14002")
	require.True(t, err == nil, "testClient setup failed: %s", err)

	defer testClient.Shutdown()

	err = testClient.PerformDefaultHandshake(context.Background(), primitive.ProtocolVersion4, false)
	require.True(t, err == nil, "No-auth handshake failed: %s", err)

	queryPrimeNoResponse :=
		simulacron.WhenQuery(
			"INSERT INTO myks.users (name) VALUES (?)",
			simulacron.
				NewWhenQueryOptions().
				WithPositionalParameter(simulacron.DataTypeText, "john")).ThenNoResult()

	queryPrimeResponse :=
		simulacron.WhenQuery(
			"INSERT INTO myks.users (name) VALUES (?)",
			simulacron.
				NewWhenQueryOptions().
				WithPositionalParameter(simulacron.DataTypeText, "john")).
			ThenRowsSuccess(simulacron.NewRowsResult(map[string]simulacron.DataType{"name": simulacron.DataTypeText}).
				WithRow(map[string]interface{}{"name": "john"}))

	clusters := []string{"origin", "target", "both"}
	for _, clusterNotResponding := range clusters {

		t.Run(clusterNotResponding, func(t *testing.T) {

			err := simulacronSetup.Origin.ClearPrimes()
			require.True(t, err == nil, "clear primes failed on origin: %s", err)

			err = simulacronSetup.Target.ClearPrimes()
			require.True(t, err == nil, "clear primes failed on target: %s", err)

			switch clusterNotResponding {
			case "origin":
				err = simulacronSetup.Origin.Prime(queryPrimeNoResponse)
				require.True(t, err == nil, "Error priming Origin for no response: ", err)

				err = simulacronSetup.Target.Prime(queryPrimeResponse)
				require.True(t, err == nil, "Error priming Target for response: ", err)

			case "target":
				err = simulacronSetup.Origin.Prime(queryPrimeResponse)
				require.True(t, err == nil, "Error priming Origin for response: ", err)

				err = simulacronSetup.Target.Prime(queryPrimeNoResponse)
				require.True(t, err == nil, "Error priming Target for no response: ", err)

			case "both":
				err = simulacronSetup.Origin.Prime(queryPrimeNoResponse)
				require.True(t, err == nil, "Error priming Origin for no response: ", err)

				err = simulacronSetup.Target.Prime(queryPrimeNoResponse)
				require.True(t, err == nil, "Error priming Target for no response: ", err)
			}

			query := &message.Query{
				Query: "INSERT INTO myks.users (name) VALUES (?)",
				Options: &message.QueryOptions{
					PositionalValues: []*primitive.Value{primitive.NewValue([]byte("john"))},
				},
			}
			response, _, err := testClient.SendMessage(context.Background(), primitive.ProtocolVersion4, query)

			require.True(t, response == nil, "a response has been received")
			require.True(t, err != nil, "no error has been received, but the request should have failed")
			require.True(t, strings.EqualFold(err.Error(), "request timed out at client level"), "the request should have timed out at client level, but it didn't")
		})
	}

}
