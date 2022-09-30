package integration_tests

import (
	"fmt"
	"github.com/datastax/zdm-proxy/integration-tests/setup"
	"github.com/datastax/zdm-proxy/integration-tests/simulacron"
	"github.com/datastax/zdm-proxy/integration-tests/utils"
	"github.com/datastax/zdm-proxy/proxy/pkg/config"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
)

var rpcAddressExpected = net.IPv4(192, 168, 1, 1)

var rows = simulacron.NewRowsResult(
	map[string]simulacron.DataType{
		"rpc_address": simulacron.DataTypeInet,
	}).WithRow(map[string]interface{}{
	"rpc_address": rpcAddressExpected,
})

func TestForwardDecisionsForReads(t *testing.T) {
	primaryClusters := []string{config.PrimaryClusterOrigin, config.PrimaryClusterTarget}
	systemQueriesModes := []string{config.SystemQueriesModeOrigin, config.SystemQueriesModeTarget}
	for _, primary := range primaryClusters {
		for _, systemQueryMode := range systemQueriesModes {
			t.Run(fmt.Sprintf("Primary-%v_SystemQueryMode-%v", primary, systemQueryMode), func(t *testing.T) {
				testForwardDecisionsForReads(t, primary, systemQueryMode)
			})
		}
	}
}

func testForwardDecisionsForReads(t *testing.T, primaryCluster string, systemQueriesMode string) {
	c := setup.NewTestConfig("", "")
	c.PrimaryCluster = primaryCluster
	c.SystemQueriesMode = systemQueriesMode
	testSetup, err := setup.NewSimulacronTestSetupWithConfig(c)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	var expectedSystemQueryCluster *simulacron.Cluster
	var expectedNonSystemQueryCluster *simulacron.Cluster

	switch primaryCluster {
	case config.PrimaryClusterOrigin:
		expectedNonSystemQueryCluster = testSetup.Origin
	case config.PrimaryClusterTarget:
		expectedNonSystemQueryCluster = testSetup.Target
	default:
		require.FailNow(t, "unexpected primary cluster: %v", primaryCluster)
	}

	switch systemQueriesMode {
	case config.SystemQueriesModeOrigin:
		expectedSystemQueryCluster = testSetup.Origin
	case config.SystemQueriesModeTarget:
		expectedSystemQueryCluster = testSetup.Target
	default:
		require.FailNow(t, "unexpected system queries mode: %v", systemQueriesMode)
	}

	tests := []struct {
		name     string
		keyspace string
		query    string
		cluster  *simulacron.Cluster
	}{
		// SELECT queries routed to Target
		{"system.local", "", " /* trick to skip prepare */ SELECT rpc_address FROM system.local", expectedSystemQueryCluster},
		{"system.local quoted", "", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"system\" . \"local\"", expectedSystemQueryCluster},
		{"system.peers", "", " /* trick to skip prepare */ SELECT rpc_address FROM system.peers", expectedSystemQueryCluster},
		{"system.peers quoted", "", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"system\" . \"peers\"", expectedSystemQueryCluster},
		{"system.peers_v2", "", " /* trick to skip prepare */ SELECT rpc_address FROM system.peers_v2", expectedSystemQueryCluster},
		{"system.peers_v2 quoted", "", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"system\" . \"peers_v2\"", expectedSystemQueryCluster},
		{"system_auth.roles", "", " /* trick to skip prepare */ SELECT foo FROM system_auth.roles", expectedSystemQueryCluster},
		{"system_auth.roles quoted", "", " /* trick to skip prepare */ SELECT \"foo\" AS f FROM \"system_auth\" . \"roles\"", expectedSystemQueryCluster},
		{"dse_insights.tokens", "", " /* trick to skip prepare */ SELECT foo FROM dse_insights.tokens", expectedSystemQueryCluster},
		{"dse_insights.tokens quoted", "", " /* trick to skip prepare */ SELECT \"foo\" AS f FROM \"dse_insights\" . \"tokens\"", expectedSystemQueryCluster},
		// all other SELECT queries routed to Origin
		{"generic read", "", " /* trick to skip prepare */ SELECT rpc_address FROM ks1.local", expectedNonSystemQueryCluster},
		{"generic read quoted", "", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"ks1\" . \"peers\"", expectedNonSystemQueryCluster},
		{"peers", "", " /* trick to skip prepare */ SELECT rpc_address FROM peers", expectedNonSystemQueryCluster},

		// SELECT queries with USE keyspace routed to Target
		{"system.local", "system", " /* trick to skip prepare */ SELECT rpc_address FROM local", expectedSystemQueryCluster},
		{"system.local quoted", "system", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"local\"", expectedSystemQueryCluster},
		{"system.peers", "system", " /* trick to skip prepare */ SELECT rpc_address FROM peers", expectedSystemQueryCluster},
		{"system.peers quoted", "system", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"peers\"", expectedSystemQueryCluster},
		{"system.peers_v2", "system", " /* trick to skip prepare */ SELECT rpc_address FROM peers_v2", expectedSystemQueryCluster},
		{"system.peers_v2 quoted", "system", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"peers_v2\"", expectedSystemQueryCluster},
		{"system_auth.roles", "system_auth", " /* trick to skip prepare */ SELECT foo FROM system_auth.roles", expectedSystemQueryCluster},
		{"system_auth.roles quoted", "system_auth", " /* trick to skip prepare */ SELECT \"foo\" AS f FROM \"system_auth\" . \"roles\"", expectedSystemQueryCluster},
		{"dse_insights.tokens", "dse_insights", " /* trick to skip prepare */ SELECT foo FROM dse_insights.tokens", expectedSystemQueryCluster},
		{"dse_insights.tokens quoted", "dse_insights", " /* trick to skip prepare */ SELECT \"foo\" AS f FROM \"dse_insights\" . \"tokens\"", expectedSystemQueryCluster},
		// all other SELECT queries with USE keyspace routed to Origin or Target according to configuration
		{"generic read", "foo", " /* trick to skip prepare */ SELECT rpc_address FROM local2", expectedNonSystemQueryCluster},
		{"generic read quoted", "foo", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"peers_v3\"", expectedNonSystemQueryCluster},
		{"peers", "foo", " /* trick to skip prepare */ SELECT rpc_address FROM peers", expectedNonSystemQueryCluster},
	}
	for _, tt := range tests {
		testName := tt.name
		if tt.keyspace != "" {
			testName = testName + " with USE " + tt.keyspace
		}
		t.Run(testName, func(t *testing.T) {
			cluster := utils.NewCluster("127.0.0.1", "", "", 14002)
			cluster.NumConns = 1 // required to test USE behavior reliably
			if tt.keyspace != "" {
				// only way to issue a USE statement with the gocql driver
				cluster.Keyspace = tt.keyspace
			}
			proxy, err := cluster.CreateSession()
			require.Nil(t, err)
			defer proxy.Close()

			err = testSetup.Origin.ClearPrimes()
			require.Nil(t, err)
			err = testSetup.Target.ClearPrimes()
			require.Nil(t, err)
			err = testSetup.Origin.DeleteLogs()
			require.Nil(t, err)
			err = testSetup.Target.DeleteLogs()
			require.Nil(t, err)

			queryPrime :=
				simulacron.WhenQuery(
					tt.query,
					simulacron.NewWhenQueryOptions()).
					ThenRowsSuccess(rows)

			err = tt.cluster.Prime(queryPrime)
			if err != nil {
				t.Fatal("prime error: ", err.Error())
			}

			iter := proxy.Query(tt.query).Iter()
			require.True(t, iter.NumRows() == 1, "query should have returned 1 row but returned instead: ", iter.NumRows())
			var rpcAddressActual net.IP
			ok := iter.Scan(&rpcAddressActual)
			require.True(t, ok, "row scan failed")
			require.True(t, rpcAddressActual.Equal(rpcAddressExpected), "expecting rpc_address to be ", rpcAddressExpected, ", got: ", rpcAddressActual)

			logsOrigin, err := testSetup.Origin.GetLogsWithFilter(func(entry *simulacron.RequestLogEntry) bool {
				if entry.QueryType == simulacron.QueryTypeQuery && entry.Query == tt.query {
					return true
				}
				return false
			})
			require.Nil(t, err)
			logsTarget, err := testSetup.Target.GetLogsWithFilter(func(entry *simulacron.RequestLogEntry) bool {
				if entry.QueryType == simulacron.QueryTypeQuery && entry.Query == tt.query {
					return true
				}
				return false
			})
			require.Nil(t, err)
			if testSetup.Origin == tt.cluster {
				require.Equal(t, 1, len(logsOrigin.Datacenters))
				require.Equal(t, 1, len(logsOrigin.Datacenters[0].Nodes))
				require.Equal(t, 1, len(logsOrigin.Datacenters[0].Nodes[0].Queries))
				require.Equal(t, tt.query, logsOrigin.Datacenters[0].Nodes[0].Queries[0].Query)

				require.Equal(t, 1, len(logsTarget.Datacenters))
				require.Equal(t, 1, len(logsTarget.Datacenters[0].Nodes))
				require.Equal(t, 0, len(logsTarget.Datacenters[0].Nodes[0].Queries))
			} else {
				require.Equal(t, 1, len(logsTarget.Datacenters))
				require.Equal(t, 1, len(logsTarget.Datacenters[0].Nodes))
				require.Equal(t, 1, len(logsTarget.Datacenters[0].Nodes[0].Queries))
				require.Equal(t, tt.query, logsTarget.Datacenters[0].Nodes[0].Queries[0].Query)

				require.Equal(t, 1, len(logsOrigin.Datacenters))
				require.Equal(t, 1, len(logsOrigin.Datacenters[0].Nodes))
				require.Equal(t, 0, len(logsOrigin.Datacenters[0].Nodes[0].Queries))
			}
		})
	}
}