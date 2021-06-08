package integration_tests

import (
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/integration-tests/simulacron"
	"github.com/riptano/cloud-gate/utils"
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
	testSetup, err := setup.NewSimulacronTestSetup()
	require.Nil(t, err)
	defer testSetup.Cleanup()

	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)
	if err != nil {
		t.Log("Unable to connect to proxy session.")
		t.Fatal(err)
	}
	defer proxy.Close()

	tests := []struct {
		name    string
		query   string
		cluster *simulacron.Cluster
	}{
		// SELECT queries routed to Target
		{"system.local", " /* trick to skip prepare */ SELECT rpc_address FROM system.local", testSetup.Target},
		{"system.local quoted", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"system\" . \"local\"", testSetup.Target},
		{"system.peers", " /* trick to skip prepare */ SELECT rpc_address FROM system.peers", testSetup.Target},
		{"system.peers quoted", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"system\" . \"peers\"", testSetup.Target},
		{"system.peers_v2", " /* trick to skip prepare */ SELECT rpc_address FROM system.peers_v2", testSetup.Target},
		{"system.peers_v2 quoted", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"system\" . \"peers_v2\"", testSetup.Target},
		{"system_auth.roles", " /* trick to skip prepare */ SELECT foo FROM system_auth.roles", testSetup.Target},
		{"system_auth.roles quoted", " /* trick to skip prepare */ SELECT \"foo\" AS f FROM \"system_auth\" . \"roles\"", testSetup.Target},
		{"dse_insights.tokens", " /* trick to skip prepare */ SELECT foo FROM dse_insights.tokens", testSetup.Target},
		{"dse_insights.tokens quoted", " /* trick to skip prepare */ SELECT \"foo\" AS f FROM \"dse_insights\" . \"tokens\"", testSetup.Target},
		// all other SELECT queries routed to Origin
		{"generic read", " /* trick to skip prepare */ SELECT rpc_address FROM ks1.local", testSetup.Origin},
		{"generic read quoted", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"ks1\" . \"peers\"", testSetup.Origin},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			_ = testSetup.Origin.ClearPrimes()
			_ = testSetup.Target.ClearPrimes()

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

		})
	}

}

func TestForwardDecisionsForReadsWithUseStatement(t *testing.T) {
	testSetup, err := setup.NewSimulacronTestSetupWithSession(false, false)
	require.Nil(t, err)
	defer testSetup.Cleanup()

	t.Run("ForwardToOrigin", func(tt *testing.T) {
		testForwardDecisionsForReadsWithUseStatement(tt, testSetup, false)
	})

	t.Run("ForwardToTarget", func(tt *testing.T) {
		testForwardDecisionsForReadsWithUseStatement(tt, testSetup, true)
	})
}

func testForwardDecisionsForReadsWithUseStatement(
	t *testing.T, testSetup *setup.SimulacronTestSetup, forwardReadsToTarget bool) {

	config := setup.NewTestConfig(testSetup.Origin.GetInitialContactPoint(), testSetup.Target.GetInitialContactPoint())
	config.ForwardReadsToTarget = forwardReadsToTarget
	proxy, err := setup.NewProxyInstanceWithConfig(config)
	require.Nil(t, err)
	defer proxy.Shutdown()

	cluster := utils.NewCluster("127.0.0.1", "", "", 14002)
	cluster.NumConns = 1 // required to test USE behavior reliably

	var expectedCluster *simulacron.Cluster
	if forwardReadsToTarget {
		expectedCluster = testSetup.Target
	} else {
		expectedCluster = testSetup.Origin
	}
	tests := []struct {
		name     string
		keyspace string
		query    string
		cluster  *simulacron.Cluster
	}{
		// SELECT queries routed to Target
		{"system.local", "system", " /* trick to skip prepare */ SELECT rpc_address FROM local", testSetup.Target},
		{"system.local quoted", "system", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"local\"", testSetup.Target},
		{"system.peers", "system", " /* trick to skip prepare */ SELECT rpc_address FROM peers", testSetup.Target},
		{"system.peers quoted", "system", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"peers\"", testSetup.Target},
		{"system.peers_v2", "system", " /* trick to skip prepare */ SELECT rpc_address FROM peers_v2", testSetup.Target},
		{"system.peers_v2 quoted", "system", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"peers_v2\"", testSetup.Target},
		{"system_auth.roles", "system_auth", " /* trick to skip prepare */ SELECT foo FROM system_auth.roles", testSetup.Target},
		{"system_auth.roles quoted", "system_auth", " /* trick to skip prepare */ SELECT \"foo\" AS f FROM \"system_auth\" . \"roles\"", testSetup.Target},
		{"dse_insights.tokens", "dse_insights", " /* trick to skip prepare */ SELECT foo FROM dse_insights.tokens", testSetup.Target},
		{"dse_insights.tokens quoted", "dse_insights", " /* trick to skip prepare */ SELECT \"foo\" AS f FROM \"dse_insights\" . \"tokens\"", testSetup.Target},
		// all other SELECT queries routed to Origin or Target according to configuration
		{"generic read", "foo", " /* trick to skip prepare */ SELECT rpc_address FROM local2", expectedCluster},
		{"generic read quoted", "foo", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"peers_v3\"", expectedCluster},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// only way to issue a USE statement with the gocql driver
			cluster.Keyspace = tt.keyspace
			proxy, err := cluster.CreateSession()
			if err != nil {
				t.Log("Unable to connect to proxy session.")
				t.Fatal(err)
			}
			defer proxy.Close()

			_ = testSetup.Origin.ClearPrimes()
			_ = testSetup.Target.ClearPrimes()

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

		})
	}
}
