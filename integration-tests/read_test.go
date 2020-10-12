package integration_tests

import (
	"github.com/bmizerany/assert"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/integration-tests/simulacron"
	"github.com/riptano/cloud-gate/utils"
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
	testSetup := setup.NewSimulacronTestSetup()
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
			assert.T(t, iter.NumRows() == 1, "query should have returned 1 row but returned instead: ", iter.NumRows())
			var rpcAddressActual net.IP
			ok := iter.Scan(&rpcAddressActual)
			assert.T(t, ok, "row scan failed")
			assert.T(t, rpcAddressActual.Equal(rpcAddressExpected), "expecting rpc_address to be ", rpcAddressExpected, ", got: ", rpcAddressActual)

		})
	}

}

func TestForwardDecisionsForReadsWithUseStatement(t *testing.T) {
	testSetup := setup.NewSimulacronTestSetup()
	defer testSetup.Cleanup()

	cluster := utils.NewCluster("127.0.0.1", "", "", 14002)
	cluster.NumConns = 1        // required to test USE behavior reliably
	cluster.Keyspace = "system" // only way to issue a USE statement with the gocql driver

	proxy, err := cluster.CreateSession()
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
		{"system.local", " /* trick to skip prepare */ SELECT rpc_address FROM local", testSetup.Target},
		{"system.local quoted", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"local\"", testSetup.Target},
		{"system.peers", " /* trick to skip prepare */ SELECT rpc_address FROM peers", testSetup.Target},
		{"system.peers quoted", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"peers\"", testSetup.Target},
		{"system.peers_v2", " /* trick to skip prepare */ SELECT rpc_address FROM peers_v2", testSetup.Target},
		{"system.peers_v2 quoted", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"peers_v2\"", testSetup.Target},
		// all other SELECT queries routed to Origin
		{"generic read", " /* trick to skip prepare */ SELECT rpc_address FROM local2", testSetup.Origin},
		{"generic read quoted", " /* trick to skip prepare */ SELECT \"rpc_address\" AS addr FROM \"peers_v3\"", testSetup.Origin},
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
			assert.T(t, iter.NumRows() == 1, "query should have returned 1 row but returned instead: ", iter.NumRows())
			var rpcAddressActual net.IP
			ok := iter.Scan(&rpcAddressActual)
			assert.T(t, ok, "row scan failed")
			assert.T(t, rpcAddressActual.Equal(rpcAddressExpected), "expecting rpc_address to be ", rpcAddressExpected, ", got: ", rpcAddressActual)

		})
	}

}
