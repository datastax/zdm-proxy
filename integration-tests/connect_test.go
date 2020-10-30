package integration_tests

import (
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/utils"
	"github.com/stretchr/testify/require"
	"testing"
)

// BasicBatch tests basic batch statement functionality
// The test runs a basic batch statement, which includes an insert and update,
// and then runs an insert and update after to make sure it works
func TestGoCqlConnect(t *testing.T) {
	testSetup := setup.NewSimulacronTestSetup()
	defer testSetup.Cleanup()

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		t.Log("Unable to connect to proxy session.")
		t.Fatal(err)
	}
	defer proxy.Close()

	iter := proxy.Query("SELECT * FROM fakeks.faketb").Iter()
	result, err := iter.SliceMap()

	if err != nil {
		t.Fatal("query failed:", err)
	}

	require.Equal(t, 0, len(result))

	// simulacron generates fake response metadata when queries aren't primed
	require.Equal(t, "fake", iter.Columns()[0].Name)
}
