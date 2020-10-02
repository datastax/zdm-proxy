package integration_tests

import (
	"github.com/bmizerany/assert"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/utils"
	"testing"
)

// BasicBatch tests basic batch statement functionality
// The test runs a basic batch statement, which includes an insert and update,
// and then runs an insert and update after to make sure it works
func TestGoCqlConnect(t *testing.T) {
	testSetup := setup.NewTestSetup()
	defer testSetup.Cleanup()

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)
	defer proxy.Close()

	if err != nil {
		t.Log("Unable to connect to proxy session.")
		t.Fatal(err)
	}

	iter := proxy.Query("SELECT * FROM fakeks.faketb").Iter()
	result, err := iter.SliceMap()

	if err != nil {
		t.Fatal("query failed:", err)
	}

	assert.Equal(t, 0, len(result))

	// simulacron generates fake response metadata when queries aren't primed
	assert.Equal(t, "fake", iter.Columns()[0].Name)
}
