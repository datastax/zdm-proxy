package integration_tests

import (
	"fmt"
	"github.com/bmizerany/assert"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/utils"
	"testing"
)

// BasicUpdate tests if update queries run correctly
// Unloads the source database,
// performs an update where through the proxy
// then loads the unloaded data into the destination
func TestBasicUpdate(t *testing.T) {
	proxyInstance := NewProxyInstance()
	defer proxyInstance.Shutdown()

	// Initialize test data
	dataIds1 := []string{
		"cf0f4cf0-8c20-11ea-9fc6-6d2c86545d91",
		"d1b05da0-8c20-11ea-9fc6-6d2c86545d91",
		"eed574b0-8c20-11ea-9fc6-6d2c86545d91"}
	dataTasks1 := []string{
		"MSzZMTWA9hw6tkYWPTxT0XfGL9nGQUpy",
		"IH0FC3aWM4ynriOFvtr5TfiKxziR5aB1",
		"FgQfJesbNcxAebzFPRRcW2p1bBtoz1P1"}

	// Seed source and dest w/ schema and data
	setup.SeedData(source.GetSession(), dest.GetSession(), setup.TestTable, dataIds1, dataTasks1)

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		t.Log("Unable to connect to proxy session.")
		t.Fatal(err)
	}

	// Run query on proxied connection
	err = proxy.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'terrance' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91;", setup.TestKeyspace, setup.TestTable)).Exec()
	if err != nil {
		t.Log("Mid-migration update failed.")
		t.Fatal(err)
	}

	// Assertions!
	itr := dest.GetSession().Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91;", setup.TestKeyspace, setup.TestTable)).Iter()
	row := make(map[string]interface{})

	assert.T(t, itr.MapScan(row))
	task := setup.MapToTask(row)

	setup.AssertEqual(t, "terrance", task.Task)
}
