package integration_tests

import (
	"fmt"
	"github.com/bmizerany/assert"
	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/utils"
	"testing"

	"github.com/gocql/gocql"
)

// BasicBatch tests basic batch statement functionality
// The test runs a basic batch statement, which includes an insert and update,
// and then runs an insert and update after to make sure it works
func TestBasicBatch(t *testing.T) {

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

	// Run queries on proxied connection

	// Batch statement: Update to katelyn, Insert terrance
	b := proxy.NewBatch(gocql.LoggedBatch)
	b.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'katelyn' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91", setup.TestKeyspace, setup.TestTable))
	b.Query(fmt.Sprintf("INSERT INTO %s.%s (id, task) VALUES (d1b05da0-8c20-11ea-9fc6-6d2c86545d92 ,'terrance')", setup.TestKeyspace, setup.TestTable))

	err = proxy.ExecuteBatch(b)
	if err != nil {
		t.Log("Batch failed.")
		t.Fatal(err)
	}

	// Update: terrance --> kelvin
	err = proxy.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'kelvin' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d92;", setup.TestKeyspace, setup.TestTable)).Exec()
	if err != nil {
		t.Log("Post-batch update failed.")
		t.Fatal(err)
	}

	// Insert isabelle
	err = proxy.Query(fmt.Sprintf("INSERT INTO %s.%s (id, task) VALUES (d1b05da0-8c20-11ea-9fc6-6d2c86545d93 ,'isabelle');", setup.TestKeyspace, setup.TestTable)).Exec()
	if err != nil {
		t.Log("Post-batch insert failed.")
		t.Fatal(err)
	}

	// Update: isabelle --> ryan
	err = proxy.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'ryan' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d93;", setup.TestKeyspace, setup.TestTable)).Exec()
	if err != nil {
		t.Log("Post-batch update failed.")
		t.Fatal(err)
	}

	// Assertions!

	// Check katelyn
	itr := proxy.Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91;", setup.TestKeyspace, setup.TestTable)).Iter()
	row := make(map[string]interface{})

	assert.T(t, itr.MapScan(row))
	task := setup.MapToTask(row)

	setup.AssertEqual(t, "katelyn", task.Task)

	// Check kelvin
	itr = proxy.Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d92;", setup.TestKeyspace, setup.TestTable)).Iter()
	row = make(map[string]interface{})

	assert.T(t, itr.MapScan(row))
	task = setup.MapToTask(row)

	setup.AssertEqual(t, "kelvin", task.Task)

	// Check ryan
	itr = proxy.Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d93;", setup.TestKeyspace, setup.TestTable)).Iter()
	row = make(map[string]interface{})

	assert.T(t, itr.MapScan(row))
	task = setup.MapToTask(row)

	setup.AssertEqual(t, "ryan", task.Task)
}
