package test

import (
	"cloud-gate/integration-tests/setup"
	"cloud-gate/migration/migration"
	"cloud-gate/utils"
	"fmt"
	"net"
	"time"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

// BasicBatch tests basic batch statement functionality
// The test runs a basic batch statement, which includes an insert and update,
// and then runs an insert and update after to make sure it works
func BasicBatch(c net.Conn, source *gocql.Session, dest *gocql.Session) {
	status := setup.CreateStatusObject()
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
	setup.SeedData(source, dest, setup.TestTable, dataIds1, dataTasks1)

	// Send start
	log.Info("Sending start signal to proxy")
	setup.SendStart(c, status)

	log.Info("Attempting to connect to db as client through proxy...")

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		log.WithError(err).Error("Unable to connect to proxy session.")
	}

	// Send unload table
	status.Tables[setup.TestKeyspace][setup.TestTable].Step = migration.UnloadingData
	setup.SendTableUpdate(c, status.Tables[setup.TestKeyspace][setup.TestTable])

	// Unload the table
	unloadedData := setup.UnloadData(source, setup.TestTable)

	log.Info("unloaded data", unloadedData)

	// Run queries on proxied connection

	// Batch statement: Update to katelyn, Insert terrance
	b := proxy.NewBatch(gocql.LoggedBatch)
	b.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'katelyn' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91", setup.TestKeyspace, setup.TestTable))
	b.Query(fmt.Sprintf("INSERT INTO %s.%s (id, task) VALUES (d1b05da0-8c20-11ea-9fc6-6d2c86545d92 ,'terrance')", setup.TestKeyspace, setup.TestTable))

	err = proxy.ExecuteBatch(b)
	if err != nil {
		log.WithError(err).Error("Batch failed.")
	}

	// Update: terrance --> kelvin
	err = proxy.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'kelvin' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d92;", setup.TestKeyspace, setup.TestTable)).Exec()
	if err != nil {
		log.WithError(err).Error("Post-batch update failed.")
	}

	// Insert isabelle
	err = proxy.Query(fmt.Sprintf("INSERT INTO %s.%s (id, task) VALUES (d1b05da0-8c20-11ea-9fc6-6d2c86545d93 ,'isabelle');", setup.TestKeyspace, setup.TestTable)).Exec()
	if err != nil {
		log.WithError(err).Error("Post-batch insert failed.")
	}

	// Update: isabelle --> ryan
	err = proxy.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'ryan' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d93;", setup.TestKeyspace, setup.TestTable)).Exec()
	if err != nil {
		log.WithError(err).Error("Post-batch update failed.")
	}

	// Send load table
	status.Tables[setup.TestKeyspace][setup.TestTable].Step = migration.LoadingData
	setup.SendTableUpdate(c, status.Tables[setup.TestKeyspace][setup.TestTable])

	// Load the table
	setup.LoadData(dest, unloadedData, setup.TestTable)

	// Send table complete
	status.Tables[setup.TestKeyspace][setup.TestTable].Step = migration.LoadingDataComplete
	setup.SendTableUpdate(c, status.Tables[setup.TestKeyspace][setup.TestTable])

	// Send migration complete
	setup.SendMigrationComplete(c, status)

	time.Sleep(2 * time.Second)
	log.Info("Sleep 2 seconds")

	// Assertions!

	// Check katelyn
	itr := proxy.Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91;", setup.TestKeyspace, setup.TestTable)).Iter()
	row := make(map[string]interface{})

	itr.MapScan(row)
	task := setup.MapToTask(row)

	setup.Assert("katelyn", task.Task)

	// Check kelvin
	itr = proxy.Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d92;", setup.TestKeyspace, setup.TestTable)).Iter()
	row = make(map[string]interface{})

	itr.MapScan(row)
	task = setup.MapToTask(row)

	setup.Assert("kelvin", task.Task)

	// Check ryan
	itr = proxy.Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d93;", setup.TestKeyspace, setup.TestTable)).Iter()
	row = make(map[string]interface{})

	itr.MapScan(row)
	task = setup.MapToTask(row)

	setup.Assert("ryan", task.Task)
}
