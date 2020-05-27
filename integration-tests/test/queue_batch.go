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

// QueueBatch tests if the queuing functionality works correctly with batch statements
// Batch tests with multiple tables
func QueueBatch(c net.Conn, source *gocql.Session, dest *gocql.Session) {
	status := setup.CreateStatusObject()

	dataIds1 := []string{
		"cf0f4cf0-8c20-11ea-9fc6-6d2c86545d91",
		"d1b05da0-8c20-11ea-9fc6-6d2c86545d91",
		"eed574b0-8c20-11ea-9fc6-6d2c86545d91"}
	dataTasks1 := []string{
		"isabelle",
		"kelvin",
		"jodie"}

	// Seed source and dest w/ schema and data
	setup.SeedData(source, dest, setup.TestTable, dataIds1, dataTasks1)

	dataIds2 := []string{
		"cf0f4cf0-8c20-11ea-9fc6-6d2c86545d92",
		"d1b05da0-8c20-11ea-9fc6-6d2c86545d92"}
	dataTasks2 := []string{
		"sacramento",
		"austin"}

	setup.SeedData(source, dest, setup.TestTable2, dataIds2, dataTasks2)

	// Send start
	setup.SendStart(c, status)

	log.Info("Attempting to connect to db as client through proxy...")

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		log.WithError(err).Error("Unable to connect to proxy session.")
	}

	log.Info("Sending table update for unloading table 2")

	// Send unload table2
	status.Tables[setup.TestKeyspace][setup.TestTable2].Step = migration.UnloadingData
	setup.SendTableUpdate(c, status.Tables[setup.TestKeyspace][setup.TestTable2])

	// Unload the table2
	unloadedData2 := setup.UnloadData(source, setup.TestTable2)

	log.Info("unloaded data", unloadedData2)

	// Send unload table1
	status.Tables[setup.TestKeyspace][setup.TestTable].Step = migration.UnloadingData
	setup.SendTableUpdate(c, status.Tables[setup.TestKeyspace][setup.TestTable])

	// Unload the table1
	unloadedData1 := setup.UnloadData(source, setup.TestTable)

	log.Info("unloaded data", unloadedData1)

	// Run query on proxied connection

	// Send load table2
	status.Tables[setup.TestKeyspace][setup.TestTable2].Step = migration.LoadingData
	setup.SendTableUpdate(c, status.Tables[setup.TestKeyspace][setup.TestTable2])

	// Load the table2
	setup.LoadData(dest, unloadedData2, setup.TestTable2)

	// Send table complete
	status.Tables[setup.TestKeyspace][setup.TestTable2].Step = migration.LoadingDataComplete
	setup.SendTableUpdate(c, status.Tables[setup.TestKeyspace][setup.TestTable2])

	//Batch statement: update to katelyn, insert terrance
	b := proxy.NewBatch(gocql.LoggedBatch)
	b.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'katelyn' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91", setup.TestKeyspace, setup.TestTable))
	b.Query(fmt.Sprintf("INSERT INTO %s.%s (id, task) VALUES (d1b05da0-8c20-11ea-9fc6-6d2c86545d93 ,'harrisburg')", setup.TestKeyspace, setup.TestTable2))

	err = proxy.ExecuteBatch(b)
	if err != nil {
		log.WithError(err).Error("Batch failed.")
	}

	time.Sleep(2 * time.Second)
	log.Info("Sleep 2 seconds")
	var count int

	//Check that the batch insert has not run in Astra
	iter := dest.Query(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d93;", setup.TestKeyspace, setup.TestTable2)).Iter()
	iter.Scan(&count)
	setup.Assert(0, count)

	// Update to table 2 after batch - should be queued
	err = proxy.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'annapolis' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d93", setup.TestKeyspace, setup.TestTable2)).Exec()
	if err != nil {
		log.WithError(err).Error("Post-batch update failed.")
	}

	// Check that later update has not run in Astra
	iter = dest.Query(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d93;", setup.TestKeyspace, setup.TestTable2)).Iter()
	iter.Scan(&count)
	setup.Assert(0, count)

	// Send load table
	status.Tables[setup.TestKeyspace][setup.TestTable].Step = migration.LoadingData
	setup.SendTableUpdate(c, status.Tables[setup.TestKeyspace][setup.TestTable])

	// Load the table
	setup.LoadData(dest, unloadedData1, setup.TestTable)

	// Send loading data complete
	status.Tables[setup.TestKeyspace][setup.TestTable].Step = migration.LoadingDataComplete
	setup.SendTableUpdate(c, status.Tables[setup.TestKeyspace][setup.TestTable])

	// Send migration complete
	setup.SendMigrationComplete(c, status)

	time.Sleep(2 * time.Second)
	log.Info("Sleep 2 seconds")

	// Assertions!

	//Check that the batch update has worked
	itr := proxy.Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91;", setup.TestKeyspace, setup.TestTable)).Iter()
	row := make(map[string]interface{})

	itr.MapScan(row)
	task := setup.MapToTask(row)

	setup.Assert("katelyn", task.Task)

	//Check that Astra is consistent after the insert and update were queued
	itr = proxy.Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d93;", setup.TestKeyspace, setup.TestTable2)).Iter()
	row = make(map[string]interface{})

	itr.MapScan(row)
	task = setup.MapToTask(row)

	setup.Assert("annapolis", task.Task)

	log.Info("Success!")
}
