package test1

import (
	"cloud-gate/integration-tests/test"
	"cloud-gate/migration/migration"
	"cloud-gate/utils"
	"fmt"
	"net"
	"time"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

// Test1 is the first test case
// Unloads the source database,
// performs an update where through the proxy
// then loads the unloaded data into the destination
func Test1(c net.Conn, source *gocql.Session, dest *gocql.Session) {
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
	test.SeedData(source, dest, test.TestTable, dataIds1, dataTasks1)

	// Send start
	test.SendStart(c)

	log.Info("Attempting to connect to db as client through proxy...")

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		log.WithError(err).Error("Unable to connect to proxy session.")
	}

	// Send unload table
	test.SendTableUpdate(migration.UnloadingData, c, test.TestTable)

	// Unload the table
	unloadedData := test.UnloadData(source, test.TestTable)

	// Run query on proxied connection
	err = proxy.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'terrance' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91;", test.TestKeyspace, test.TestTable)).Exec()
	if err != nil {
		log.WithError(err).Error("Mid-migration update failed.")
	}

	// Send load table
	test.SendTableUpdate(migration.LoadingData, c, test.TestTable)

	// Load the table
	test.LoadData(dest, unloadedData, test.TestTable)

	// Send table complete
	test.SendTableUpdate(migration.LoadingDataComplete, c, test.TestTable)

	// Send migration complete
	test.SendMigrationComplete(c)

	// Assertions!
	itr := dest.Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91;", test.TestKeyspace, test.TestTable)).Iter()
	row := make(map[string]interface{})

	itr.MapScan(row)
	task := test.MapToTask(row)

	test.Assert("terrance", task.Task)

	log.Info("Success!")
}

// Test2 is the second test case
// The test runs a basic batch statement, which includes an insert and update,
// and then runs an insert and update after to make sure it works
func Test2(c net.Conn, source *gocql.Session, dest *gocql.Session) {
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
	test.SeedData(source, dest, test.TestTable, dataIds1, dataTasks1)

	// Send start
	test.SendStart(c)

	log.Info("Attempting to connect to db as client through proxy...")

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		log.WithError(err).Error("Unable to connect to proxy session.")
	}

	// Send unload table
	test.SendTableUpdate(migration.UnloadingData, c, test.TestTable)

	// Unload the table
	unloadedData := test.UnloadData(source, test.TestTable)

	log.Info("unloaded data", unloadedData)

	// Run query on proxied connection

	//Batch statement: update to katelyn, insert terrance
	b := proxy.NewBatch(gocql.LoggedBatch)
	b.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'katelyn' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91", test.TestKeyspace, test.TestTable))
	b.Query(fmt.Sprintf("INSERT INTO %s.%s (id, task) VALUES (d1b05da0-8c20-11ea-9fc6-6d2c86545d92 ,'terrance')", test.TestKeyspace, test.TestTable))

	err = proxy.ExecuteBatch(b)
	if err != nil {
		log.WithError(err).Error("Batch failed.")
	}

	//Update: terrance --> kelvin
	err = proxy.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'kelvin' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d92;", test.TestKeyspace, test.TestTable)).Exec()
	if err != nil {
		log.WithError(err).Error("Post-batch update failed.")
	}

	//Insert isabelle, update: isabelle --> ryan
	err = proxy.Query(fmt.Sprintf("INSERT INTO %s.%s (id, task) VALUES (d1b05da0-8c20-11ea-9fc6-6d2c86545d93 ,'isabelle');", test.TestKeyspace, test.TestTable)).Exec()
	if err != nil {
		log.WithError(err).Error("Post-batch insert failed.")
	}

	err = proxy.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'ryan' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d93;", test.TestKeyspace, test.TestTable)).Exec()
	if err != nil {
		log.WithError(err).Error("Post-batch update failed.")
	}

	// Send load table
	test.SendTableUpdate(migration.LoadingData, c, test.TestTable)

	// Load the table
	test.LoadData(dest, unloadedData, test.TestTable)

	// Send table complete
	test.SendTableUpdate(migration.LoadingDataComplete, c, test.TestTable)

	// Send migration complete
	test.SendMigrationComplete(c)

	time.Sleep(2 * time.Second)
	log.Info("Sleep 2 seconds")
	// Assertions!

	//Check katelyn
	itr := proxy.Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91;", test.TestKeyspace, test.TestTable)).Iter()
	row := make(map[string]interface{})

	itr.MapScan(row)
	task := test.MapToTask(row)

	test.Assert("katelyn", task.Task)

	//Check kelvin
	itr = proxy.Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d92;", test.TestKeyspace, test.TestTable)).Iter()
	row = make(map[string]interface{})

	itr.MapScan(row)
	task = test.MapToTask(row)

	test.Assert("kelvin", task.Task)

	//Check ryan
	itr = proxy.Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d93;", test.TestKeyspace, test.TestTable)).Iter()
	row = make(map[string]interface{})

	itr.MapScan(row)
	task = test.MapToTask(row)

	test.Assert("ryan", task.Task)

	log.Info("Success!")
}

// Test3 is the third test case
// Batch tests with multiple tables
func Test3(c net.Conn, source *gocql.Session, dest *gocql.Session) {
	dataIds1 := []string{
		"cf0f4cf0-8c20-11ea-9fc6-6d2c86545d91",
		"d1b05da0-8c20-11ea-9fc6-6d2c86545d91",
		"eed574b0-8c20-11ea-9fc6-6d2c86545d91"}
	dataTasks1 := []string{
		"isabelle",
		"kelvin",
		"jodie"}

	// Seed source and dest w/ schema and data
	test.SeedData(source, dest, test.TestTable, dataIds1, dataTasks1)

	dataIds2 := []string{
		"cf0f4cf0-8c20-11ea-9fc6-6d2c86545d92",
		"d1b05da0-8c20-11ea-9fc6-6d2c86545d92"}
	dataTasks2 := []string{
		"sacramento",
		"austin"}

	test.SeedData(source, dest, test.TestTable2, dataIds2, dataTasks2)

	// Send start
	test.SendStart(c)

	log.Info("Attempting to connect to db as client through proxy...")

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		log.WithError(err).Error("Unable to connect to proxy session.")
	}

	// Send unload table2
	test.SendTableUpdate(migration.UnloadingData, c, test.TestTable2)

	// Unload the table2
	unloadedData2 := test.UnloadData(source, test.TestTable2)

	log.Info("unloaded data", unloadedData2)

	// Send unload table1
	test.SendTableUpdate(migration.UnloadingData, c, test.TestTable)

	// Unload the table1
	unloadedData1 := test.UnloadData(source, test.TestTable)

	// Run query on proxied connection

	//Batch statement: update to katelyn, insert terrance
	b := proxy.NewBatch(gocql.LoggedBatch)
	b.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'katelyn' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91", test.TestKeyspace, test.TestTable))
	b.Query(fmt.Sprintf("INSERT INTO %s.%s (id, task) VALUES (d1b05da0-8c20-11ea-9fc6-6d2c86545d92 ,'terrance')", test.TestKeyspace, test.TestTable))

	err = proxy.ExecuteBatch(b)
	if err != nil {
		log.WithError(err).Error("Batch failed.")
	}

	//Update: terrance --> kelvin
	err = proxy.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'kelvin' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d92;", test.TestKeyspace, test.TestTable)).Exec()
	if err != nil {
		log.WithError(err).Error("Post-batch update failed.")
	}

	// Send load table2
	test.SendTableUpdate(migration.LoadingData, c, test.TestTable2)

	// Load the table2
	test.LoadData(dest, unloadedData2, test.TestTable2)

	// Send table complete
	test.SendTableUpdate(migration.LoadingDataComplete, c, test.TestTable2)

	// Send migration complete
	test.SendMigrationComplete(c)

	time.Sleep(2 * time.Second)
	log.Info("Sleep 2 seconds")
	// Assertions!

	//Check katelyn
	itr := proxy.Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91;", test.TestKeyspace, test.TestTable)).Iter()
	row := make(map[string]interface{})

	itr.MapScan(row)
	task := test.MapToTask(row)

	test.Assert("katelyn", task.Task)

	//Check kelvin
	itr = proxy.Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d92;", test.TestKeyspace, test.TestTable)).Iter()
	row = make(map[string]interface{})

	itr.MapScan(row)
	task = test.MapToTask(row)

	test.Assert("kelvin", task.Task)

	log.Info("Success!")
}
