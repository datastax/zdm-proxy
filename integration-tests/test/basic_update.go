package test

import (
	"fmt"
	"net"
	"time"

	"github.com/riptano/cloud-gate/integration-tests/setup"
	"github.com/riptano/cloud-gate/migration/migration"
	"github.com/riptano/cloud-gate/utils"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

// BasicUpdate tests if update queries run correctly
// Unloads the source database,
// performs an update where through the proxy
// then loads the unloaded data into the destination
func BasicUpdate(c net.Conn, source *gocql.Session, dest *gocql.Session) {
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
	log.Info("Running Seed Data")
	setup.SeedData(source, dest, setup.TestTable, dataIds1, dataTasks1)

	time.Sleep(time.Second * 4)
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

	// Run query on proxied connection
	err = proxy.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'terrance' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91;", setup.TestKeyspace, setup.TestTable)).Exec()
	if err != nil {
		log.WithError(err).Error("Mid-migration update failed.")
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
	itr := dest.Query(fmt.Sprintf("SELECT * FROM %s.%s WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91;", setup.TestKeyspace, setup.TestTable)).Iter()
	row := make(map[string]interface{})

	itr.MapScan(row)
	task := setup.MapToTask(row)

	setup.Assert("terrance", task.Task)
}
