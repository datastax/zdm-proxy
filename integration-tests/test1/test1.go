package test1

import (
	"cloud-gate/integration-tests/test"
	"cloud-gate/migration/migration"
	"cloud-gate/utils"
	"fmt"
	"net"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

// Test1 is the first test case
// Unloads the source database,
// performs an update where through the proxy
// then loads the unloaded data into the destination
func Test1(c net.Conn, source *gocql.Session, dest *gocql.Session) {
	// Send start
	test.SendStart(c)

	log.Info("Attempting to connect to db as client through proxy...")

	// Connect to proxy as a "client"
	proxy, err := utils.ConnectToCluster("127.0.0.1", "", "", 14002)

	if err != nil {
		log.WithError(err).Error("Unable to connect to proxy session.")
	}

	// Send unload table
	test.SendTableUpdate(migration.UnloadingData, c)

	// Unload the table
	unloadedData := test.UnloadData(source)

	// Run query on proxied connection
	err = proxy.Query(fmt.Sprintf("UPDATE %s.%s SET task = 'terrance' WHERE id = d1b05da0-8c20-11ea-9fc6-6d2c86545d91;", test.TestKeyspace, test.TestTable)).Exec()
	if err != nil {
		log.WithError(err).Error("Mid-migration update failed.")
	}

	// Send load table
	test.SendTableUpdate(migration.LoadingData, c)

	// Load the table
	test.LoadData(dest, unloadedData)

	// Send table complete
	test.SendTableUpdate(migration.LoadingDataComplete, c)

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
