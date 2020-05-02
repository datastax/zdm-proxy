package main

import (
	"cloud-gate/integration-tests/test"
	"cloud-gate/utils"
	"fmt"
	"os/exec"

	"github.com/gocql/gocql"

	log "github.com/sirupsen/logrus"
)

// TestKeyspace is the dedicated keyspace for testing
// no other keyspaces will be modified
const TestKeyspace = "cloudgate_test"

func dropExistingKeyspaces(session *gocql.Session) {
	session.Query(fmt.Sprintf("DROP KEYSPACE %s;", TestKeyspace)).Exec()
}

func seedKeyspace(session *gocql.Session) {
	log.Info("Seeding keyspace")
	session.Query(fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};", TestKeyspace)).Exec()
}

func seedData(session *gocql.Session) {
	tableName := "tasks"

	// Create the table
	err := session.Query(fmt.Sprintf("CREATE TABLE %s.%s(id UUID, task text, PRIMARY KEY(id));", TestKeyspace, tableName)).Exec()
	if err != nil {
		log.WithError(err).Error(err)
	}

	// Seed data
	ids := []string{
		"d1b05da0-8c20-11ea-9fc6-6d2c86545d91",
		"eed574b0-8c20-11ea-9fc6-6d2c86545d91",
		"cf0f4cf0-8c20-11ea-9fc6-6d2c86545d91",
		"d4b2ef40-8c20-11ea-9fc6-6d2c86545d91",
		"da9e7000-8c20-11ea-9fc6-6d2c86545d91",
		"cac98cf0-8c20-11ea-9fc6-6d2c86545d91",
		"c617ebc0-8c20-11ea-9fc6-6d2c86545d91",
		"d0b69450-8c20-11ea-9fc6-6d2c86545d91",
		"c3539ba0-8c20-11ea-9fc6-6d2c86545d91",
		"d2293720-8c20-11ea-9fc6-6d2c86545d91"}

	tasks := []string{
		"MSzZMTWA9hw6tkYWPTxT0XfGL9nGQUpy",
		"IH0FC3aWM4ynriOFvtr5TfiKxziR5aB1",
		"FgQfJesbNcxAebzFPRRcW2p1bBtoz1P1",
		"pzRxykuPkQ13oXv8cFzlOGsz51Zy68lS",
		"R3xkC90pKxGzkAQbmGGIYdI24Bo82RaL",
		"lsgVQ912cUYiL6TxR6ykiH7qRej6Lkfe",
		"So5O5ai4snEwD3aq1EXgzuoFeoUplLhD",
		"WW00H9IOC9JOslNwUVBKhCi3M2W9SfSD",
		"CFgmMRUzLcZdKuwdIKHPO5ohKJrThm5R",
		"g5qVsck8edpGfVA7NaHiSJOAmIGqo3dl",
	}

	// Seed 10 rows
	for i := 0; i < len(ids); i++ {
		id, task := ids[i], tasks[i]
		err = session.Query(fmt.Sprintf("INSERT INTO %s.%s(id, task) VALUES (%s, '%s');", TestKeyspace, tableName, id, task)).Exec()
		if err != nil {
			log.WithError(err).Error(err)
		}
	}
}

func main() {
	// Connect to source and dest sessions
	var err error
	sourceSession, err := utils.ConnectToCluster("127.0.0.1", "", "", 9042)
	if err != nil {
		log.WithError(err).Error(err)
	}

	destSession, err := utils.ConnectToCluster("127.0.0.1", "", "", 9043)
	if err != nil {
		log.WithError(err).Error(err)
	}

	// Drop all existing data
	dropExistingKeyspaces(sourceSession)
	dropExistingKeyspaces(destSession)

	// Seed source and dest with keyspace
	seedKeyspace(sourceSession)
	seedKeyspace(destSession)

	// Seed source with test data
	seedData(sourceSession)

	err = exec.Command("go", "run", "./proxy/main.go").Start()
	if err != nil {
		log.WithError(err).Error(err)
	}

	log.Info("PROXY STARTED")

	migrationCommand := exec.Command("go", "run", "migration/main.go")
	migrationCommand.Env = append(migrationCommand.Env, "DRY_RUN=true")
	err = migrationCommand.Start()
	if err != nil {
		log.Fatal(err)
	}

	log.Info("MIGRATION STARTED")

	// Run test package here
	go test.Test1()

	for {
	}
}
