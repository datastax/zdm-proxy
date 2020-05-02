package main

import (
	"cloud-gate/integration-tests/test"
	"cloud-gate/utils"
	"errors"
	"fmt"
	"os/exec"

	"github.com/gocql/gocql"

	log "github.com/sirupsen/logrus"
)

func getKeyspaces(session *gocql.Session) ([]string, error) {
	ignoreKeyspaces := []string{"system_auth", "system_schema", "dse_system_local", "dse_system", "dse_leases", "solr_admin",
		"dse_insights", "dse_insights_local", "system_distributed", "system", "dse_perf", "system_traces", "dse_security"}

	kQuery := `SELECT keyspace_name FROM system_schema.keyspaces;`
	itr := session.Query(kQuery).Iter()
	if itr == nil {
		return nil, errors.New("Did not find any keyspaces to migrate")
	}
	existingKeyspaces := make([]string, 0)
	var keyspaceName string
	for itr.Scan(&keyspaceName) {
		if !utils.Contains(ignoreKeyspaces, keyspaceName) {
			existingKeyspaces = append(existingKeyspaces, keyspaceName)
		}
	}
	return existingKeyspaces, nil
}

func dropExistingKeyspaces(session *gocql.Session) {
	keyspaces, err := getKeyspaces(session)
	if err != nil {
		log.WithError(err).Fatal(err)
	}

	for _, keyspace := range keyspaces {
		session.Query(fmt.Sprintf("DROP KEYSPACE %s;", keyspace)).Exec()
	}
}

func seedKeyspace(session *gocql.Session, keyspace string) {
	session.Query(fmt.Sprintf("CREATE KEYSPACE %s;", keyspace)).Exec() // TODO:
}

func seedData(session *gocql.Session, keyspace string) {
	// TODO:
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
	seedKeyspace(sourceSession, "test_keyspace")
	seedKeyspace(destSession, "test_keyspace")

	// Seed source with test data
	seedData(sourceSession, "test_keyspace")

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
