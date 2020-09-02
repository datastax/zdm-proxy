package main

import (
	"bufio"
	"errors"
	"os"
	"strings"
	"sync"

	"github.com/riptano/cloud-gate/migration/migration"
	"github.com/riptano/cloud-gate/proxy/pkg/config"
	"github.com/riptano/cloud-gate/proxy/pkg/cloudgateproxy"
	"github.com/riptano/cloud-gate/utils"


	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

var (
	cp *cloudgateproxy.CloudgateProxy
)

// Method mainly to test the proxy service for now
func main() {
	conf := config.New().ParseEnvVars()

	if conf.Debug {
		log.SetLevel(log.DebugLevel)
	}
	log.Debugf("parsed env vars")

	cp = &cloudgateproxy.CloudgateProxy{
		Conf: conf,
	}

	m := new(CPMetadata)

	err := m.Init(cp)
	if err != nil {
		log.WithError(err).Fatal("Proxy initialization failed when attempting to connect to the clusters")
	}

	// for testing purposes. to delete
	if conf.Test {
		go doTestingCP()
	}

	go m.prepCloudgateProxy(cp)

	err2 := cp.Start()
	if err2 != nil {
		// TODO: handle error
		log.Error(err2)
		panic(err)
	}

	log.Debugf("Started, waiting for ReadyForRedirect in background")
	for {
		select {
		case <-cp.ReadyForRedirect:
			log.Info("Coordinate received signal that there are no more connections to Client Database.")
		}
	}
}

func (m *CPMetadata) Init(cp *cloudgateproxy.CloudgateProxy) error {

	// Connect to source and destination sessions w/ gocql
	var err error
	m.sourceSession, err = utils.ConnectToCluster(cp.Conf.SourceHostname, cp.Conf.SourceUsername, cp.Conf.SourcePassword, cp.Conf.SourcePort)
	if err != nil {
		return err
	}

	m.destSession, err = utils.ConnectToCluster(cp.Conf.AstraHostname, cp.Conf.AstraUsername, cp.Conf.AstraPassword, cp.Conf.AstraPort)
	if err != nil {
		return err
	}
	return nil
}

func (m CPMetadata) prepCloudgateProxy(cp *cloudgateproxy.CloudgateProxy) {
	var err error
	// Discover schemas on source database
	err = m.getKeyspaces()
	if err != nil {
		log.WithError(err).Fatal("Failed to fetch keyspaces from source cluster")
	}

	if len(m.keyspaces) == 0 {
		log.Debug(strings.Join(m.keyspaces, ","))
		log.WithError(err).Fatal("No non system keyspaces in source cluster")
	}

	// TODO this should go
	//tables := make(map[string]map[string]*migration.Table)
	//
	//tables, err = m.getTables(m.keyspaces)
	//if err != nil {
	//	log.WithError(err).Fatal("Failed to discover table schemas from source cluster")
	//}
	//
	//p.MigrationStart <- &migration.Status{Tables: tables,
	//	Lock: &sync.Mutex{}}

}

//function for testing purposes. Will be deleted later. toggles status for table 'codebase' upon user input
func doTestingCP() {
	for {
		scanner := bufio.NewScanner(os.Stdin)
		var tables map[string]map[string]*migration.Table
		for scanner.Scan() {
			switch scanner.Text() {
			case "start":
				tables = make(map[string]map[string]*migration.Table)

				tables["codebase"] = make(map[string]*migration.Table)
				tables["codebase"]["tasks"] = &migration.Table{
					Name:     "tasks",
					Keyspace: "codebase",
					Step:     migration.MigratingSchema,
					Error:    nil,

					Lock: &sync.Mutex{},
				}

				tables["codebase"]["people"] = &migration.Table{
					Name:     "people",
					Keyspace: "codebase",
					Step:     migration.MigratingSchema,
					Error:    nil,

					Lock: &sync.Mutex{},
				}

				tables["blueprint"] = make(map[string]*migration.Table)
				tables["blueprint"]["people"] = &migration.Table{
					Name:     "people",
					Keyspace: "blueprint",
					Step:     migration.MigratingSchema,
					Error:    nil,

					Lock: &sync.Mutex{},
				}

				tables["mdb"] = make(map[string]*migration.Table)
				tables["mdb"]["people"] = &migration.Table{
					Name:     "people",
					Keyspace: "mdb",
					Step:     migration.MigratingSchema,
					Error:    nil,

					Lock: &sync.Mutex{},
				}

				//cp.MigrationStart <- &migration.Status{Tables: tables,
				//	Lock: &sync.Mutex{}}
			case "complete":
//				cp.MigrationDone <- struct{}{}
			case "shutdown":
//				cp.ShutdownChan <- struct{}{}
			}
		}
	}
}

// getKeyspaces populates m.keyspaces with the non-system keyspaces in the source cluster
func (m *CPMetadata) getKeyspaces() error {
	ignoreKeyspaces := []string{"system_auth", "system_schema", "dse_system_local", "dse_system", "dse_leases", "solr_admin",
		"dse_insights", "dse_insights_local", "system_distributed", "system", "dse_perf", "system_traces", "dse_security"}

	kQuery := `SELECT keyspace_name FROM system_schema.keyspaces;`
	itr := m.sourceSession.Query(kQuery).Iter()
	if itr == nil {
		return errors.New("Did not find any keyspaces to migrate")
	}
	var keyspaceName string
	for itr.Scan(&keyspaceName) {
		if !utils.Contains(ignoreKeyspaces, keyspaceName) {
			m.keyspaces = append(m.keyspaces, keyspaceName)
		}
	}
	return nil
}

// getTables gets table information from a keyspace in the source cluster
func (m *CPMetadata) getTables(keyspaces []string) (map[string]map[string]*migration.Table, error) {
	//tableMetadata := make(map[string]map[string]*gocql.TableMetadata)
	tableMap := make(map[string]map[string]*migration.Table)
	for _, keyspace := range keyspaces {
		md, err := m.sourceSession.KeyspaceMetadata(keyspace)

		if len(md.Tables) > 200 {
			log.Fatalf("Astra keyspaces can have 200 tables max; keyspace %s has %d tables", keyspace, len(md.Tables))
		}

		if err != nil {
			log.WithError(err).Fatalf("Failed to discover tables from keyspace %s", keyspace)
			return nil, err
		}

		for tableName := range md.Tables {

			log.Debugf("Registering table %s and keyspace %s", tableName, keyspace)
			table := &migration.Table{
				Name:     tableName,
				Keyspace: keyspace,
				Step:     migration.MigratingSchema,
				Error:    nil,

				Lock: &sync.Mutex{},
			}

			tableMap[keyspace] = make(map[string]*migration.Table)
			tableMap[keyspace][tableName] = table
		}
	}

	return tableMap, nil
}

type CPMetadata struct {
	// Sessions TODO rename these more clearly
	sourceSession *gocql.Session
	destSession   *gocql.Session

	// Data
	keyspaces []string
}
