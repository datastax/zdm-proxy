package setup

import (
	"fmt"

	"github.com/apache/cassandra-gocql-driver/v2"
	log "github.com/sirupsen/logrus"
)

// Task is a struct that represents a row in the test db
type Task struct {
	ID        gocql.UUID
	Task      string
	WriteTime int64
	TTL       int
}

// SeedKeyspace seeds the specified source and dest sessions with TestKeyspace
func SeedKeyspace(session *gocql.Session) error {
	log.Info("Seeding keyspace...")
	err := session.Query(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};", TestKeyspace)).Exec()
	if err != nil {
		return fmt.Errorf("failed to seed keyspace: %w", err)
	}

	return nil
}

// SeedData seeds the specified source and dest sessions with data in data.go
// Currently, this includes DataIds and DataTasks
func SeedData(source *gocql.Session, dest *gocql.Session, table string, data [][]string) {
	log.Info("Drop existing data...")
	// Create the table in source
	err := source.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s;", TestKeyspace, table)).Exec()
	if err != nil {
		log.WithError(err).Error("Error dropping table in source cluster.")
	}

	err = dest.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s;", TestKeyspace, table)).Exec()
	if err != nil {
		log.WithError(err).Error("Error dropping table in dest cluster.")
	}

	log.Info("Seeding tables...")
	// Create the table in source
	for _, statement := range DataModels[table].SchemaDefinition(TestKeyspace) {
		err = source.Query(statement).Exec()
		if err != nil {
			log.WithError(err).Error("Error creating table in source cluster.")
		}
	}

	for _, statement := range DataModels[table].SchemaDefinition(TestKeyspace) {
		err = dest.Query(statement).Exec()
		if err != nil {
			log.WithError(err).Error("Error creating table in dest cluster.")
		}
	}

	// Seed the rows
	for _, statement := range DataModels[table].DataSeed(TestKeyspace, data) {
		err = source.Query(statement).Exec()
		if err != nil {
			log.WithError(err).Error("Error inserting into table for source cluster.")
		}
	}
}

// MapToTask converts a map loaded by Iter.MapScan() into a Task
func MapToTask(row map[string]interface{}) Task {
	return Task{
		ID:   row["id"].(gocql.UUID),
		Task: row["task"].(string),
	}
}

type DataModel interface {
	SchemaDefinition(keyspace string) []string
	DataSeed(keyspace string, params [][]string) []string
}

type SimpleDataModel struct {
	table string
}

func (s SimpleDataModel) SchemaDefinition(keyspace string) []string {
	return []string{fmt.Sprintf("CREATE TABLE %s.%s(id UUID, task text, PRIMARY KEY(id));", keyspace, s.table)}
}

func (s SimpleDataModel) DataSeed(keyspace string, params [][]string) []string {
	stmts := make([]string, len(params))
	for i, param := range params {
		stmt := fmt.Sprintf("INSERT INTO %s.%s(id, task) VALUES (%s, '%s');", keyspace, s.table, param[0], param[1])
		stmts[i] = stmt
	}
	return stmts
}

type SearchDataModel struct {
	SimpleDataModel
}

func (s SearchDataModel) SchemaDefinition(keyspace string) []string {
	return []string{
		fmt.Sprintf("CREATE TABLE %s.%s(id UUID, task text, PRIMARY KEY(id));", keyspace, s.table),
		fmt.Sprintf("CREATE CUSTOM INDEX %s_sai_idx ON %s.%s(task) USING 'StorageAttachedIndex' WITH OPTIONS = {'case_sensitive': 'false'};", s.table, keyspace, s.table),
	}
}
