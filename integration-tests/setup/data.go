package setup

import (
	"fmt"

	"github.com/gocql/gocql"
	log "github.com/sirupsen/logrus"
)

// Task is a struct that represents a row in the test db
type Task struct {
	ID        gocql.UUID
	Task      string
	WriteTime int64
	TTL       int
}

// DataIds is the seed id column data
var DataIds []string

// DataTasks is the seed task column data
var DataTasks []string

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
func SeedData(source *gocql.Session, dest *gocql.Session, table string, dataIds []string, dataEntries []string) {
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
	err = source.Query(fmt.Sprintf("CREATE TABLE %s.%s(id UUID, task text, PRIMARY KEY(id));", TestKeyspace, table)).Exec()
	if err != nil {
		log.WithError(err).Error("Error creating table in source cluster.")
	}

	err = dest.Query(fmt.Sprintf("CREATE TABLE %s.%s(id UUID, task text, PRIMARY KEY(id));", TestKeyspace, table)).Exec()
	if err != nil {
		log.WithError(err).Error("Error creating table in dest cluster.")
	}

	// Seed the rows
	for i := 0; i < len(dataIds); i++ {
		id, task := dataIds[i], dataEntries[i]
		err = source.Query(fmt.Sprintf("INSERT INTO %s.%s(id, task) VALUES (%s, '%s');", TestKeyspace, table, id, task)).Exec()
		if err != nil {
			log.WithError(err).Error("Error inserting into table for source cluster.")
		}
	}
}

// MapToTaskWithTimestamps converts a map loaded by Iter.MapScan() into a Task
func MapToTaskWithTimestamps(row map[string]interface{}) Task {
	return Task{
		ID:        row["id"].(gocql.UUID),
		Task:      row["task"].(string),
		WriteTime: row["w_task"].(int64),
		TTL:       row["l_task"].(int),
	}
}

// MapToTask converts a map loaded by Iter.MapScan() into a Task
func MapToTask(row map[string]interface{}) Task {
	return Task{
		ID:   row["id"].(gocql.UUID),
		Task: row["task"].(string),
	}
}

// UnloadData unloads data from the specified source session
func UnloadData(source *gocql.Session, table string) []Task {
	unloadedData := make([]Task, 0)

	query := fmt.Sprintf(`SELECT id, task, WRITETIME(task) as w_task, TTL(task) as l_task FROM cloudgate_test.%s;`, table)
	itr := source.Query(query).Iter()

	for {
		row := make(map[string]interface{})
		if !itr.MapScan(row) {
			break
		}

		unloadedData = append(unloadedData, MapToTaskWithTimestamps(row))
	}
	return unloadedData
}

// LoadData loads the given data into the specified destination session
func LoadData(dest *gocql.Session, unloadedData []Task, table string) {
	query := "BEGIN BATCH "
	for _, task := range unloadedData {
		query += fmt.Sprintf("INSERT INTO cloudgate_test.%s(id, task) VALUES (%s, '%s') USING TIMESTAMP %d AND TTL %d; ",
			table, task.ID, task.Task, task.WriteTime, task.TTL)
	}
	query += "APPLY BATCH;"
	err := dest.Query(query).Exec()
	if err != nil {
		log.Fatal(err)
	}
}
