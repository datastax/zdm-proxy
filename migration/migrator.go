package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/gorilla/mux"
)

var (
	httpServer    *http.Server
	sourceSession *gocql.Session
	destSession   *gocql.Session
	directory     string

	// StatusMap map of statuses
	StatusMap map[string]string

	// MigrationComplete boolean flag for migration complete
	MigrationComplete bool

	// Flag parameters
	keyspace            string
	sourceHost          string
	sourceUsername      string
	sourcePassword      string
	sourcePort          int
	destinationHost     string
	destinationUsername string
	destinationPassword string
	destinationPort     int
	dsbulkPath          string
	hardRestart         bool

	ignoreKeyspaces = []string{"system_auth", "system_schema", "dse_system_local", "dse_system", "dse_leases", "solr_admin",
		"dse_insights", "dse_insights_local", "system_distributed", "system", "dse_perf", "system_traces", "dse_security"}
)

func main() {
	flag.StringVar(&keyspace, "k", "", "Keyspace to migrate")
	flag.StringVar(&sourceHost, "sh", "127.0.0.1", "Source cluster hostname")
	flag.StringVar(&sourceUsername, "su", "", "Source cluster username")
	flag.StringVar(&sourcePassword, "sp", "", "Source cluster password")
	flag.IntVar(&sourcePort, "sport", 9042, "Source cluster port")
	flag.StringVar(&destinationHost, "dh", "127.0.0.1", "Destination host")
	flag.StringVar(&destinationUsername, "du", "", "Destination cluster username")
	flag.StringVar(&destinationPassword, "dp", "", "Destination cluster password")
	flag.IntVar(&destinationPort, "dport", 9042, "Destination cluster port")
	flag.StringVar(&dsbulkPath, "d", "/Users/terranceli/Documents/projects/codebase/datastax-s20/dsbulk-1.4.1/bin/dsbulk", "dsbulk executable path")
	flag.BoolVar(&hardRestart, "r", false, "Hard restart (ignore checkpoint)")
	flag.Parse()

	directory = fmt.Sprintf("./migration-%s/", strconv.FormatInt(time.Now().Unix(), 10))
	os.Mkdir(directory, 0755)

	connectionRouter := mux.NewRouter()
	connectionRouter.HandleFunc("/status", status).Methods(http.MethodGet)
	connectionRouter.HandleFunc("/abort", abort).Methods(http.MethodGet)

	StatusMap = make(map[string]string)

	sourceCluster := gocql.NewCluster(sourceHost)
	sourceCluster.Authenticator = gocql.PasswordAuthenticator{
		Username: sourceUsername,
		Password: sourcePassword,
	}
	sourceCluster.Port = sourcePort

	destCluster := gocql.NewCluster(destinationHost)
	sourceCluster.Authenticator = gocql.PasswordAuthenticator{
		Username: destinationUsername,
		Password: destinationPassword,
	}
	destCluster.Port = destinationPort

	var err error
	sourceSession, err = sourceCluster.CreateSession()
	if err != nil {
		handleError(err)
	}
	defer sourceSession.Close()

	destSession, err = destCluster.CreateSession()
	if err != nil {
		handleError(err)
	}
	defer destSession.Close()

	go migrate(keyspace)
	httpServer = &http.Server{
		Addr:           ":8080",
		Handler:        connectionRouter,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	err = httpServer.ListenAndServe()
	if err != nil {
		handleError(err)
	}
}

func contains(arr []string, s string) bool {
	for _, elem := range arr {
		if elem == s {
			return true
		}
	}
	return false
}

// Migrates a keyspace from the source cluster to the Astra cluster
func migrate(keyspace string) {
	logAndPrint(fmt.Sprintf("== MIGRATE KEYSPACE: %s ==\n", keyspace))
	chk := readCheckpoint(keyspace)

	if hardRestart {
		// clear Astra cluster of all data
		for tableName, migrated := range chk.schema {
			if migrated {
				query := fmt.Sprintf("DROP TABLE %s.%s;", keyspace, tableName)
				destSession.Query(query).Exec()
			}
		}
		os.Remove(fmt.Sprintf("./%s.chk", keyspace))
		chk = readCheckpoint(keyspace)
	}

	tables, err := getTables(keyspace)
	if err != nil {
		handleError(err)
	}

	err = loadTableNames(tables)
	if err != nil {
		handleError(err)
	}

	for _, table := range tables {
		// if we already migrated schema, skip this
		if val, ok := chk.schema[table.Name]; !(ok && val) {
			err = createTable(table)
			if err != nil {
				handleError(err)
			}
			// edit checkpoint file to include that we successfully migrated the table schema at this time
			chk.schema[table.Name] = true
			writeCheckpoint(chk, keyspace)
		}
	}

	var wg sync.WaitGroup
	var m sync.Mutex
	wg.Add(len(tables))
	for _, table := range tables {
		// if we already migrated table, skip this
		go func(table *gocql.TableMetadata) {
			defer wg.Done()
			if val, ok := chk.tables[table.Name]; !(ok && val) {
				err = migrateTable(table)
				if err != nil {
					handleError(err)
				}
				// edit checkpoint file to include that we successfully migrated table data at this time
				chk.tables[table.Name] = true
				m.Lock()
				writeCheckpoint(chk, keyspace)
				m.Unlock()
			}
		}(table)
	}
	wg.Wait()

	logAndPrint("COMPLETED MIGRATION\n")
	MigrationComplete = true
}

func createTable(table *gocql.TableMetadata) error {
	logAndPrint(fmt.Sprintf("MIGRATING TABLE SCHEMA: %s... \n", table.Name))
	StatusMap[table.Name] = "MIGRATING SCHEMA"

	query := fmt.Sprintf("CREATE TABLE %s.%s (", keyspace, table.Name)

	for cname, column := range table.Columns {
		query += fmt.Sprintf("%s %s, ", cname, column.Type.Type().String())
	}

	// partition key
	for _, column := range table.PartitionKey {
		query += fmt.Sprintf("PRIMARY KEY (%s),", column.Name)
	}

	query += ");"

	err := destSession.Query(query).Exec()

	if err != nil {
		handleError(err)
	}
	logAndPrint(fmt.Sprintf("COMPLETED MIGRATING TABLE SCHEMA: %s\n", table.Name))
	return nil
}

// Migrates a table from the source cluster to the Astra cluster
func migrateTable(table *gocql.TableMetadata) error {

	err := unloadTable(table)
	if err != nil {
		return err
	}

	err = loadTable(table)
	if err != nil {
		return err
	}

	return nil
}

// Exports a table CSV from the source cluster into DIRECTORY
func unloadTable(table *gocql.TableMetadata) error {
	logAndPrint(fmt.Sprintf("UNLOADING TABLE: %s...\n", table.Name))
	StatusMap[table.Name] = "UNLOAD IN PROGRESS"

	cmdArgs := []string{"unload", "-port", strconv.Itoa(sourcePort), "-k", table.Keyspace, "-t", table.Name, "-url", directory + table.Name, "-logDir", directory}
	_, err := exec.Command(dsbulkPath, cmdArgs...).Output()
	if err != nil {
		StatusMap[table.Name] = fmt.Sprintf("MIGRATION FAILED ERROR: %s", err)
		return err
	}

	StatusMap[table.Name] = "UNLOAD COMPLETED"
	logAndPrint(fmt.Sprintf("COMPLETED UNLOADING TABLE: %s\n", table.Name))
	return nil
}

// Loads a table from an exported CSV (in path specified by DIRECTORY)
// into the target cluster
func loadTable(table *gocql.TableMetadata) error {
	logAndPrint(fmt.Sprintf("LOADING TABLE: %s...\n", table.Name))
	StatusMap[table.Name] = "LOAD IN PROGRESS"

	cmdArgs := []string{"load", "-h", destinationHost, "-port", strconv.Itoa(destinationPort), "-k", table.Keyspace, "-t", table.Name, "-url", directory + table.Name, "-logDir", directory}
	_, err := exec.Command(dsbulkPath, cmdArgs...).Output()
	if err != nil {
		StatusMap[table.Name] = fmt.Sprintf("MIGRATION FAILED ERROR: %s", err)
		return err
	}

	StatusMap[table.Name] = "MIGRATION COMPLETED"
	logAndPrint(fmt.Sprintf("COMPLETED LOADING TABLE: %s\n", table.Name))
	return nil
}

// Loads table names into the status map for monitoring
func loadTableNames(tables map[string]*gocql.TableMetadata) error {
	for _, tableData := range tables {
		StatusMap[tableData.Name] = "WAITING"
	}

	return nil
}

// Gets table information from a keyspace in the source cluster
func getTables(keyspace string) (map[string]*gocql.TableMetadata, error) {
	// Get table metadata
	md, err := sourceSession.KeyspaceMetadata(keyspace)
	if err != nil {
		return nil, err
	}

	return md.Tables, nil
}

type checkpoint struct {
	timestamp time.Time
	schema    map[string]bool // set of table names representing successfully migrated schemas
	tables    map[string]bool // set of table names representing successfully migrated table data
}

func writeCheckpoint(chk *checkpoint, keyspace string) {
	// overwrites keyspace.chk with the given checkpoint data
	chk.timestamp = time.Now()
	schemas := ""
	tables := ""

	for schema, successful := range chk.schema {
		if successful {
			schemas += fmt.Sprintf("s:%s\n", schema)
		}
	}

	for table, successful := range chk.tables {
		if successful {
			tables += fmt.Sprintf("d:%s\n", table)
		}
	}

	str := fmt.Sprintf("%s\n\n%s\n%s", chk.timestamp.Format(time.RFC3339), schemas, tables)
	content := []byte(str)
	err := ioutil.WriteFile(fmt.Sprintf("%s.chk", keyspace), content, 0644)
	if err != nil {
		handleError(err)
	}
}

func readCheckpoint(keyspace string) *checkpoint {
	data, err := ioutil.ReadFile(fmt.Sprintf("%s.chk", keyspace))

	chk := checkpoint{
		timestamp: time.Now(),
		schema:    make(map[string]bool),
		tables:    make(map[string]bool),
	}

	if err != nil {
		return &chk
	}

	savedMigration := strings.Fields(string(data))

	timestamp, _ := time.Parse(time.RFC3339, savedMigration[0])
	chk.timestamp = timestamp
	for _, entry := range savedMigration {
		tableName := entry[2:]
		if entry[0] == 's' {
			chk.schema[tableName] = true
		} else if entry[0] == 'd' {
			chk.tables[tableName] = true
		}
	}
	return &chk
}

// API Handler for aborting the migration
func abort(w http.ResponseWriter, r *http.Request) {
	httpServer.Shutdown(context.Background())
	// TODO: stop dsbulk somehow, do what we need to do (save or wipe checkpoints)?
}

// API Handler for fetching the migration status
func status(w http.ResponseWriter, _ *http.Request) {
	marshaledStatus, err := json.Marshal(StatusMap)

	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(`{"error": "could not load migration status"}`))
		return
	}

	w.WriteHeader(200)
	w.Write(marshaledStatus)
}

var logLock sync.Mutex

func logAndPrint(s string) {
	msg := fmt.Sprintf("[%s] %s", time.Now().String(), s)
	fmt.Printf(msg)

	logLock.Lock()
	f, err := os.OpenFile(fmt.Sprintf("%slog.txt", directory),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		handleError(err)
	}
	defer f.Close()
	if _, err := f.WriteString(msg); err != nil {
		handleError(err)
	}
	logLock.Unlock()
}

func handleError(err error) {
	log.Fatal(err)
}
