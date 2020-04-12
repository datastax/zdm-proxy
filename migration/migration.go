package migration

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud-gate/utils"

	"github.com/gocql/gocql"
)

// Step is an enum of the migration steps for a table
type Step int

// Errored, Waiting, MigratingSchema, ... are the enums of the migration steps
const (
	Errored = iota
	Waiting
	MigratingSchema
	MigratingSchemaComplete
	UnloadingData
	UnloadingDataComplete
	LoadingData
	LoadingDataComplete
)

// Table represents status of migration of a single table
type Table struct {
	Keyspace string
	Name     string
	Step     Step
	Error    error
	Priority int
}

// Status represents status of migration
type Status struct {
	Timestamp  time.Time
	Tables     map[string]*map[string]*Table
	Steps      int
	TotalSteps int
	Speed      float64
	Lock       *sync.Mutex
}

// Status constructor
func newStatus() *Status {
	var status Status
	status.Tables = make(map[string]*map[string]*Table)
	status.Lock = new(sync.Mutex)
	return &status
}

// Populates the Status with initial values in accordance w/ the given TableMetadata
func (s *Status) initTableData(tables map[string]*map[string]*gocql.TableMetadata) {
	s.TotalSteps = 0
	for keyspace, keyspaceTables := range tables {
		s.TotalSteps += 3 * len(*keyspaceTables)
		t := make(map[string]*Table)
		s.Tables[keyspace] = &t
		for table := range *keyspaceTables {
			(*s.Tables[keyspace])[table] = &(Table{
				Name:     table,
				Step:     Waiting,
				Error:    nil,
				Priority: 0,
			})
		}
	}
}

// Migration contains necessary setup information
type Migration struct {
	Keyspaces   []string
	DsbulkPath  string
	HardRestart bool
	Workers     int

	MigrationStartChan    chan *Status
	MigrationCompleteChan chan struct{}

	SourceHostname string
	SourceUsername string
	SourcePassword string
	SourcePort     int

	DestHostname string
	DestUsername string
	DestPassword string
	DestPort     int

	status        *Status
	directory     string
	sourceSession *gocql.Session
	destSession   *gocql.Session
	chkLock       *sync.Mutex
	logLock       *sync.Mutex
	initialized   bool
}

// Init creates the connections to the databases and locks needed for checkpoint and logging
func (m *Migration) Init() error {
	m.status = newStatus()
	m.directory = fmt.Sprintf("./migration-%s/", strconv.FormatInt(time.Now().Unix(), 10))
	os.Mkdir(m.directory, 0755)

	var err error
	m.sourceSession, err = utils.ConnectToCluster(m.SourceHostname, m.SourceUsername, m.SourcePassword, m.SourcePort)
	if err != nil {
		return err
	}

	m.destSession, err = utils.ConnectToCluster(m.DestHostname, m.DestUsername, m.DestPassword, m.DestPort)
	if err != nil {
		return err
	}

	m.chkLock = new(sync.Mutex)
	m.logLock = new(sync.Mutex)
	m.initialized = true

	return nil
}

// Migrate executes the migration
func (m *Migration) Migrate() error {
	if !m.initialized {
		return errors.New("Migration must be initialized before migration can begin")
	}

	print(fmt.Sprintf("== BEGIN MIGRATION ==\n"))

	defer m.sourceSession.Close()
	defer m.destSession.Close()

	m.getKeyspaces()
	tables, err := m.getTables(m.Keyspaces)
	if err != nil {
		log.Fatal(err)
	}

	m.status.initTableData(tables)
	m.readCheckpoint()

	if m.HardRestart {
		// On hard restart, clear Astra cluster of all data
		for keyspace, keyspaceTables := range m.status.Tables {
			for tableName, table := range *keyspaceTables {
				if table.Step >= MigratingSchemaComplete {
					query := fmt.Sprintf("DROP TABLE %s.%s;", keyspace, tableName)
					m.destSession.Query(query).Exec()
				}
			}
		}

		os.Remove("./migration.chk")
		m.readCheckpoint()
	}

	begin := time.Now()
	schemaJobs := make(chan *gocql.TableMetadata, len(tables))
	tableJobs := make(chan *gocql.TableMetadata, len(tables))
	var wgSchema sync.WaitGroup
	var wgTables sync.WaitGroup

	for worker := 1; worker <= m.Workers; worker++ {
		go m.schemaPool(worker, &wgSchema, schemaJobs)
		go m.tablePool(worker, &wgTables, tableJobs)
	}

	for _, keyspaceTables := range tables {
		for _, table := range *keyspaceTables {
			wgSchema.Add(1)
			schemaJobs <- table
		}
	}
	close(schemaJobs)
	wgSchema.Wait()
	// Notify proxy service that schemas are finished migrating, unload/load starting
	m.MigrationStartChan <- m.status

	for _, keyspaceTables := range tables {
		for _, table := range *keyspaceTables {
			wgTables.Add(1)
			tableJobs <- table
		}
	}

	close(tableJobs)
	wgTables.Wait()
	print("COMPLETED MIGRATION\n")

	// calculate total file size of migrated data
	var size int64
	for keyspace, keyspaceTables := range tables {
		for tableName := range *keyspaceTables {
			dSize, _ := utils.DirSize(m.directory + keyspace + "." + tableName)
			size += dSize
		}
	}

	// calculate average speed in megabytes per second
	m.status.Speed = (float64(size) / 1024 / 1024) / (float64(time.Now().UnixNano()-begin.UnixNano()) / 1000000000)

	m.writeCheckpoint()
	m.MigrationCompleteChan <- struct{}{}
	return nil
}

func (m *Migration) schemaPool(worker int, wg *sync.WaitGroup, jobs <-chan *gocql.TableMetadata) {
	for table := range jobs {
		// if we already migrated schema, skip this
		if (*m.status.Tables[table.Keyspace])[table.Name].Step < MigratingSchemaComplete {
			err := m.migrateSchema(table.Keyspace, table)
			if err != nil {
				log.Fatal(err)
			}

			m.status.Steps++
			(*m.status.Tables[table.Keyspace])[table.Name].Step = MigratingSchemaComplete
			print(fmt.Sprintf("COMPLETED MIGRATING TABLE SCHEMA: %s.%s\n", table.Keyspace, table.Name))
			m.writeCheckpoint()
		}
		wg.Done()
	}
}

func generateSchemaMigrationQuery(table *gocql.TableMetadata, compactionMap map[string]string) string {
	query := fmt.Sprintf("CREATE TABLE %s.%s (", table.Keyspace, table.Name)
	for cname, column := range table.Columns {
		query += fmt.Sprintf("%s %s, ", cname, column.Type.Type().String())
	}

	for _, column := range table.PartitionKey {
		query += fmt.Sprintf("PRIMARY KEY (%s),", column.Name)
	}

	query += ") "

	cString := "{"
	for key, value := range compactionMap {
		cString += fmt.Sprintf("'%s': '%s', ", key, value)
	}
	cString = cString[0:(len(cString)-2)] + "}"
	query += fmt.Sprintf("WITH compaction = %s;", cString)

	return query
}

// migrateSchema creates each new table in Astra with
// column names and types, primary keys, and compaction information
func (m *Migration) migrateSchema(keyspace string, table *gocql.TableMetadata) error {
	(*m.status.Tables[keyspace])[table.Name].Step = MigratingSchema
	print(fmt.Sprintf("MIGRATING TABLE SCHEMA: %s.%s... \n", table.Keyspace, table.Name))

	// Fetch table compaction metadata
	cQuery := `SELECT compaction FROM system_schema.tables
					WHERE keyspace_name = ? and table_name = ?;`
	cMap := make(map[string]interface{})
	itr := m.sourceSession.Query(cQuery, keyspace, table.Name).Iter()
	itr.MapScan(cMap)

	compactionMap := (cMap["compaction"]).(map[string]string)

	query := generateSchemaMigrationQuery(table, compactionMap)
	err := m.destSession.Query(query).Exec()

	if err != nil {
		(*m.status.Tables[keyspace])[table.Name].Step = Errored
		(*m.status.Tables[keyspace])[table.Name].Error = err
		return err
	}
	return nil
}

func (m *Migration) tablePool(worker int, wg *sync.WaitGroup, jobs <-chan *gocql.TableMetadata) {
	for table := range jobs {
		if (*m.status.Tables[table.Keyspace])[table.Name].Step != LoadingDataComplete {
			err := m.migrateData(table.Keyspace, table)
			if err != nil {
				log.Fatal(err)
			}

			m.status.Steps += 2
			(*m.status.Tables[table.Keyspace])[table.Name].Step = LoadingDataComplete
			print(fmt.Sprintf("COMPLETED LOADING TABLE DATA: %s.%s\n", table.Keyspace, table.Name))
			m.writeCheckpoint()
		}
		wg.Done()
	}
}

// migrateData migrates a table from the source cluster to the Astra cluster
func (m *Migration) migrateData(keyspace string, table *gocql.TableMetadata) error {

	err := m.unloadTable(keyspace, table)
	if err != nil {
		return err
	}

	err = m.loadTable(keyspace, table)
	if err != nil {
		return err
	}
	return nil
}

// unloadTable exports a table CSV from the source cluster into DIRECTORY
func (m *Migration) unloadTable(keyspace string, table *gocql.TableMetadata) error {
	(*m.status.Tables[keyspace])[table.Name].Step = UnloadingData
	print(fmt.Sprintf("UNLOADING TABLE: %s.%s...\n", table.Keyspace, table.Name))

	cmdArgs := []string{"unload", "-port", strconv.Itoa(m.SourcePort), "-k", keyspace, "-t", table.Name, "-url", m.directory + keyspace + "." + table.Name, "-logDir", m.directory}
	_, err := exec.Command(m.DsbulkPath, cmdArgs...).Output()
	if err != nil {
		(*m.status.Tables[keyspace])[table.Name].Step = Errored
		(*m.status.Tables[keyspace])[table.Name].Error = err
		return err
	}

	(*m.status.Tables[keyspace])[table.Name].Step = UnloadingDataComplete
	print(fmt.Sprintf("COMPLETED UNLOADING TABLE: %s.%s\n", table.Keyspace, table.Name))
	return nil
}

// loadTable loads a table from an exported CSV (in path specified by DIRECTORY)
// into the target cluster
func (m *Migration) loadTable(keyspace string, table *gocql.TableMetadata) error {
	(*m.status.Tables[keyspace])[table.Name].Step = LoadingData
	print(fmt.Sprintf("LOADING TABLE: %s.%s...\n", table.Keyspace, table.Name))

	cmdArgs := []string{"load", "-h", m.DestHostname, "-port", strconv.Itoa(m.DestPort), "-k", keyspace, "-t", table.Name, "-url", m.directory + keyspace + "." + table.Name, "-logDir", m.directory}
	_, err := exec.Command(m.DsbulkPath, cmdArgs...).Output()
	if err != nil {
		(*m.status.Tables[keyspace])[table.Name].Step = Errored
		(*m.status.Tables[keyspace])[table.Name].Error = err
		return err
	}

	return nil
}

func (m *Migration) getKeyspaces() {
	ignoreKeyspaces := []string{"system_auth", "system_schema", "dse_system_local", "dse_system", "dse_leases", "solr_admin",
		"dse_insights", "dse_insights_local", "system_distributed", "system", "dse_perf", "system_traces", "dse_security"}

	kQuery := `SELECT keyspace_name FROM system_schema.keyspaces;`
	itr := m.sourceSession.Query(kQuery).Iter()

	var keyspaceName string
	for itr.Scan(&keyspaceName) {
		if !utils.Contains(ignoreKeyspaces, keyspaceName) {
			m.Keyspaces = append(m.Keyspaces, keyspaceName)
		}
	}
}

// getTables gets table information from a keyspace in the source cluster
func (m *Migration) getTables(keyspaces []string) (map[string]*map[string]*gocql.TableMetadata, error) {
	tableMetadata := make(map[string]*map[string]*gocql.TableMetadata)
	for _, keyspace := range keyspaces {
		md, err := m.sourceSession.KeyspaceMetadata(keyspace)
		if err != nil {
			print(fmt.Sprintf("ERROR GETTING KEYSPACE %s...\n", keyspace))
			log.Fatal(err)
		}
		tableMetadata[keyspace] = &md.Tables
	}
	return tableMetadata, nil
}

// writeCheckpoint overwrites migration.chk with the given checkpoint data
func (m *Migration) writeCheckpoint() {
	m.chkLock.Lock()
	defer m.chkLock.Unlock()
	m.status.Timestamp = time.Now()
	data := ""

	for keyspace, keyspaceTables := range m.status.Tables {
		for tableName, table := range *keyspaceTables {
			if table.Step >= MigratingSchemaComplete {
				data += fmt.Sprintf("s:%s\n", keyspace+"."+tableName)
			}

			if table.Step == LoadingDataComplete {
				data += fmt.Sprintf("d:%s\n", keyspace+"."+tableName)
			}
		}
	}

	str := fmt.Sprintf("%s\n\n%s", m.status.Timestamp.Format(time.RFC3339), data)

	content := []byte(str)
	err := ioutil.WriteFile("migration.chk", content, 0644)
	if err != nil {
		log.Fatal(err)
	}
}

// readCheckpoint fills in the Status w/ the checkpoint on file
func (m *Migration) readCheckpoint() {
	m.chkLock.Lock()
	defer m.chkLock.Unlock()
	data, err := ioutil.ReadFile("migration.chk")

	m.status.Timestamp = time.Now()

	if err != nil {
		return
	}

	savedMigration := strings.Fields(string(data))

	m.status.Timestamp, _ = time.Parse(time.RFC3339, savedMigration[0])
	for _, entry := range savedMigration[1:] {
		tableNameTokens := strings.Split(entry[2:], ".")
		keyspace := tableNameTokens[0]
		tableName := tableNameTokens[1]
		if entry[0] == 's' {
			(*m.status.Tables[keyspace])[tableName].Step = MigratingSchemaComplete
			m.status.Steps++
		} else if entry[0] == 'd' {
			(*m.status.Tables[keyspace])[tableName].Step = LoadingDataComplete
			m.status.Steps += 2
		}
	}
}

func print(s string) {
	msg := fmt.Sprintf("[%s] %s", time.Now().String(), s)
	fmt.Printf(msg)
}
