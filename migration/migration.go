package migration

import (
	"cloud-gate/utils"

	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

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
	Timestamp	 time.Time
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
		s.Tables[keyspace] = new(map[string]*Table)
		for table, tableData := range *keyspaceTables {
			s.Tables[keyspace][table] = &(Table{
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

	MigrationStartChan chan *Status
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
func (m *Migration) Migrate() {
	if !m.initialized {
		panic("Migration must be initialized before migration can begin")
	}

	m.logAndPrint(fmt.Sprintf("== BEGIN MIGRATION ==\n"))

	defer m.sourceSession.Close()
	defer m.destSession.Close()
	
	keyspaces, err := m.getKeyspaces()
	if err != nil {
		log.Fatal(err)
	}
	
	tables, err := m.getTables(keyspaces)
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

	var wgSchema sync.WaitGroup
	wgSchema.Add(len(tables)) // TODO: what does this do and how to fix?
	for keyspace, keyspaceTables := range tables {
		for tableName, table := range *keyspaceTables {
			// if we already migrated schema, skip this
			go func(table *gocql.TableMetadata) {
				defer wgSchema.Done()
				if (*m.status.Tables[keyspace])[tableName].Step < MigratingSchemaComplete {
					err = m.migrateSchema(keyspace, table)
					if err != nil {
						log.Fatal(err)
					}
	
					m.status.Steps++
					(*m.status.Tables[keyspace])[table.Name].Step = MigratingSchemaComplete
					m.logAndPrint(fmt.Sprintf("COMPLETED MIGRATING TABLE SCHEMA: %s\n", table.Name))
					m.writeCheckpoint()
				}
			}(table)
		}
	}
	wgSchema.Wait()

	// Notify proxy service that schemas are finished migrating, unload/load starting
	m.MigrationStartChan <- m.status

	var wgTables sync.WaitGroup
	wgTables.Add(len(tables)) // TODO: what does this do and how to fix?
	for keyspace, keyspaceTables := range tables {
		for tableName, table := range *keyspaceTables {
			// if we already migrated table, skip this
			go func(table *gocql.TableMetadata) {
				defer wgTables.Done()
				if (*m.status.Tables[keyspace])[tableName].Step != LoadingDataComplete {
					err = m.migrateData(keyspace, table)
					if err != nil {
						log.Fatal(err)
					}
	
					m.status.Steps += 2
					(*m.status.Tables[keyspace])[tableName].Step = LoadingDataComplete
					m.logAndPrint(fmt.Sprintf("COMPLETED LOADING TABLE DATA: %s\n", table.Name))
					m.writeCheckpoint()
				}
				
			}(table)
		}
	}
	wgTables.Wait()

	m.logAndPrint("COMPLETED MIGRATION\n")

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

	fmt.Println(m.status)
	m.writeCheckpoint()
	m.MigrationCompleteChan <- struct{}{}
}

// migrateSchema creates each new table in Astra with
// column names and types, primary keys, and compaction information
func (m *Migration) migrateSchema(keyspace string, table *gocql.TableMetadata) error {
	(*m.status.Tables[keyspace])[table.Name].Step = MigratingSchema
	m.logAndPrint(fmt.Sprintf("MIGRATING TABLE SCHEMA: %s... \n", table.Name))

	query := fmt.Sprintf("CREATE TABLE %s.%s (", keyspace, table.Name)

	for cname, column := range table.Columns {
		query += fmt.Sprintf("%s %s, ", cname, column.Type.Type().String())
	}

	for _, column := range table.PartitionKey {
		query += fmt.Sprintf("PRIMARY KEY (%s),", column.Name)
	}

	query += ") "

	// compaction
	cQuery := `SELECT compaction FROM system_schema.tables
					WHERE keyspace_name = ? and table_name = ?;`
	cMap := make(map[string]interface{})
	cString := "{"
	itr := m.sourceSession.Query(cQuery, keyspace, table.Name).Iter()
	itr.MapScan(cMap)

	compactionMap := (cMap["compaction"]).(map[string]string)

	for key, value := range compactionMap {
		cString += fmt.Sprintf("'%s': '%s', ", key, value)
	}
	cString = cString[0:(len(cString)-2)] + "}"
	query += fmt.Sprintf("WITH compaction = %s;", cString)

	err := m.destSession.Query(query).Exec()

	if err != nil {
		(*m.status.Tables[keyspace])[table.Name].Step = Errored
		(*m.status.Tables[keyspace])[table.Name].Error = err
		return err
	}
	return nil
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
	m.logAndPrint(fmt.Sprintf("UNLOADING TABLE: %s...\n", table.Name))

	cmdArgs := []string{"unload", "-port", strconv.Itoa(m.SourcePort), "-k", keyspace, "-t", table.Name, "-url", m.directory + keyspace + "." + table.Name, "-logDir", m.directory}
	_, err := exec.Command(m.DsbulkPath, cmdArgs...).Output()
	if err != nil {
		(*m.status.Tables[keyspace])[table.Name].Step = Errored
		(*m.status.Tables[keyspace])[table.Name].Error = err
		return err
	}

	(*m.status.Tables[keyspace])[table.Name].Step = UnloadingDataComplete
	m.logAndPrint(fmt.Sprintf("COMPLETED UNLOADING TABLE: %s\n", table.Name))
	return nil
}

// loadTable loads a table from an exported CSV (in path specified by DIRECTORY)
// into the target cluster
func (m *Migration) loadTable(keyspace string, table *gocql.TableMetadata) error {
	(*m.status.Tables[keyspace])[table.Name].Step = LoadingData
	m.logAndPrint(fmt.Sprintf("LOADING TABLE: %s...\n", table.Name))

	cmdArgs := []string{"load", "-h", m.DestHostname, "-port", strconv.Itoa(m.DestPort), "-k", keyspace, "-t", table.Name, "-url", m.directory + keyspace + "." + table.Name, "-logDir", m.directory}
	_, err := exec.Command(m.DsbulkPath, cmdArgs...).Output()
	if err != nil {
		(*m.status.Tables[keyspace])[table.Name].Step = Errored
		(*m.status.Tables[keyspace])[table.Name].Error = err
		return err
	}

	return nil
}

func (m *Migration) getKeyspaces() ([]string, error) {
	kQuery := `SELECT keyspace_name FROM system_schema.keyspaces;`
	kMap := make(map[string]interface{})
	itr := m.sourceSession.Query(kQuery).Iter()
	itr.MapScan(kMap)

	names := (kMap["keyspace_name"]).([]string)
	// ignoreKeyspaces is for testing and omitting system keyspaces
	ignoreKeyspaces := []string{"system_auth", "system_schema", "dse_system_local", "dse_system", "dse_leases", "solr_admin",
	"dse_insights", "dse_insights_local", "system_distributed", "system", "dse_perf", "system_traces", "dse_security"}
	
	for _, name := range names {
		if (!utils.Contains(ignoreKeyspaces, name)) {
			m.Keyspaces = append(m.Keyspaces, ) // TODO:
		}
	}
}

// getTables gets table information from a keyspace in the source cluster
func (m *Migration) getTables(keyspaces []string) (map[string]*map[string]*gocql.TableMetadata, error) {
	tableMetadata := make(map[string]*map[string]*gocql.TableMetadata)
	for _, keyspace := range keyspaces {
		md, err := m.sourceSession.KeyspaceMetadata(keyspace)
		if (err != nil) {
			m.logAndPrint(fmt.Sprintf("ERROR GETTING KEYSPACE %s...\n", keyspace))
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
				data += fmt.Sprintf("s:%s\n", keyspace + "." + tableName)
			}
	
			if table.Step == LoadingDataComplete {
				data += fmt.Sprintf("d:%s\n", keyspace + "." + tableName)
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
	for _, entry := range savedMigration {
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

func (m *Migration) logAndPrint(s string) {
	msg := fmt.Sprintf("[%s] %s", time.Now().String(), s)
	fmt.Printf(msg)

	m.logLock.Lock()
	defer m.logLock.Unlock()
	f, err := os.OpenFile(fmt.Sprintf("%slog.txt", m.directory),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(msg); err != nil {
		log.Fatal(err)
	}
}
