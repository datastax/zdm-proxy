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

// Waiting, MigratingSchema, ... are the enums of the migration steps
const (
	Waiting = iota
	MigratingSchema
	MigratingSchemaComplete
	UnloadingData
	UnloadingDataComplete
	LoadingData
	LoadingDataComplete
	Errored
)

// Table represents status of migration of a single table
type Table struct {
	Name     string
	Step     Step
	Error    error
	Priority int
}

// Status represents status of migration
type Status struct {
	Tables     map[string]*Table
	Steps      int
	TotalSteps int
	Speed      float64
	Lock       *sync.Mutex
}

func newStatus() *Status {
	var status Status
	status.Tables = make(map[string]*Table)
	status.Lock = new(sync.Mutex)
	return &status
}

func (s *Status) initTableData(tables map[string]*gocql.TableMetadata) {
	for _, tableData := range tables {
		s.Tables[tableData.Name] = &(Table{
			Name:     tableData.Name,
			Step:     Waiting,
			Error:    nil,
			Priority: 0,
		})
	}
}

// checkpoint is used to save the progress of migration,
// and restart migration in case of failure
type checkpoint struct {
	timestamp time.Time
	schema    map[string]bool // set of table names representing successfully migrated schemas
	tables    map[string]bool // set of table names representing successfully migrated table data
	complete  bool
}

// ignoreKeyspaces := []string{"system_auth", "system_schema", "dse_system_local", "dse_system", "dse_leases", "solr_admin",
// 		"dse_insights", "dse_insights_local", "system_distributed", "system", "dse_perf", "system_traces", "dse_security"}

// Migration ...
type Migration struct {
	Keyspace    string
	DsbulkPath  string
	HardRestart bool

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
}

func (m *Migration) Init() error {
	m.status = newStatus()
	m.directory = fmt.Sprintf("./migration-%s/", strconv.FormatInt(time.Now().Unix(), 10))
	os.Mkdir(m.directory, 0755)

	var err error
	m.sourceSession, err = utils.ConnectToCluster(m.SourceHostname, m.SourceUsername, m.SourcePassword, m.SourcePort)
	if err != nil {
		return err
	}
	// defer m.sourceSession.Close()

	m.destSession, err = utils.ConnectToCluster(m.DestHostname, m.DestUsername, m.DestPassword, m.DestPort)
	if err != nil {
		return err
	}
	// defer m.destSession.Close()

	return nil
}

// Migrates a keyspace from the source cluster to the Astra cluster
func (m *Migration) Migrate() {
	m.logAndPrint(fmt.Sprintf("== MIGRATE KEYSPACE: %s ==\n", m.Keyspace))
	chk := readCheckpoint(m.Keyspace)

	if m.HardRestart {
		// clear Astra cluster of all data
		for tableName, migrated := range chk.schema {
			if migrated {
				query := fmt.Sprintf("DROP TABLE %s.%s;", m.Keyspace, tableName)
				m.destSession.Query(query).Exec()
			}
		}

		// clear the checkpoint
		os.Remove(fmt.Sprintf("./%s.chk", m.Keyspace))
		chk = readCheckpoint(m.Keyspace)
	}

	begin := time.Now()

	if chk.complete {
		m.status.Steps = -1
		m.status.TotalSteps = -1
		m.status.Speed = -1
		for _, table := range m.status.Tables {
			table.Step = LoadingDataComplete
			table.Error = nil
		}
		m.logAndPrint("COMPLETED MIGRATION\n")
		return
	}

	tables, err := m.getTables()
	if err != nil {
		log.Fatal(err)
	}

	// Each table load/unload
	m.status.TotalSteps = 3 * len(tables)
	m.status.initTableData(tables)

	var wgSchema sync.WaitGroup
	wgSchema.Add(len(tables))
	for _, table := range tables {
		// if we already migrated schema, skip this
		go func(table *gocql.TableMetadata) {
			defer wgSchema.Done()
			if val, ok := chk.schema[table.Name]; !(ok && val) {
				err = m.migrateSchema(table)
				if err != nil {
					log.Fatal(err)
				}
				// edit checkpoint file to include that we successfully migrated the table schema at this time
				chk.schema[table.Name] = true
				writeCheckpoint(chk, m.Keyspace)
			}
			m.status.Steps++
			m.status.Tables[table.Name].Step = MigratingSchemaComplete
			m.logAndPrint(fmt.Sprintf("COMPLETED MIGRATING TABLE SCHEMA: %s\n", table.Name))
		}(table)
	}
	wgSchema.Wait()

	var wgTables sync.WaitGroup
	wgTables.Add(len(tables))
	for _, table := range tables {
		// if we already migrated table, skip this
		go func(table *gocql.TableMetadata) {
			defer wgTables.Done()
			if val, ok := chk.tables[table.Name]; !(ok && val) {
				err = m.migrateData(table)
				if err != nil {
					log.Fatal(err)
				}
				// edit checkpoint file to include that we successfully migrated table data at this time
				chk.tables[table.Name] = true
				writeCheckpoint(chk, m.Keyspace)
			}
			m.status.Steps += 2
			m.status.Tables[table.Name].Step = LoadingDataComplete
			m.logAndPrint(fmt.Sprintf("COMPLETED LOADING TABLE DATA: %s\n", table.Name))
		}(table)
	}
	wgTables.Wait()

	m.logAndPrint("COMPLETED MIGRATION\n")

	// calculate total file size of migrated data
	var size int64
	for table := range tables {
		dSize, _ := utils.DirSize(m.directory + table)
		size += dSize
	}

	// calculate average speed in megabytes per second
	m.status.Speed = (float64(size) / 1024 / 1024) / (float64(time.Now().UnixNano()-begin.UnixNano()) / 1000000000)

	fmt.Println(m.status)
	chk.complete = true
	writeCheckpoint(chk, m.Keyspace)
}

func (m *Migration) migrateSchema(table *gocql.TableMetadata) error {
	m.status.Tables[table.Name].Step = MigratingSchema
	m.logAndPrint(fmt.Sprintf("MIGRATING TABLE SCHEMA: %s... \n", table.Name))

	query := fmt.Sprintf("CREATE TABLE %s.%s (", m.Keyspace, table.Name)

	for cname, column := range table.Columns {
		query += fmt.Sprintf("%s %s, ", cname, column.Type.Type().String())
	}

	// partition key
	for _, column := range table.PartitionKey {
		query += fmt.Sprintf("PRIMARY KEY (%s),", column.Name)
	}

	query += ") "

	// compaction
	cQuery := `SELECT compaction FROM system_schema.tables
					WHERE keyspace_name = ? and table_name = ?;`
	cMap := make(map[string]interface{})
	cString := "{"
	itr := m.sourceSession.Query(cQuery, m.Keyspace, table.Name).Iter()
	itr.MapScan(cMap)

	compactionMap := (cMap["compaction"]).(map[string]string)

	for key, value := range compactionMap {
		cString += fmt.Sprintf("'%s': '%s', ", key, value)
	}
	cString = cString[0:(len(cString)-2)] + "}"
	query += fmt.Sprintf("WITH compaction = %s;", cString)

	err := m.destSession.Query(query).Exec()

	if err != nil {
		m.status.Tables[table.Name].Step = Errored
		m.status.Tables[table.Name].Error = err
		return err
	}
	return nil
}

// Migrates a table from the source cluster to the Astra cluster
func (m *Migration) migrateData(table *gocql.TableMetadata) error {

	err := m.unloadTable(table)
	if err != nil {
		return err
	}

	err = m.loadTable(table)
	if err != nil {
		return err
	}
	return nil
}

// Exports a table CSV from the source cluster into DIRECTORY
func (m *Migration) unloadTable(table *gocql.TableMetadata) error {
	m.status.Tables[table.Name].Step = UnloadingData
	m.logAndPrint(fmt.Sprintf("UNLOADING TABLE: %s...\n", table.Name))

	cmdArgs := []string{"unload", "-port", strconv.Itoa(m.SourcePort), "-k", m.Keyspace, "-t", table.Name, "-url", m.directory + table.Name, "-logDir", m.directory}
	_, err := exec.Command(m.DsbulkPath, cmdArgs...).Output()
	if err != nil {
		m.status.Tables[table.Name].Step = Errored
		m.status.Tables[table.Name].Error = err
		return err
	}

	m.status.Tables[table.Name].Step = UnloadingDataComplete
	m.logAndPrint(fmt.Sprintf("COMPLETED UNLOADING TABLE: %s\n", table.Name))
	return nil
}

// Loads a table from an exported CSV (in path specified by DIRECTORY)
// into the target cluster
func (m *Migration) loadTable(table *gocql.TableMetadata) error {
	m.status.Tables[table.Name].Step = LoadingData
	m.logAndPrint(fmt.Sprintf("LOADING TABLE: %s...\n", table.Name))

	cmdArgs := []string{"load", "-h", m.DestHostname, "-port", strconv.Itoa(m.DestPort), "-k", m.Keyspace, "-t", table.Name, "-url", m.directory + table.Name, "-logDir", m.directory}
	_, err := exec.Command(m.DsbulkPath, cmdArgs...).Output()
	if err != nil {
		m.status.Tables[table.Name].Step = Errored
		m.status.Tables[table.Name].Error = err
		return err
	}

	return nil
}

// Gets table information from a keyspace in the source cluster
func (m *Migration) getTables() (map[string]*gocql.TableMetadata, error) {
	// Get table metadata
	md, err := m.sourceSession.KeyspaceMetadata(m.Keyspace)
	if err != nil {
		return nil, err
	}

	return md.Tables, nil
}

var chkLock sync.Mutex

func writeCheckpoint(chk *checkpoint, keyspace string) {
	// overwrites keyspace.chk with the given checkpoint data
	chkLock.Lock()
	defer chkLock.Unlock()
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

	if chk.complete {
		str += "c:\n"
	}

	content := []byte(str)
	err := ioutil.WriteFile(fmt.Sprintf("%s.chk", keyspace), content, 0644)
	if err != nil {
		log.Fatal(err)
	}
}

func readCheckpoint(keyspace string) *checkpoint {
	chkLock.Lock()
	defer chkLock.Unlock()
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
		} else if entry[0] == 'c' {
			chk.complete = true
		}
	}
	return &chk
}

var logLock sync.Mutex

func (m *Migration) logAndPrint(s string) {
	msg := fmt.Sprintf("[%s] %s", time.Now().String(), s)
	fmt.Printf(msg)

	logLock.Lock()
	defer logLock.Unlock()
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
