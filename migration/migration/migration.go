package migration

import (
	"cloud-gate/updates"
	"cloud-gate/utils"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"
)

// Migration contains necessary setup information
type Migration struct {
	Conf      *Config
	Keyspaces []string

	conn               net.Conn
	status             *Status
	directory          string
	sourceSession      *gocql.Session
	destSession        *gocql.Session
	chkLock            *sync.Mutex
	logLock            *sync.Mutex
	outstandingUpdates map[string]*updates.Update
	updateLock         *sync.Mutex
	initialized        bool
}

// Init creates the connections to the databases and locks needed for checkpoint and logging
func (m *Migration) Init() error {
	m.status = newStatus()
	m.directory = fmt.Sprintf("./migration-%s/", strconv.FormatInt(time.Now().Unix(), 10))
	os.Mkdir(m.directory, 0755)

	var err error
	m.sourceSession, err = utils.ConnectToCluster(m.Conf.SourceHostname, m.Conf.SourceUsername, m.Conf.SourcePassword, m.Conf.SourcePort)
	if err != nil {
		return err
	}

	m.destSession, err = utils.ConnectToCluster(m.Conf.AstraHostname, m.Conf.AstraUsername, m.Conf.AstraPassword, m.Conf.AstraPort)
	if err != nil {
		return err
	}

	m.chkLock = new(sync.Mutex)
	m.logLock = new(sync.Mutex)
	m.updateLock = new(sync.Mutex)
	m.outstandingUpdates = make(map[string]*updates.Update)
	m.initialized = true

	return nil
}

// Migrate executes the migration
func (m *Migration) Migrate() {
	if !m.initialized {
		log.Fatal("Migration must be initialized before migration can begin")
	}

	log.Info(fmt.Sprintf("== BEGIN MIGRATION =="))

	defer m.sourceSession.Close()
	defer m.destSession.Close()

	err := m.getKeyspaces()
	if err != nil {
		log.WithError(err).Fatal(err)
	}
	tables, err := m.getTables(m.Keyspaces)
	if err != nil {
		log.WithError(err).Fatal(err)
	}

	m.status.initTableData(tables)
	m.readCheckpoint()

	if m.Conf.HardRestart {
		// On hard restart, clear Astra cluster of all data
		for keyspace, keyspaceTables := range m.status.Tables {
			for tableName, table := range keyspaceTables {
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

	for worker := 1; worker <= m.Conf.Threads; worker++ {
		go m.schemaPool(&wgSchema, schemaJobs)
		go m.tablePool(&wgTables, tableJobs)
	}

	for _, keyspaceTables := range tables {
		for _, table := range keyspaceTables {
			wgSchema.Add(1)
			schemaJobs <- table
		}
	}
	close(schemaJobs)
	wgSchema.Wait()

	// Start listening to proxy communications
	go m.listenProxy()

	// Establish a connection to the proxy service
	m.conn = m.establishConnection()
	defer m.conn.Close()

	// Notify proxy service that schemas are finished migrating, unload/load starting
	m.sendStart()

	// Ensure the start signal has been received and processed before continuing
	for {
		startReceived := true
		for _, update := range m.outstandingUpdates {
			if update.Type == updates.Start {
				startReceived = false
				break
			}
		}

		if startReceived {
			break
		}

		time.Sleep(500 * time.Millisecond)
		log.Debug("Waiting for start signal to be received...")
	}

	for _, keyspaceTables := range tables {
		for _, table := range keyspaceTables {
			wgTables.Add(1)
			tableJobs <- table
		}
	}

	close(tableJobs)
	wgTables.Wait()
	log.Info("COMPLETED MIGRATION")

	// Calculate total file size of migrated data
	var size int64
	for keyspace, keyspaceTables := range tables {
		for tableName := range keyspaceTables {
			dSize, err := utils.DirSize(m.directory + keyspace + "." + tableName)
			if err != nil {
				log.WithError(err).Fatal(err)
			}
			size += dSize
		}
	}

	// Calculate average speed in megabytes per second
	m.status.Speed = (float64(size) / 1024 / 1024) / (float64(time.Now().UnixNano()-begin.UnixNano()) / 1000000000)

	m.writeCheckpoint()

	if m.status.Steps == m.status.TotalSteps {
		m.sendComplete()
	} else {
		log.Fatal(errors.New("Migration ended early"))
	}

	select {}
}

func (m *Migration) schemaPool(wg *sync.WaitGroup, jobs <-chan *gocql.TableMetadata) {
	for table := range jobs {
		// If we already migrated schema, skip this
		if m.status.Tables[table.Keyspace][table.Name].Step < MigratingSchemaComplete {
			err := m.migrateSchema(table.Keyspace, table)
			if err != nil {
				log.Fatal(err)
			}

			m.status.Steps++
			m.status.Tables[table.Keyspace][table.Name].Step = MigratingSchemaComplete
			log.Info(fmt.Sprintf("COMPLETED MIGRATING TABLE SCHEMA: %s.%s", table.Keyspace, table.Name))
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
	m.status.Tables[keyspace][table.Name].Step = MigratingSchema
	log.Info(fmt.Sprintf("MIGRATING TABLE SCHEMA: %s.%s... ", table.Keyspace, table.Name))

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
		m.status.Tables[keyspace][table.Name].Step = Errored
		m.status.Tables[keyspace][table.Name].Error = err
		return err
	}
	return nil
}

func (m *Migration) tablePool(wg *sync.WaitGroup, jobs <-chan *gocql.TableMetadata) {
	for table := range jobs {
		if m.status.Tables[table.Keyspace][table.Name].Step != LoadingDataComplete {
			err := m.migrateData(table)
			if err != nil {
				log.Fatal(err)
			}

			m.status.Steps += 2
			m.status.Tables[table.Keyspace][table.Name].Step = LoadingDataComplete
			log.Info(fmt.Sprintf("COMPLETED LOADING TABLE DATA: %s.%s", table.Keyspace, table.Name))
			m.writeCheckpoint()

			m.sendTableUpdate(m.status.Tables[table.Keyspace][table.Name])
		}
		wg.Done()
	}
}

// migrateData migrates a table from the source cluster to the Astra cluster
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

func (m *Migration) buildUnloadQuery(table *gocql.TableMetadata) string {
	query := "SELECT "
	for colName, column := range table.Columns {
		if column.Kind == gocql.ColumnPartitionKey {
			query += colName + ", "
			continue
		}
		query += fmt.Sprintf("%[1]s, WRITETIME(%[1]s) AS w_%[1]s, TTL(%[1]s) AS l_%[1]s, ", colName)
	}

	return query[0:len(query)-2] + fmt.Sprintf(" FROM %s.%s", table.Keyspace, table.Name)
}

// unloadTable exports a table CSV from the source cluster into DIRECTORY
func (m *Migration) unloadTable(table *gocql.TableMetadata) error {
	m.status.Tables[table.Keyspace][table.Name].Step = UnloadingData
	log.Info(fmt.Sprintf("UNLOADING TABLE: %s.%s...", table.Keyspace, table.Name))
	m.sendTableUpdate(m.status.Tables[table.Keyspace][table.Name])

	query := m.buildUnloadQuery(table)
	cmdArgs := []string{"unload", "-port", strconv.Itoa(m.Conf.SourcePort), "-query", query, "-url", m.directory + table.Keyspace + "." + table.Name, "-logDir", m.directory}
	_, err := exec.Command(m.Conf.DsbulkPath, cmdArgs...).Output()
	if err != nil {
		m.status.Tables[table.Keyspace][table.Name].Step = Errored
		m.status.Tables[table.Keyspace][table.Name].Error = err
		return err
	}

	m.status.Tables[table.Keyspace][table.Name].Step = UnloadingDataComplete
	log.Info(fmt.Sprintf("COMPLETED UNLOADING TABLE: %s.%s", table.Keyspace, table.Name))

	m.sendTableUpdate(m.status.Tables[table.Keyspace][table.Name])

	return nil
}

func (m *Migration) buildLoadQuery(table *gocql.TableMetadata) string {
	query := "BEGIN BATCH "
	partitionKey := table.PartitionKey[0].Name
	for colName := range table.Columns {
		if colName == partitionKey {
			continue
		}
		query += fmt.Sprintf("INSERT INTO %[1]s.%[2]s(%[3]s, %[4]s) VALUES (:%[3]s, :%[4]s) USING TIMESTAMP :w_%[4]s AND TTL :l_%[4]s; ", table.Keyspace, table.Name, partitionKey, colName)
	}
	query += "APPLY BATCH;"
	return query
}

// loadTable loads a table from an exported CSV (in path specified by DIRECTORY)
// into the target cluster
func (m *Migration) loadTable(table *gocql.TableMetadata) error {
	m.status.Tables[table.Keyspace][table.Name].Step = LoadingData
	log.Info(fmt.Sprintf("LOADING TABLE: %s.%s...", table.Keyspace, table.Name))
	m.sendTableUpdate(m.status.Tables[table.Keyspace][table.Name])

	query := m.buildLoadQuery(table)
	cmdArgs := []string{"load", "-h", m.Conf.AstraHostname, "-port", strconv.Itoa(m.Conf.AstraPort), "-query", query, "-url", m.directory + table.Keyspace + "." + table.Name, "-logDir", m.directory, "--batch.mode", "DISABLED"}
	_, err := exec.Command(m.Conf.DsbulkPath, cmdArgs...).Output()
	if err != nil {
		m.status.Tables[table.Keyspace][table.Name].Step = Errored
		m.status.Tables[table.Keyspace][table.Name].Error = err
		return err
	}

	return nil
}

func (m *Migration) getKeyspaces() error {
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
			m.Keyspaces = append(m.Keyspaces, keyspaceName)
		}
	}
	return nil
}

// getTables gets table information from a keyspace in the source cluster
func (m *Migration) getTables(keyspaces []string) (map[string]map[string]*gocql.TableMetadata, error) {
	tableMetadata := make(map[string]map[string]*gocql.TableMetadata)
	for _, keyspace := range keyspaces {
		md, err := m.sourceSession.KeyspaceMetadata(keyspace)
		if err != nil {
			log.WithError(err).Fatal(fmt.Sprintf("ERROR GETTING KEYSPACE %s...", keyspace))
			return nil, err
		}
		tableMetadata[keyspace] = md.Tables
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
		for tableName, table := range keyspaceTables {
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
		log.Error(err)
	}
}

// readCheckpoint fills in the Status w/ the checkpoint on file
// "s" shows the table schema has been successfully migrated
// "d" shows the table data has been successfully migrated
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
			m.status.Tables[keyspace][tableName].Step = MigratingSchemaComplete
			m.status.Steps++
		} else if entry[0] == 'd' {
			m.status.Tables[keyspace][tableName].Step = LoadingDataComplete
			m.status.Steps += 2
		}
	}
}

func (m *Migration) establishConnection() net.Conn {
	hostname := fmt.Sprintf("%s:%d", m.Conf.ProxyServiceHostname, m.Conf.ProxyCommunicationPort)
	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: false,
	}

	log.Info(fmt.Sprintf("Attempting to connect to Proxy Service at %s...", hostname))
	for {
		conn, err := net.Dial("tcp", hostname)
		if err != nil {
			nextDuration := b.Duration()
			log.Info(fmt.Sprintf("Couldn't connect to Proxy Service, retrying in %s...", nextDuration.String()))
			time.Sleep(nextDuration)
			continue
		}
		log.Info("Successfully established connection with Proxy Service")
		return conn
	}
}

// listenProxy sets up communication between migration and proxy service using web sockets
func (m *Migration) listenProxy() error {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", m.Conf.MigrationCommunicationPort))
	if err != nil {
		return err
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			default:
				log.Error(err)
				continue
			}
		}

		updates.CommunicationHandler(conn, conn, m.handleRequest)
	}
}

func (m *Migration) sendStart() {
	bytes, err := json.Marshal(m.status)
	if err != nil {
		log.WithError(err).Fatal("Error marshalling status for start signal")
	}

	m.sendRequest(updates.New(updates.Start, bytes))
}

func (m *Migration) sendComplete() {
	bytes, err := json.Marshal(m.status)
	if err != nil {
		log.WithError(err).Fatal("Error marshalling status for complete signal")
	}

	m.sendRequest(updates.New(updates.Complete, bytes))
}

func (m *Migration) sendTableUpdate(table *Table) {
	bytes, err := json.Marshal(table)
	if err != nil {
		log.WithError(err).Fatal("Error marshalling table for update")
	}

	m.sendRequest(updates.New(updates.TableUpdate, bytes))
}

// sendRequest will notify the proxy service about the migration progress
func (m *Migration) sendRequest(req *updates.Update) {
	m.updateLock.Lock()
	m.outstandingUpdates[req.ID] = req
	m.updateLock.Unlock()

	err := updates.Send(req, m.conn)
	if err != nil {
		log.WithError(err).Errorf("Error sending request %s", req.ID)
	}
}

// handleRequest handles the notifications from proxy service
func (m *Migration) handleRequest(req *updates.Update) error {
	switch req.Type {
	case updates.Shutdown:
		// 1) On Shutdown, restarts the migration process due to proxy service failure
		// TODO: figure out how to restart automatically (in progress)
		log.Fatal("Proxy Service failed, need to restart services")
	case updates.TableUpdate:
		// 2) On TableUpdate, update priority queue with the next table that needs to be migrated (in progress)
		var table Table
		err := json.Unmarshal(req.Data, &table)
		if err != nil {
			// log error
			return err
		}
		m.status.Tables[table.Keyspace][table.Name].Priority = table.Priority
		// TODO: priority queue logic here (in progress)
	case updates.Success:
		// On success, delete the stored request
		m.updateLock.Lock()
		delete(m.outstandingUpdates, req.ID)
		m.updateLock.Unlock()
	case updates.Failure:
		// If failed, resend the same update
		var toSend *updates.Update
		m.updateLock.Lock()
		toSend = m.outstandingUpdates[req.ID]
		m.updateLock.Unlock()
		m.sendRequest(toSend)
	}
	return nil
}
