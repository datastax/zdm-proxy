package migration

import (
	"bytes"
	"cloud-gate/updates"
	"cloud-gate/utils"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	pipe "github.com/b4b4r07/go-pipe"

	"github.com/gocql/gocql"
	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"
)

// Migration contains necessary setup information
type Migration struct {
	Conf    *Config
	Metrics *Metrics

	// Sessions
	sourceSession *gocql.Session
	destSession   *gocql.Session

	// States
	initialized        bool
	status             *Status
	outstandingUpdates map[string]*updates.Update

	// Data
	keyspaces []string
	conn      net.Conn
	comms     Comms
	directory string

	// Locks
	chkLock    *sync.Mutex
	updateLock *sync.Mutex
}

// Init creates the connections to the databases, locks needed for checkpoint and logging, and initializes various data and states
func (m *Migration) Init() error {
	m.status = newStatus()

	// m.directory is used to save logs to the local machine and data and checkpoints to s3
	// It is comprised of migration ID and the current timestamp so each migration instance has a unique directory
	m.directory = fmt.Sprintf("%s/migration-%s", m.Conf.MigrationID, strconv.FormatInt(time.Now().Unix(), 10))
	os.Mkdir(m.directory, 0755)

	// Connect to source and destination sessions w/ gocql
	var err error
	m.sourceSession, err = utils.ConnectToCluster(m.Conf.SourceHostname, m.Conf.SourceUsername, m.Conf.SourcePassword, m.Conf.SourcePort)
	if err != nil {
		return err
	}

	m.destSession, err = utils.ConnectToCluster(m.Conf.AstraHostname, m.Conf.AstraUsername, m.Conf.AstraPassword, m.Conf.AstraPort)
	if err != nil {
		return err
	}

	// Initialize locks, PQs, and misc. data and states
	m.chkLock = new(sync.Mutex)
	m.updateLock = new(sync.Mutex)
	m.outstandingUpdates = make(map[string]*updates.Update)
	m.initialized = true
	m.keyspaces = make([]string, 0)
	m.comms = Comms{m: m}

	return nil
}

// Migrate executes the migration
func (m *Migration) Migrate() {
	if !m.initialized {
		log.Fatal("Migration must be initialized before migration can begin")
	}

	log.Info("== BEGIN MIGRATION ==")

	defer m.sourceSession.Close()
	defer m.destSession.Close()

	var err error

	// Discover schemas on source database
	err = m.getKeyspaces()
	if err != nil {
		log.WithError(err).Fatal("Failed to fetch keyspaces from source cluster")
	}

	tables, err := m.getTables(m.keyspaces)
	if err != nil {
		log.WithError(err).Fatal("Failed to discover table schemas from source cluster")
	}

	// Initialize Status and Metrics with discovered schemas
	m.status.initTableData(tables)
	m.Metrics = NewMetrics(m.Conf.MigrationMetricsPort, m.directory, len(m.status.Tables), m.Conf.MigrationS3)
	m.Metrics.Expose()

	m.readCheckpoint()
	if m.Conf.HardRestart {
		log.Info("== HARD RESTARTING ==")
		// On hard restart, iterate through all tables in source and drop them from astra
		for keyspace, keyspaceTables := range m.status.Tables {
			for tableName, table := range keyspaceTables {
				if table.Step >= MigratingSchemaComplete {
					query := fmt.Sprintf("DROP TABLE %s.%s;", strconv.Quote(keyspace), strconv.Quote(tableName))
					err := m.destSession.Query(query).Exec()

					if err != nil {
						log.WithError(err).Fatalf("Failed to drop table %s.%s for hard restart",
							strconv.Quote(keyspace), strconv.Quote(tableName))
					}
				}

				table.Step = Waiting
			}
		}

		// Remove the existing checkpoint file
		err := exec.Command("aws", "s3", "rm", fmt.Sprintf("s3://%s/%s/migration.chk", m.Conf.MigrationS3, m.Conf.MigrationID)).Run()
		if err != nil {
			log.WithError(err).Fatal("Error removing migration checkpoint file on hard restart")
		}

		// Re-read the (nonexistent) checkpoint to start with clean m.status object
		m.readCheckpoint()
	}

	// Initialize workgroups for migrating schemas and tables
	schemaJobs := make(chan *gocql.TableMetadata, len(tables))
	tableJobs := make(chan *gocql.TableMetadata, len(tables))
	var wgSchema sync.WaitGroup
	var wgTables sync.WaitGroup

	// Add workers to workgroups
	for worker := 1; worker <= m.Conf.Threads; worker++ {
		go m.schemaPool(&wgSchema, schemaJobs)
		go m.tablePool(&wgTables, tableJobs)
	}

	// Migrate schemas using workgroups
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
	m.comms.sendStart()

	// Wait for the start signal to be received and processed before continuing
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

	// Migrate tables using workgroups
	for _, keyspaceTables := range tables {
		for _, table := range keyspaceTables {
			wgTables.Add(1)
			tableJobs <- table
		}
	}
	close(tableJobs)
	wgTables.Wait()

	// Migration complete; write checkpoint and notify proxy
	log.Info("== COMPLETED MIGRATION ==")
	m.writeCheckpoint()
	m.comms.sendComplete()
	os.Exit(0)
}

// schemaPool adds a "worker" that ingests and performs schema migration jobs
func (m *Migration) schemaPool(wg *sync.WaitGroup, jobs <-chan *gocql.TableMetadata) {
	for table := range jobs {
		// If we already migrated schema, skip this
		if m.status.Tables[table.Keyspace][table.Name].Step < MigratingSchemaComplete {
			// Migrate schemas
			err := m.migrateSchema(table.Keyspace, table)
			if err != nil {
				log.WithError(err).Fatalf("Error migrating schema for table %s.%s", table.Keyspace, table.Name)
			}

			// Migrate indexes
			itr := m.sourceSession.Query("SELECT index_name, options FROM system_schema.indexes WHERE keyspace_name = ? AND table_name = ?;", table.Keyspace, table.Name).Iter()
			if itr.NumRows() > 1 {
				log.Fatalf("Astra tables can only have one secondary index; %s.%s has multiple", table.Keyspace, table.Name)
			} else if itr.NumRows() == 1 {
				var indexName string
				var options map[string]string
				itr.Scan(&indexName, &options)

				query := fmt.Sprintf("CREATE INDEX %s ON %s.%s (%s); ", strconv.Quote(indexName), strconv.Quote(table.Keyspace),
					strconv.Quote(table.Name), strconv.Quote(options["target"]))

				err = m.destSession.Query(query).Exec()

				if err != nil {
					log.WithError(err).Fatal("Failed to migrate index")
				}
			}

			// Update status
			m.status.Tables[table.Keyspace][table.Name].Step = MigratingSchemaComplete
			log.Infof("COMPLETED MIGRATING TABLE SCHEMA: %s.%s", table.Keyspace, table.Name)
			m.writeCheckpoint()
		}
		wg.Done()
	}
}

// buildSchemaMigrationQuery generates a CQL query that recreates a table schema
func buildSchemaMigrationQuery(table *gocql.TableMetadata) string {
	query := fmt.Sprintf("CREATE TABLE %s.%s (", strconv.Quote(table.Keyspace), strconv.Quote(table.Name))
	for cname, column := range table.Columns {
		query += fmt.Sprintf("%s %s, ", strconv.Quote(cname), column.Type.Type().String())
	}

	query += fmt.Sprintf("PRIMARY KEY (")

	for _, column := range table.PartitionKey {
		query += fmt.Sprintf("%s, ", strconv.Quote(column.Name))
	}

	clustering := false
	clusterDesc := ""
	if len(table.ClusteringColumns) > 0 {
		clustering = true
		for _, column := range table.ClusteringColumns {
			clusterKey := strconv.Quote(column.Name)
			query += fmt.Sprintf("%s, ", clusterKey)
			clusterDesc += fmt.Sprintf("%s %s, ", clusterKey, column.ClusteringOrder)
		}
	}

	query = query[0:(len(query) - 2)]
	query += ")) "

	if clustering {
		clusterDesc = clusterDesc[0:(len(clusterDesc) - 2)]
		query += fmt.Sprintf("WITH CLUSTERING ORDER BY (%s);", clusterDesc)
	}

	return query
}

// migrateSchema creates each new table in Astra with
// column names and types, primary keys, and clustering information
func (m *Migration) migrateSchema(keyspace string, table *gocql.TableMetadata) error {
	m.status.Tables[keyspace][table.Name].Step = MigratingSchema
	log.Infof("MIGRATING TABLE SCHEMA: %s.%s... ", table.Keyspace, table.Name)

	if len(table.Columns) > 50 {
		log.Fatalf("Astra tables can have 50 columns max; table %s.%s has %d columns", keyspace, table.Name, len(table.Columns))
	}

	query := buildSchemaMigrationQuery(table)
	err := m.destSession.Query(query).Exec()

	if err != nil {
		m.status.Tables[keyspace][table.Name].Step = Errored
		m.status.Tables[keyspace][table.Name].Error = err
		return err
	}

	return nil
}

// tablePool adds a "worker" that ingests and performs table data migration jobs
func (m *Migration) tablePool(wg *sync.WaitGroup, jobs <-chan *gocql.TableMetadata) {
	for table := range jobs {
		if m.status.Tables[table.Keyspace][table.Name].Step != LoadingDataComplete {
			err := m.migrateData(table)
			if err != nil {
				log.WithError(err).Fatalf("Failed to migrate table data for %s.%s", table.Keyspace, table.Name)
			}
			m.Metrics.IncrementTablesMigrated()
			m.Metrics.DecrementTablesLeft()

			currTable := m.status.Tables[table.Keyspace][table.Name]
			currTable.Lock.Lock()
			if currTable.Redo {
				m.Metrics.DecrementTablesMigrated()
				m.Metrics.IncrementTablesLeft()
				log.Infof("REMIGRATING TABLE: %s.%s... ", table.Keyspace, table.Name)
				keyspace := strconv.Quote(table.Keyspace)
				name := strconv.Quote(table.Name)
				truncateQuery := fmt.Sprintf("TRUNCATE %s.%s;", keyspace, name)
				err := m.destSession.Query(truncateQuery, keyspace, name).Exec()
				if err != nil {
					log.WithError(err).Fatalf("Failed to drop table %s.%s for remigration", keyspace, name)
				}

				err = m.migrateData(table)
				if err != nil {
					log.WithError(err).Fatalf("Failed to remigrate data for table %s.%s", keyspace, name)
				}
				m.Metrics.IncrementTablesMigrated()
				m.Metrics.DecrementTablesLeft()
				currTable.Redo = false
			}
			currTable.Lock.Unlock()

			m.status.Tables[table.Keyspace][table.Name].Step = LoadingDataComplete
			log.Infof("COMPLETED LOADING TABLE DATA: %s.%s", table.Keyspace, table.Name)
			m.writeCheckpoint()
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

	m.status.Tables[table.Keyspace][table.Name].Step = UnloadingDataComplete
	log.Infof("COMPLETED UNLOADING TABLE: %s.%s", table.Keyspace, table.Name)

	err = m.loadTable(table)
	if err != nil {
		return err
	}

	return nil
}

// buildUnloadQuery builds a CQL query used by dsbulk to unload data into CSVs with timestamps
func (m *Migration) buildUnloadQuery(table *gocql.TableMetadata) string {
	query := "SELECT "
	for colName, column := range table.Columns {
		if column.Kind == gocql.ColumnPartitionKey || column.Kind == gocql.ColumnClusteringKey {
			query += "\\\"" + colName + "\\\", "
			continue
		}
		query += fmt.Sprintf("\\\"%[1]s\\\", WRITETIME(\\\"%[1]s\\\") AS \\\"%[2]s\\\", TTL(\\\"%[1]s\\\") AS \\\"%[3]s\\\", ",
			colName, "w_"+colName, "l_"+colName)
	}
	return query[0:len(query)-2] + fmt.Sprintf(" FROM \\\"%s\\\"", table.Name)
}

// unloadTable exports a table CSV from the source cluster into DIRECTORY
func (m *Migration) unloadTable(table *gocql.TableMetadata) error {
	m.status.Tables[table.Keyspace][table.Name].Step = UnloadingData
	log.Infof("UNLOADING TABLE: %s.%s...", table.Keyspace, table.Name)

	query := m.buildUnloadQuery(table)
	unloadCmdArgs := []string{"unload", "-k", table.Keyspace, "-port", strconv.Itoa(m.Conf.SourcePort), "-query", query, "-logDir", m.directory}
	awsStreamArgs := []string{"s3", "cp", "-", fmt.Sprintf("s3://%s/%s/%s.%s", m.Conf.MigrationS3, m.directory, table.Keyspace, table.Name)}

	// Start both dsbulk unload, pipe to aws s3 cp
	var b bytes.Buffer
	err := pipe.Command(&b,
		exec.Command(m.Conf.DsbulkPath, unloadCmdArgs...),
		exec.Command("aws", awsStreamArgs...),
	)
	io.Copy(os.Stdout, &b)

	if err != nil {
		m.status.Tables[table.Keyspace][table.Name].Step = Errored
		m.status.Tables[table.Keyspace][table.Name].Error = err
		return err
	}

	m.status.Tables[table.Keyspace][table.Name].Step = UnloadingDataComplete
	log.Infof("COMPLETED UNLOADING TABLE: %s.%s", table.Keyspace, table.Name)

	return nil
}

// buildLoadQuery builds a CQL query used by dsbulk to load data from CSVs with timestamps
func (m *Migration) buildLoadQuery(table *gocql.TableMetadata) string {
	query := "BEGIN BATCH "
	partitionKeys := ""
	partitionKeysColons := ""
	for _, column := range table.PartitionKey {
		partitionKeys += fmt.Sprintf("\\\"%s\\\", ", column.Name)
		partitionKeysColons += fmt.Sprintf(":\\\"%s\\\", ", column.Name)
	}
	if len(table.ClusteringColumns) > 0 {
		for _, column := range table.ClusteringColumns {
			partitionKeys += fmt.Sprintf("\\\"%s\\\", ", column.Name)
			partitionKeysColons += fmt.Sprintf(":\\\"%s\\\", ", column.Name)
		}
	}
	for colName, column := range table.Columns {
		if column.Kind == gocql.ColumnPartitionKey || column.Kind == gocql.ColumnClusteringKey {
			continue
		}
		query += fmt.Sprintf("INSERT INTO \\\"%[1]s\\\"(%[2]s\\\"%[3]s\\\") VALUES (%[6]s:\\\"%[3]s\\\") USING TIMESTAMP :\\\"%[4]s\\\" AND TTL :\\\"%[5]s\\\"; ",
			table.Name, partitionKeys, colName, "w_"+colName, "l_"+colName, partitionKeysColons)
	}
	query += "APPLY BATCH;"
	return query
}

// loadTable loads a table from an exported CSV (in path specified by DIRECTORY)
// into the target cluster
func (m *Migration) loadTable(table *gocql.TableMetadata) error {
	m.status.Tables[table.Keyspace][table.Name].Step = LoadingData
	log.Infof("LOADING TABLE: %s.%s...", table.Keyspace, table.Name)

	query := m.buildLoadQuery(table)
	loadCmdArgs := []string{
		"load", "-k", table.Keyspace, "-h", m.Conf.AstraHostname, "-port", strconv.Itoa(m.Conf.AstraPort), "-query", query,
		"-logDir", m.directory, "--batch.mode", "DISABLED"}
	awsStreamArgs := []string{"s3", "cp", fmt.Sprintf("s3://%s/%s/%s.%s", m.Conf.MigrationS3, m.directory, table.Keyspace, table.Name), "-"}

	// Start aws s3 cp, pipe to dsbulk load
	var b bytes.Buffer
	err := pipe.Command(&b,
		exec.Command("aws", awsStreamArgs...),
		exec.Command(m.Conf.DsbulkPath, loadCmdArgs...),
	)
	io.Copy(os.Stdout, &b)

	if err != nil {
		m.status.Tables[table.Keyspace][table.Name].Step = Errored
		m.status.Tables[table.Keyspace][table.Name].Error = err
		return err
	}

	return nil
}

// getKeyspaces populates m.keyspaces with the non-system keyspaces in the source cluster
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
			m.keyspaces = append(m.keyspaces, keyspaceName)
		}
	}
	return nil
}

// getTables gets table information from a keyspace in the source cluster
func (m *Migration) getTables(keyspaces []string) (map[string]map[string]*gocql.TableMetadata, error) {
	tableMetadata := make(map[string]map[string]*gocql.TableMetadata)
	for _, keyspace := range keyspaces {
		md, err := m.sourceSession.KeyspaceMetadata(keyspace)

		if len(md.Tables) > 200 {
			log.Fatalf("Astra keyspaces can have 200 tables max; keyspace %s has %d tables", keyspace, len(md.Tables))
		}

		if err != nil {
			log.WithError(err).Fatalf("Failed to discover tables from keyspace %s", keyspace)
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

	// Write checkpoint file to s3
	printCommandArgs := []string{string(content)}
	awsStreamArgs := []string{"s3", "cp", "-", fmt.Sprintf("s3://%s/%s/migration.chk", m.Conf.MigrationS3, m.Conf.MigrationID)}
	var b bytes.Buffer
	err := pipe.Command(&b,
		exec.Command("printf", printCommandArgs...),
		exec.Command("aws", awsStreamArgs...),
	)
	io.Copy(os.Stdout, &b)

	if err != nil {
		log.WithError(err).Error("Error writing migration checkpoint file")
	}
}

// readCheckpoint fills in the Status w/ the checkpoint on file
// "s" shows the table schema has been successfully migrated
// "d" shows the table data has been successfully migrated
func (m *Migration) readCheckpoint() {
	m.chkLock.Lock()
	defer m.chkLock.Unlock()
	data, err := exec.Command("aws", "s3", "cp", fmt.Sprintf("s3://%s/%s/migration.chk", m.Conf.MigrationS3, m.Conf.MigrationID), "-").CombinedOutput()

	m.status.Timestamp = time.Now()

	if err != nil {
		log.Debug("No migration checkpoint found, starting fresh migration")
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
		} else if entry[0] == 'd' {
			m.status.Tables[keyspace][tableName].Step = LoadingDataComplete
		}
	}
}

// establishConnect returns a net.Conn that enables us to send data to the proxy service
func (m *Migration) establishConnection() net.Conn {
	hostname := fmt.Sprintf("%s:%d", m.Conf.ProxyServiceHostname, m.Conf.ProxyCommunicationPort)
	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    10 * time.Second,
		Factor: 2,
		Jitter: false,
	}

	log.Infof("Attempting to connect to Proxy Service at %s...", hostname)
	for {
		conn, err := net.Dial("tcp", hostname)
		if err != nil {
			nextDuration := b.Duration()
			log.Debugf("Couldn't connect to Proxy Service, retrying in %s...", nextDuration.String())
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
				log.WithError(err).Errorf("Error receiving bytes on tcp:%d", m.Conf.MigrationCommunicationPort)
				continue
			}
		}

		updates.CommunicationHandler(conn, conn, m.handleRequest)
	}
}

// handleRequest handles the notifications from proxy service
func (m *Migration) handleRequest(req *updates.Update) error {
	switch req.Type {
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
		m.comms.sendRequest(toSend)
	case updates.TableRestart:
		// Restart single table migration
		var toMigrate Table
		err := json.Unmarshal(req.Data, &toMigrate)
		if err != nil {
			log.WithError(err).Error("Error unmarshalling received update")
			return err
		}
		table := m.status.Tables[toMigrate.Keyspace][toMigrate.Name]
		if table.Step >= UnloadingData && table.Step <= LoadingData {
			table.Lock.Lock()
			table.Redo = true
			table.Lock.Unlock()
			m.Metrics.IncrementTablesLeft()
			m.Metrics.DecrementTablesMigrated()
		}
	}
	return nil
}
