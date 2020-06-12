# Migration Service

## Overview

The Migration Service creates two sessions, to a source cluster and destination cluster. We first create the tables in the destination cluster, and then use `dsbulk` to unload and load each table. We track the status of each table and communicate w/ the proxy service with our own protocol on top of TCP. The status of the migration is also stored on S3 in a .chk file that shows where migration stopped, in case any of the services fail during the process.

![](https://paper-attachments.dropbox.com/s_7247316FB853F6B8340A26E55C47BAE338C143790A25CB623AF3483C70F28191_1588622084419_Migration+Architecture+Detailed.png)

## Environment Variables
    export SOURCE_HOSTNAME            // Hostname of client's DB           (string)
    export SOURCE_USERNAME            // Username to access client's DB    (string) 
    export SOURCE_PASSWORD            // Password to access client's DB    (string) 
    export SOURCE_PORT                // Port of client's DB               (int)
    export ASTRA_HOSTNAME             // Hostname of Astra DB              (string)
    export ASTRA_USERNAME             // Username to access Astra DB       (string) 
    export ASTRA_PASSWORD             // Password to access Astra DB       (string)
    export ASTRA_PORT                 // Port of Astra DB                  (int)
    export PROXY_QUERY_PORT           // Port proxy listens on for queries (int)
    export MIGRATION_COMPLETE         // Flag of migration status          (bool)
    export MIGRATION_SERVICE_HOSTNAME // Hostname of migration service     (string)
    export MIGRATION_PORT             // Port of migration service         (int)
    export MIGRATION_METRICS_PORT     // Port of migration metrics         (int)
    export PROXY_SERVICE_HOSTNAME     // Hostname of proxy service         (string)
    export PROXY_PORT                 // Port of proxy service             (int)
    export DSBULK_PATH                // Dsbulk path                       (string)
    export THREADS                    // Size of worker pool               (int)
    export HARD_RESTART               // Restarts migration                (bool)
    export DEBUG                      // For testing                       (bool)
    export MIGRATION_ID               // For distinguishing .chks on s3    (string, UUID)

These environment variables are read and processed into a `Config` struct, which is passed into the Migration Service.

## Starting Migration

We created a bash script `migrate.sh` which builds the migration service into a binary file in the migration folder, and continues to restart the migration service until it returns exit code 0 (migration complete).
```
go build -o ./migration/bin ./migration/main.go
./migration/bin
code=$?
echo $code
while [ $code -eq 100 ]; do
  HARD_RESTART=true ./migration/bin
  code=$?
done
```
If the proxy service fails, the migration service will return exit code 100. The migration service will be run again with `HARD_RESTART=TRUE`. On restart, the service will go through the previously saved checkpoint file, drop all of the tables that had been migrated, and start over, waiting for the proxy to reconnect.  

## Migrating Schema

Our script uses GoCQL to pull schema information from the source cluster. We then build and run queries to create identical tables on the Astra cluster. If the table does not meet Astra database limits, the migration service will exit. The pseudocode is shown below:

    func createTable(table *gocql.TableMetadata) error {
      ...
      query := fmt.Sprintf("CREATE TABLE %s.%s (", keyspace, table.Name)
      // for each column, add column name and type to the query
      // for each partition key, add column name to the query
      query += ");"
      err := destSession.Query(query).Exec()
      if err != nil {
        panic(err)
      }
      return nil
    }
    
## Migrating Indexes 

After migrating the schema, any index of the table will be migrated. As per Astra database limits, if a table has more than one secondary index, the migration service will exit.

## Migrating Data

Our Go script will connect to the source cluster first to pull data and schema information. After creating a predetermined number of threads, we will use the thread pool to first migrate all of the table schema. After all schema has been migrated, the thread pool will be used for table migration, using dsbulk to migrate the tables to the destination cluster concurrently. We are using sync.Mutex locks to ensure that the threads do not overwrite one another in the Checkpoint file. Migration pseudocode is shown below:

    for each table to be migrated {
      spawn a worker thread to migrate the table schema
    }
    // wait for all threads to complete migrating schema
    notifyProxy()
    for each table to be migrated {
      spawn a worker thread to migrate the table data
      notifyProxy() //if proxy is waiting on a table, here we will notify it
    }
    // wait for all threads to complete migrating data
    notifyProxy() // notify that migration has been completed

Unloading data from the source database (store dsbulk CSVs in S3):

    unloadCmdArgs := []string{"unload", "-k", table.Keyspace, "-port", strconv.Itoa(m.Conf.SourcePort), "-query", query, "-logDir", m.directory}
    awsStreamArgs := []string{"s3", "cp", "-", "s3://codebase-datastax-test/" + m.directory + table.Keyspace + "." + table.Name}

Loading data into destination database (get dsbulk CSVs from S3):

    loadCmdArgs := []string{
            "load", "-k", table.Keyspace, "-h", m.Conf.AstraHostname, "-port", strconv.Itoa(m.Conf.AstraPort), "-query", query,
            "-logDir", m.directory, "--batch.mode", "DISABLED"}
    awsStreamArgs := []string{"s3", "cp", "s3://codebase-datastax-test/" + m.directory + table.Keyspace + "." + table.Name, "-"}

The query parameter passed into the dsbulk unload command allows us to preserve the writetime and TTL of each column. Then when we are loading the data into Astra, we will load using batch statements to ensure the last write persists. An example can be found here: [https://www.datastax.com/blog/2019/12/datastax-bulk-loader-examples-loading-other-locations](https://www.datastax.com/blog/2019/12/datastax-bulk-loader-examples-loading-other-locations)
Building the Unload query:

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

Building the Load query:

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

In each of the queries above, we are also accounting for case-sensitive keyspace/table/column names by putting names in quotes.

## Checkpoints

We save checkpoints by writing to a `migration.chk`. We overwrite the checkpoint when a table’s schema is done migrating, or when dsbulk finishes migrating a table’s data. The checkpoint file contains data on which tables are done migrating schema and/or data. The "s" prefix means the table schema has been successfully migrated, and the "d" prefix means the table data has been successfully unloaded and loaded. The below checkpoint file shows that the schemas for `k1.tasks` and `k2.todos` are done migrating, and that dsbulk has finished migrating data for `k1.tasks`. As mentioned below, this file will be saved to a S3 bucket for volume purposes.

    s:k1.tasks
    s:k2.todos
    
    d:k1.tasks
    
## Logging

We log information to standard output. We also create a unique directory `migration-<timestamp>` to hold dsbulk logs, exports, etc.

    [2020-04-15 17:13:15.00777 -0700 PDT m=+0.047558280] == BEGIN MIGRATION ==
    [2020-04-15 17:13:15.06961 -0700 PDT m=+0.109398739] MIGRATING TABLE SCHEMA: test.todos... 
    [2020-04-15 17:13:15.069609 -0700 PDT m=+0.109397738] MIGRATING TABLE SCHEMA: test.tasks... 
    [2020-04-15 17:13:15.069616 -0700 PDT m=+0.109404437] MIGRATING TABLE SCHEMA: test2.tasks... 
    [2020-04-15 17:13:15.187058 -0700 PDT m=+0.226846182] COMPLETED MIGRATING TABLE SCHEMA: test.tasks
    [2020-04-15 17:13:15.292145 -0700 PDT m=+0.331933799] COMPLETED MIGRATING TABLE SCHEMA: test.todos
    [2020-04-15 17:13:15.385301 -0700 PDT m=+0.425090006] COMPLETED MIGRATING TABLE SCHEMA: test2.tasks
    [2020-04-15 17:13:15.385826 -0700 PDT m=+0.425614703] UNLOADING TABLE: test.tasks...
    [2020-04-15 17:13:15.385838 -0700 PDT m=+0.425626130] UNLOADING TABLE: test.todos...
    [2020-04-15 17:13:15.385945 -0700 PDT m=+0.425733627] UNLOADING TABLE: test2.tasks...
    [2020-04-15 17:13:22.880206 -0700 PDT m=+7.919999108] COMPLETED UNLOADING TABLE: test2.tasks
    [2020-04-15 17:13:22.880246 -0700 PDT m=+7.920038523] LOADING TABLE: test2.tasks...
    [2020-04-15 17:13:22.88756 -0700 PDT m=+7.927352896] COMPLETED UNLOADING TABLE: test.tasks
    [2020-04-15 17:13:22.887593 -0700 PDT m=+7.927385563] LOADING TABLE: test.tasks...
    [2020-04-15 17:13:22.901757 -0700 PDT m=+7.941549677] COMPLETED UNLOADING TABLE: test.todos
    [2020-04-15 17:13:22.901787 -0700 PDT m=+7.941579934] LOADING TABLE: test.todos...
    [2020-04-15 17:13:29.563704 -0700 PDT m=+14.603500866] COMPLETED LOADING TABLE DATA: test.todos
    [2020-04-15 17:13:29.576327 -0700 PDT m=+14.616123729] COMPLETED LOADING TABLE DATA: test.tasks
    [2020-04-15 17:13:29.596237 -0700 PDT m=+14.636033438] COMPLETED LOADING TABLE DATA: test2.tasks
    [2020-04-15 17:13:29.596774 -0700 PDT m=+14.636570478] COMPLETED MIGRATION

## S3

With very large tables, storing the dsbulk CSVs in local disk on the k8 nodes is not scalable. Therefore we will be saving the CSVs as well as the migration checkpoint file to an S3 bucket (by piping the output of dsbulk to S3) to handle the volume.

## Communication with Proxy Service

We use Golang’s `net` package to communicate w/ the proxy service using our own protocol over TCP. Every “update” is of the format:

    // UpdateType is an enum of types of update between the proxy and migration services
    type UpdateType int
    // TableUpdate, Start, Complete, ... are the enums of the update types
    const (
        Start = iota
        Complete
        Success
        Failure
    )
    // Update represents a request between the migration and proxy services
    type Update struct {
        ID    string
        Type  UpdateType
        Data  []byte
        Error string
    }

The migration service sends an update to the proxy service when:

1. We begin migrating data (schemas are migrated)
2. When we complete the entire migration
3. We have successfully received and handled a proxy service update
4. We have failed handling a proxy service update

The Migration Service expects requests from the Proxy Service when:

1. A single table's migration needs to be restarted
2. Proxy has successfully received and handled a migration update
3. Proxy has failed handling a migration update

## Metrics

On port MIGRATION_METRICS_PORT (8081), the client can see characteristics of the migration service, including number of tables left to be migrated, speed of migration, etc. 
```
type Metrics struct {
    TablesMigrated int
    TablesLeft     int
    Speed          float64
    SizeMigrated   float64
}
```

## Limitations and Assumptions

- Client and Astra DB permissions will be provided to the service
- S3 credentials will be provided to the service
- Keyspace(s) will already exist in both databases
- Dsbulk will not fail, if dsbulk returns an error, the migration process will halt.
- Client will not be able to alter the schema of their keyspace or tables. Since our service only notifies proxy that the migration service is starting after migrating all of the schema, a dsbulk load into a table whose keyspace has been changed will fail.
- When a table is migrated to Astra, the TTL remains unchanged from when it was unloaded from the client DB.
