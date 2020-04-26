# Migration Service

## Overview

The Migration Service creates two sessions, to a source cluster and destination cluster. We first create the tables in the destination cluster, and then use `dsbulk` to unload and load each table. 
We track the status of each table and communicate w/ the proxy service with our own protocol on top of TCP. The status of the migration is also persisted onto disk in a .chk file that shows where migration stopped, in case any of the services fail during the process.

## Script Flags
    go run main.go
      [-source_hostname Source cluster host (127.0.0.1)]
      [-source_username Source cluster username]
      [-source_password Source cluster password]
      [-source_port Source cluster port (9042)]
      [-astra_hostname Destination cluster host (127.0.0.1)]
      [-astra_username Destinationcluster username]
      [-astra_password Destination cluster password]
      [-astra_port Destination cluster port (9042)]
      [-d Dsbulk path]
      [-r Boolean, ignores checkpoints if present (false)]
      [-t Number of threads to use (1)]
      [-pp Proxy service port, used to communicate w/ proxy service]
## Migrating Schema

Our script uses GoCQL to pull schema information from the source cluster. We then build and run queries to create identical tables on the Astra cluster. The pseudocode is shown below:

    func createTable(table *gocql.TableMetadata) error {
      ...
      query := fmt.Sprintf("CREATE TABLE %s.%s (", keyspace, table.Name)
      // for each column, add column name and type to the query
      // for each partition key, add column name to the query
      // add the compaction factor of the table to the query
      query += ");"
      err := destSession.Query(query).Exec()
      if err != nil {
        panic(err)
      }
      return nil
    }
## Migrating Data

Our Go script will connect to the source cluster first to pull data and schema information. After creating a predetermined number of threads, we will use the thread pool to first migrate all of the table schema. After all schema has been migrated, the thread pool will be used for table migration, using dsbulk to migrate the tables to the destination cluster concurrently. We are using sync.Mutex locks to ensure that the threads do not overwrite one another in the Checkpoint file nor the Log file.
Migration pseudocode is shown below:

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

Unloading data from the source database:

    cmdArgs := []string{"unload", "-port", strconv.Itoa(sourcePort), "-query", query, -k", table.Keyspace, "-t", table.Name, "-url", directory + table.Name}

Loading data into destination database:

    cmdArgs := []string{"load", "-h", destinationHost, "-port", strconv.Itoa(destinationPort), "-query", query, "-k", table.Keyspace, "-t", table.Name, "-url", directory + table.Name}

The query parameter passed into the dsbulk unload command allows us to preserve the writetime and TTL of each column. Then when we are loading the data into Astra, we will load using batch statements to ensure the last write persists. An example can be found here: https://www.datastax.com/blog/2019/12/datastax-bulk-loader-examples-loading-other-locations 

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


## Priority Queue

(In progress) To handle edge cases such as updating table data as it is being migrated, we implement a priority queue which will order the tables based on table.Priority. The proxy service will hold update queries until the migration service notifies it that the table to be queried has been successfully migrated.

## Communicating w/ Proxy Service

We use Golang’s `net` package to communicate w/ the proxy service using our own protocol over TCP. Every “update” is of the format:


    // UpdateType is an enum of types of update between the proxy and migration services
    type UpdateType int
    // TableUpdate, Start, Complete, ... are the enums of the update types
    const (
        TableUpdate = iota
        Start
        Complete
        Shutdown
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
2. When we begin unloading data for a table
3. When we finish migrating data for a table
4. When we complete the entire migration
5. The migration service errors and shuts down

The Migration Service expects requests from the Proxy Service when:

1. There is an update to the migration priority for a table
2. The proxy service errors and shuts down
## Checkpoints

We save checkpoints by writing to a `migration.chk` file. We overwrite the checkpoint when a table’s schema is done migrating, or when dsbulk finishes migrating a table’s data. The checkpoint file contains data on which tables are done migrating schema and/or data. The "s" prefix means the table schema has been successfully migrated, and the "d" prefix means the table data has been successfully unloaded and loaded. The below checkpoint file shows that the schemas for `k1.tasks` and `k2.todos` are done migrating, and that dsbulk has finished migrating data for `k1.tasks`. As mentioned below, this file will be saved to a S3 bucket for volume purposes.


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

(In progress) With very large tables, storing the dsbulk CSVs in local disk on the k8 nodes is not scalable. Therefore we will be saving the CSVs as well as the migration checkpoint file to an S3 bucket (by passing the S3 bucket’s URL into dsbulk) to handle the volume.

## Testing

(In progress) 

## Constraints/Assumptions
- dsbulk will not fail
- Client will not be able to alter the schema of their keyspace (or tables?)
- No weird stuff
## TODOs
- Integrate S3 buckets to store exports, logs, checkpoint files, etc.
- Handle updates from the proxy service
- Priority queue for ordering table migrations
- Write tests

