## Codebase Migration Service 

This data migration service allows for clients to continue accessing their database throughout the process of migrating data from their original database (client DB) to their new Astra instance (Astra DB). Our service allows for almost all queries to the database including the major reads and writes. Any major exceptions are listed at the bottom of this document. Although the two services work independently, they communicate to ensure consistency through an `Update` struct.

![](https://lh5.googleusercontent.com/ztKN7gzbeskAYy8Km_JyPrwoLOyRRr8yXJw6C9u1JieyL7uNdZc-2_N2clynzpXCO9_NcBNKb_lJyyOOivH13fXIgcXzkbectJNrvrqVlRrHCV_ICL2Yep2qAq7SkrL_aHr-nPR7)

## Components

The migration service is split into two independent services:

- The proxy service- all queries that the client makes to the Astra DB are proxied to the client DB as well. For reads, the response from the client DB is returned. 
- The migration service- using DSBulk, the client’s data will be loaded into CSV files using S3 buckets and then loaded into the Astra DB table by table.

## Communication

Because the two services are for the most part independent, they use an `Update` struct to communicate. The migration services sends signals to the proxy service when the migration of the database starts and completes starts and completes. The proxy service sends signals to the migration service when there are changes to the order of table migration or if any table needs to be re-migrated. For more information on how the communication is handled on either side of the service, visit the ‘Communication’ sections of the service’s specific READMEs.

## Build and Test

Our testing creates two docker containers to simulate the client DB and Astra DB, and runs the services on these two containers.

Spinning up docker containers:
We assume that the client DB (cassandra-source) lives at port 9042 and Astra DB (cassandra-dest) lives at port 9043.


    docker run --name cassandra-source -p 9042:9042 -d cassandra
    docker run --name cassandra-dest -p 9043:9042 -d cassandra

Create test data in source cluster using a small database:

1. CQLSH into the source cluster `$ cqlsh localhost 9042`
2. Create keyspace `test` `cqlsh> CREATE KEYSPACE test WITH REPLICATION = { 'replication_factor': 1, 'class': 'SimpleStrategy' };`
3. Use keyspace `cqlsh> USE test;`
4. Create table `cqlsh> CREATE TABLE tasks(id UUID, task text, PRIMARY KEY(id));`
5. Create dummy data `cqlsh> INSERT INTO tasks(id, task) VALUES (now(), 'dsmafksadkf');`
6. Verify dummy data exists in source cluster `cqlsh> SELECT * FROM tasks;`

Run our services:

1. Specify env variables documented in the proxy README. The following are the default values we have used
    // default set of env vars
    
    export SOURCE_HOSTNAME="127.0.0.1"
    export SOURCE_USERNAME=""
    export SOURCE_PASSWORD=""
    export SOURCE_PORT=9042
    export ASTRA_HOSTNAME="127.0.0.1"
    export ASTRA_USERNAME=""
    export ASTRA_PASSWORD=""
    export ASTRA_PORT=9043
    export MIGRATION_SERVICE_HOSTNAME="127.0.0.1"
    export MIGRATION_COMMUNICATION_PORT=15000
    export PROXY_SERVICE_HOSTNAME="127.0.0.1"
    export PROXY_COMMUNICATION_PORT=14000
    export MIGRATION_COMPLETE=false
    export DEBUG=true
    export TEST=true
    export PROXY_METRICS_PORT=14001
    export PROXY_QUERY_PORT=14002
    export DSBULK_PATH=/path to dsbulk/
1. Download any necessary packages to run our services
2. Run `go run proxy/main.go` and `go run migration/main.go` at relatively similar times
3. Once they establish a connection, the migration will begin.
4. To query the database, `cqlsh localhost 14002`  and run CQL queries at this port

Between manual tests, to “refresh” make sure to…

1. Delete migration.chk: `rm -r migration.chk`
2. Delete the .csv file from DsBulk
3. Drop the table from the destination cluster (localhost 9043): `DROP TABLE test.tasks;`

## Testing Framework

To test larger and more complex edge cases automatically, we have created a basic testing framework that runs individual select statements instead of DsBulk. This framework starts the proxy service as a child process and mocks all portions of the migration service except the actual migration portions. The source and dest clusters are seeded with hardcoded, known data. During the “migration”, the user can simulate the effects of DsBulk by running individual queries. At any time, the user can assert correct behavior using queries.

The `integration-tests/main.go` contains logic to correctly seed data, start the proxy service, and invoke one or more testing scripts (e.g. `integration-tests/test1/test1.go`). The testing script is used to make individual queries make requests to the proxy service, and make assertions.

To run our testing framework:

1. Ensure two Cassandra clusters are running on ports 9042 and 9043 (preferably using Docker as specified above)
2. Ensure that the environment variables are defined(preferably using the variables specified above)
3. Note that the test will be performed in the `cloudgate_test` keyspace
4. Include test case logic within a separate go file (see `test1.go` for examples)
5. Run the testing framework using: `go run integration-tests/main.go`

## Limitations and Assumptions
- Currently we are piping our output files from DSBulk into a personal S3 bucket, this process will need to be adjusted as the service is integrated into Datastax
- We have been able to manually verify correct behaviors and ensure that the client and Astra DBs are consistent, but we have not programmatically reproduced a way to check for consistency
- We have gotten started on writing some basic config files for integration, but this process also needs to be adjust as the service is integrated into Datastax
- Schema should not be updated during the migration, this will result in the tables not being properly processed by DSBulk

For more information about constraints specific to either service (including specific queries that cannot be supported by our proxy) please visit the limitations and assumptions section of the individual READMEs for both services.