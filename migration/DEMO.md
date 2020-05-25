# Pre: remove all Cassandra Docker containers and volumes

# Spin up clusters in Docker
```
docker run --name cassandra-source -p 9042:9042 -d cassandra
docker run --name cassandra-dest -p 9043:9042 -d cassandra
```

# Create test data in source cluster
1. CQLSH into the source cluster
  `$ cqlsh localhost 9042`
2. Create keyspace `test`
  `cqlsh> CREATE KEYSPACE test WITH REPLICATION = { 'replication_factor': 1, 'class': 'SimpleStrategy' };`
3. Use keyspace
  `cqlsh> USE test;`
4. Create table
  `cqlsh> CREATE TABLE tasks(id UUID, task text, PRIMARY KEY(id));`
5. Create dummy data
  `cqlsh> INSERT INTO tasks(id, task) VALUES (now(), 'dsmafksadkf');`
6. Verify dummy data exists in source cluster
  `cqlsh> SELECT * FROM tasks;`

# Create keyspace in destination cluster
1. CQLSH into the dest cluster
  `$ cqlsh localhost 9043`
2. Create keyspace `test`
  `cqlsh> CREATE KEYSPACE test WITH REPLICATION = { 'replication_factor': 1, 'class': 'SimpleStrategy' };`
3. Verify tasks table doesn't yet exist
  `cqlsh> SELECT * FROM tasks;`

# Access metrics endpoint
`curl localhost:8081`

# Running the Go script
`./migrate.sh`
