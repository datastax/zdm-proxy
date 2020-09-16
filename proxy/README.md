# Proxy Service

## Overview

This is a simple proxy component to enable users to migrate without downtime from a Cassandra cluster to another (which may be an Astra cluster) 
without requiring code changes in the application client. 

The only change to the client is pointing it to the proxy rather than directly to the original cluster. In turn, the proxy connects to both origin and target clusters.

The proxy will forward read requests only to the origin cluster, while writes will be sent to both clusters concurrently.

An overview of the proxy architecture and logical flow will be added here soon. 

## Environment Variables

`ORIGIN_CASSANDRA_HOSTNAME=localhost`<br/>
`ORIGIN_CASSANDRA_USERNAME=cassandra`<br/>
`ORIGIN_CASSANDRA_PASSWORD=cassandra`<br/>
`ORIGIN_CASSANDRA_PORT=9042`<br/>
`TARGET_CASSANDRA_HOSTNAME=localhost`<br/>
`TARGET_CASSANDRA_USERNAME=cassandra`<br/>
`TARGET_CASSANDRA_PASSWORD=cassandra`<br/>
`TARGET_CASSANDRA_PORT=9043`<br/>
`PROXY_SERVICE_HOSTNAME=localhost`<br/>
`PROXY_COMMUNICATION_PORT=14000`<br/>
`DEBUG=true`<br/>
`TEST=true`<br/>
`PROXY_METRICS_PORT=14001`<br/>
`PROXY_QUERY_PORT=14002`<br/>

These environment variables must be set and exported for the proxy to work. They are read and processed into a `Config` struct, which is passed into the Proxy Service.

## Running and testing the proxy locally

Launch two Cassandra single-node clusters as docker containers, one listening on port `9042` and the other on `9043`:

`docker run --name cassandra-source -p 9042:9042 -d cassandra`<br/>
`docker run --name cassandra-dest -p 9043:9042 -d cassandra`<br/>

Open cqlsh directly on each of these clusters:

`docker exec -it cassandra-source /bin/bash`<br/>
`cqlsh`<br/>

`docker exec -it cassandra-dest /bin/bash`<br/>
`cqlsh`<br/>

Create a keyspace + table directly on each cluster, for example:
`create keyspace test with replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1};`<br/>
`create table test.keyvalue (key int primary key, value text);`<br/>

Clone this project into the following directory, using the exact same path specified here: `~/go/src/github.com/riptano`<br/>

If using IntelliJ Goland or the go plugin for IntelliJ Idea Ultimate, create a run configuration as shown here:

![](img/cloudgate_proxy_run_config.png)
  
In the configuration, use this environment variable list: `ORIGIN_CASSANDRA_HOSTNAME=localhost;ORIGIN_CASSANDRA_USERNAME=cassandra;ORIGIN_CASSANDRA_PASSWORD=cassandra;ORIGIN_CASSANDRA_PORT=9042;TARGET_CASSANDRA_HOSTNAME=localhost;TARGET_CASSANDRA_USERNAME=cassandra;TARGET_CASSANDRA_PASSWORD=cassandra;TARGET_CASSANDRA_PORT=9043;PROXY_SERVICE_HOSTNAME=localhost;PROXY_COMMUNICATION_PORT=14000;DEBUG=true;TEST=true;PROXY_METRICS_PORT=14001;PROXY_QUERY_PORT=14002`

Start the proxy with the newly created run configuration.

Install a cqlsh standalone client (https://downloads.datastax.com/#cqlsh) and connect to the proxy: `./cqlsh --protocol-version="4" localhost 14002`.

Note that:
* At the moment, the protocol version negotiation does not yet work. The proxy works with any protocol version but it has to be explicitly specified for now.
* cqlsh code completion is not working yet
* If you are debugging you may want to increase the timeouts to have more time to step through the code. The options are: ` --connect-timeout="XXX" --request-timeout="YYY" `
* For the moment, the keyspace must be specified when accessing a table, even after using `USE <keyspace>`.

Once connected, experiment sending some requests through the proxy. For example:

`insert into test.keyvalue (key, value) values (1, 'ABC');`
`insert into test.keyvalue (key, value) values (2, 'DEF');`
`select * from test.keyvalue`
`update test.keyvalue set value='GYEKJF' where key = 1;`
`delete from test.keyvalue where key = 2`

And verify that the data is in both clusters by querying them directly through their own cqlsh.

To test prepared statements, there is a simple noSQLBench activity under nb-tests that can be launched like this:

`java -jar nb.jar run driver=cql workload=~/go/src/github.com/riptano/cloud-gate/nb-tests/cql-nb-activity.yaml tags=phase:'rampup' cycles=20..30 host=localhost port=14002 cbopts=".withProtocolVersion(ProtocolVersion.V3)"`
