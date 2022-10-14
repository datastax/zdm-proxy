# Zero Downtime Migration (ZDM) Proxy

## Overview

The ZDM Proxy is client-server component written in Go that enables users to migrate with zero downtime from an Apache
Cassandra&reg; cluster to another (which may be an [Astra](https://astra.datastax.com/) cluster) and not requiring code
changes in the application client.

The only change to the client is pointing it to the proxy rather than directly to the original cluster (Origin). In turn,
the proxy connects to both Origin and Target clusters.

By default, the proxy will forward read requests only to the Origin cluster, though you can optionally configure it to
forward reads to both clusters asynchronously, while writes will always be sent to both clusters concurrently.

An overview of the proxy architecture and logical flow can be viewed [here](#).

## Quick Start

In order to run the proxy, you'll need to set some environment variables to configure it properly.
Below you'll find a list with the most important variables along with their default values.
The required ones are marked with a comment.

```shell
ZDM_PROXY_TOPOLOGY_INDEX=0
ZDM_PROXY_TOPOLOGY_ADDRESSES=127.0.0.1
ZDM_ORIGIN_ENABLE_HOST_ASSIGNMENT=true
ZDM_TARGET_ENABLE_HOST_ASSIGNMENT=true
ZDM_ORIGIN_CONTACT_POINTS=127.0.0.1 #required
ZDM_ORIGIN_USERNAME=cassandra       #required
ZDM_ORIGIN_PASSWORD=cassandra       #required
ZDM_ORIGIN_PORT=9042
ZDM_TARGET_CONTACT_POINTS=127.0.0.1 #required
ZDM_TARGET_USERNAME=cassandra       #required
ZDM_TARGET_PASSWORD=cassandra       #required
ZDM_TARGET_PORT=9043
ZDM_METRICS_ADDRESS=127.0.0.1
ZDM_METRICS_PORT=14001
ZDM_PROXY_LISTEN_PORT=14002
ZDM_PROXY_LISTEN_ADDRESS=127.0.0.1
ZDM_ORIGIN_CONNECTION_TIMEOUT_MS=30000
ZDM_TARGET_CONNECTION_TIMEOUT_MS=30000
ZDM_HEARTBEAT_INTERVAL_MS=30000
ZDM_METRICS_ENABLED=true
ZDM_PROXY_MAX_CLIENT_CONNECTIONS=1000
ZDM_PRIMARY_CLUSTER=ORIGIN
ZDM_READ_MODE=PRIMARY_ONLY
ZDM_PROXY_REQUEST_TIMEOUT_MS=10000
ZDM_LOG_LEVEL=INFO
```

The environment variables must be set and exported for the proxy to work.

In order to get started quickly, in your local environment, grab a copy of the binary distribution in the
[Releases](https://github.com/datastax/zdm-proxy/releases) page. For the recommended installation in a production
environment, check the [Production Setup](#production-setup) section below. 

Now, suppose you have two clusters running at `10.0.0.1` and `10.0.0.2` with `cassandra/cassandra` credentials
and the same key-value [schema](nb-tests/schema.cql). You can start the proxy and connect it to these clusters like this:

```shell
$ export ZDM_ORIGIN_CONTACT_POINTS=10.0.0.1 \ 
export ZDM_TARGET_CONTACT_POINTS=10.0.0.2 \
export ZDM_ORIGIN_USERNAME=cassandra \
export ZDM_ORIGIN_PASSWORD=cassandra \
export ZDM_TARGET_USERNAME=cassandra \
export ZDM_TARGET_PASSWORD=cassandra \
./zdm-proxy-v2.0.0 # run the ZDM proxy executable
```

At this point, you should be able to connect some client such as [CQLSH](https://downloads.datastax.com/#cqlsh) to the proxy
and write data to it and the proxy will take care of forwarding the requests to both clusters concurrently.

```shell
$ cqlsh <proxy-ip-address> 14002 # this is the proxy's default listen port
```

From the CQLSH prompt:

```cql
cqlsh> INSERT INTO test.keyvalue (key, value) VALUES (1, 'ABC');
cqlsh> INSERT INTO test.keyvalue (key, value) VALUES (2, 'DEF');
cqlsh> SELECT * FROM test.keyvalue;
cqlsh> UPDATE test.keyvalue SET value='GYEKJF' WHERE key = 1;
cqlsh> DELETE FROM test.keyvalue WHERE key = 2;
```
You can confirm that the data is stored in both clusters by querying them directly in other cqlsh sessions.

Note: For the moment, the keyspace must be specified when accessing a table, even after using `USE <keyspace>`.

If you don't have test clusters readily available to try with, check the [alternative](./CONTRIBUTING.md#running-on-localhost-with-docker-compose) method with docker-compose in the
[Contributor's guide](./CONTRIBUTING.md), which will set up all the dependencies, including two test clusters and a proxy instance, in a
containerized sandbox environment.

## Production Setup

The setup we described above is only for testing in a local environment. It is **NOT** recommended for a production
installation where the minimum number of proxy instances is 3.

For a comprehensive guide with the recommended production setup check the documentation available at

There you'll find information about an Ansible-based tool that automates most of the process.

## Project Dependencies

For information on the packaged dependencies of the Zero Downtime Migration (ZDM) Proxy and their licenses, check out our [open source report](https://app.fossa.com/reports/910065e9-5620-4ed7-befb-b69c45ebce6e).

## Frequently Asked Questions

For frequently asked questions, please refer to our separate [FAQ](faq.md) page.