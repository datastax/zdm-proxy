# Frequently Asked Questions

- [What versions of Apache Cassandra™ or CQL compatible data stores does the ZDM proxy support?](#what-versions-of-apache-cassandra-or-cql-compatible-data-stores-does-the-zdm-proxy-support)
- [What versions of the Cassandra driver are supported?](#what-versions-of-the-cassandra-driver-are-supported)
- [Will the ZDM proxy return a success if it succeeds with just the ORIGIN cluster?  What happens if a write to the TARGET cluster fails?](#will-the-zdm-proxy-return-a-success-if-it-succeeds-with-just-the-origin-cluster--what-happens-if-a-write-to-the-target-cluster-fails)
- [How does the ZDM proxy handle lightweight transactions?](#how-does-the-zdm-proxy-handle-lightweight-transactions)
- [Can the ZDM proxy be used indefinitely as a disaster recovery mechanism, i.e. having a backup cluster at the ready?](#can-the-zdm-proxy-be-used-indefinitely-as-a-disaster-recovery-mechanism-ie-having-a-backup-cluster-at-the-ready)

## What versions of Apache Cassandra™ or CQL compatible data stores does the ZDM proxy support?

The proxy can speak the CQL protocol versions 2, 3, and 4.  Version 5 support is coming.  That means that it can speak to Cassandra version 2.0+, DataStax Enterprise version 4.0+, and DataStax Astra.  It will likely work with other CQL compatible data stores but so far we have only tested with Cassandra, DataStax Enterprise, and Astra.

## What versions of the Cassandra driver are supported?

Any driver version that speaks the CQL binary protocol version 2+ will work with the ZDM proxy.

The other consideration is what driver version the TARGET cluster/environment will require. While nearly all driver versions can connect to the proxy, we would not want the user to have to change the driver in the middle of a migration. Therefore, prior to starting a migration, we recommend users upgrade or switch drivers to one that will accommodate the TARGET environment.

For example, consider when Astra is the TARGET environment. The ZDM proxy itself can speak to Astra with a secure connect bundle. However not every Cassandra driver can use a secure connect bundle.  Therefore, prior to starting a migration to Astra, we recommend users upgrade to a version that can use a secure connect bundle or, if that is prohibitive, use [cql-proxy](https://github.com/datastax/cql-proxy) as an intermediary.

## Will the ZDM proxy return a success if it succeeds with just the ORIGIN cluster?  What happens if a write to the TARGET cluster fails?

A write has to be acknowledged in both the ORIGIN and TARGET cluster at the requested consistency level for it to return a successful write acknowledgement to the client.  That means that if the write is unsuccessful in either cluster, a write failure comes back to the client. To be precise, an error will be returned by the cluster that is currently considered to be the source of truth.  By default that is the ORIGIN cluster.  If `FORWARD_READS_TO_TARGET` is set to true, it will be the TARGET cluster.

## How does the ZDM proxy handle lightweight transactions?

The ZDM proxy can bifurcate lightweight transactions to the ORIGIN and TARGET clusters. However, it only returns the `applied` flag from one cluster, whichever cluster is the source of truth, i.e. the cluster from where it returns synchronous read results to the client.  By default, that is the ORIGIN cluster.  However, if you set `FORWARD_READS_TO_TARGET`, the TARGET cluster will be considered the primary and read results from the TARGET cluster will be returned to the client, as well as the `applied` flag from any lightweight transactions.  Given that there are two separate clusters involved, the state of each cluster may be different.  For conditional writes, this may create a divergent state for a time.  It may not make a difference in many cases, but if lightweight transactions are used, we would recommend a reconciliation phase in the migration before switching reads to rely on the TARGET cluster.

## Can the ZDM proxy be used indefinitely as a disaster recovery mechanism, i.e. having a backup cluster at the ready?

The goal of the proxy is to have two separate clusters that are kept in sync for each update in preparation to cut over from the ORIGIN to the TARGET cluster.  Having the clusters separate and decoupled is a benefit for migrating between two different Cassandra clusters so that users can switch back and forth between clusters as needed in the migration process.  What this means practically is that if a write fails on either the ORIGIN or TARGET cluster, the client will receive a write failure.  This synchronous write requirement is necessary to keep the clusters in sync.  Also this decoupled cluster environment is different than a long-running two datacenter Cassandra cluster with anti-entropy.  For example there is no repair or read repair occurring between them.  Therefore, because of the synchronous write requirement across the two clusters and the write failure modes, we would only recommend using the proxy setup for the time it takes to be confident in the migration, not for a DR environment.  For a DR environment, we would recommend a single multi-datacenter/multi-region environment or possibly use CDC to send updates between clusters.
