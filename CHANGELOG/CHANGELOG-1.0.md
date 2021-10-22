# Changelog

Changelog for the Cloudgate Proxy, new PRs should update the `unreleased` section.

When cutting a new release, update the `unreleased` heading to the tag being generated and date, like `## vX.Y.Z - YYYY-MM-DD` and create a new placeholder section for  `unreleased` entries.

## Unreleased

### New Features

* [ZDM-217](https://datastax.jira.com/browse/ZDM-217) Remove authentication coupling between origin and target clusters
* [ZDM-194](https://datastax.jira.com/browse/ZDM-194) Allow users to provide origin credentials

### Bug Fixes

* [ZDM-239](https://datastax.jira.com/browse/ZDM-239) Panic when connections are closed during handshake

## v1.1.0 - 2021-10-15

### New Features

* [ZDM-201](https://datastax.jira.com/browse/ZDM-201) Support TLS when connecting to self-managed clusters

## v1.0.4 - 2021-09-29

### Bug Fixes

* [ZDM-209](https://datastax.jira.com/browse/ZDM-209) Proxy fails when handling statements using Custom types

## v1.0.3 - 2021-09-28

### Bug Fixes

* [ZDM-208](https://datastax.jira.com/browse/ZDM-208) Proxy times out requests with empty batches

## v1.0.2 - 2021-09-24

### Bug Fixes

* [ZDM-203](https://datastax.jira.com/browse/ZDM-203) Batches with prepared statements cause driver timeouts

## v1.0.1 - 2021-09-22

### Bug Fixes

* [ZDM-200](https://datastax.jira.com/browse/ZDM-200) Astra returns "invalid keyspace" errors (topology out of date)
* [ZDM-199](https://datastax.jira.com/browse/ZDM-199) Protocol negotiation for downgrades from v5 is not handled properly

## v1.0.0 - 2021-09-14

* Initial Release
