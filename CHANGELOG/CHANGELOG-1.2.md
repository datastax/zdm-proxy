# Changelog

Changelog for the Cloudgate Proxy, new PRs should update the `unreleased` section.

When cutting a new release, update the `unreleased` heading to the tag being generated and date, like `## vX.Y.Z - YYYY-MM-DD` and create a new placeholder section for  `unreleased` entries.

## Unreleased

## v1.2.2 - 2022-03-24

### New Features

* [ZDM-278](https://datastax.jira.com/browse/ZDM-278) Add a new flag to enable async reads on secondary cluster

## v1.2.1 - 2021-11-24

### Improvements

* [ZDM-246](https://datastax.jira.com/browse/ZDM-246) Replace go's type 1 UUID generator

### Tasks

* [ZDM-243](https://datastax.jira.com/browse/ZDM-243) Support for latest native-protocol API changes

## v1.2.0 - 2021-10-22

### New Features

* [ZDM-217](https://datastax.jira.com/browse/ZDM-217) Remove authentication coupling between origin and target clusters
* [ZDM-194](https://datastax.jira.com/browse/ZDM-194) Allow users to provide origin credentials

### Bug Fixes

* [ZDM-239](https://datastax.jira.com/browse/ZDM-239) Panic when connections are closed during handshake
