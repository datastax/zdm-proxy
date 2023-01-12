# Changelog

Changelog for the ZDM Proxy, new PRs should update the `unreleased` section.

When cutting a new release, update the `unreleased` heading to the tag being generated and date, like `## vX.Y.Z - YYYY-MM-DD` and create a new placeholder section for  `unreleased` entries.

## Unreleased

## v2.1.0 - 2023-11-13

### New Features

* TBD Send heartbeats to clusters

### Improvements

* [#86](https://github.com/datastax/zdm-proxy/pull/86) Pin go version to 1.19

### Bug Fixes

* [#48](https://github.com/datastax/zdm-proxy/issues/48) Fix scheduler shutdown race condition
* [#69](https://github.com/datastax/zdm-proxy/issues/69) Client connection can be closed before proxy returns protocol error
* [#76](https://github.com/datastax/zdm-proxy/issues/76) Log error when closing connection
* [#74](https://github.com/datastax/zdm-proxy/issues/74) Handshakes with auth enabled can deadlock if multiple handshakes are happening concurrently