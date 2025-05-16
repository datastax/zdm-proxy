# Changelog

Changelog for the ZDM Proxy, new PRs should update the `unreleased` section.

When cutting a new release, update the `unreleased` heading to the tag being generated and date, like `## vX.Y.Z - YYYY-MM-DD` and create a new placeholder section for `unreleased` entries.

## Unreleased

* [#143](https://github.com/datastax/zdm-proxy/issues/143): Update ANTLR dependency

---

## v2.3.3 - 2025-04-29

* [#142](https://github.com/datastax/zdm-proxy/pull/142): Handle nil control connection when reconnecting

---

## v2.3.2 - 2025-04-14

* [#139](https://github.com/datastax/zdm-proxy/pull/139): Ignore forwarding CQL requests for DSE Insights Client to target cluster
* [#140](https://github.com/datastax/zdm-proxy/issues/140): Upgrade go version to fix CVE-2025-22871

---

## v2.3.1 - 2024-11-08

### New Features

* [#130](https://github.com/datastax/zdm-proxy/issues/130): Security vulnerabilities in ZDM version 2.3.0
* [#133](https://github.com/datastax/zdm-proxy/pull/133): Upgrade _google.golang.org/protobuf_ from 1.26.0-rc.1 to 1.33.0

### Improvements

### Bug Fixes

---

## v2.3.0 - 2024-07-04

### New Features

* [#123](https://github.com/datastax/zdm-proxy/pull/123): Support providing configuration of ZDM with YAML file

### Improvements

### Bug Fixes
