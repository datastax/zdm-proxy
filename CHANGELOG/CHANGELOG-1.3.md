# Changelog

Changelog for the Cloudgate Proxy, new PRs should update the `unreleased` section.

When cutting a new release, update the `unreleased` heading to the tag being generated and date, like `## vX.Y.Z - YYYY-MM-DD` and create a new placeholder section for  `unreleased` entries.

## Unreleased

### Improvements

* [ZDM-244](https://datastax.jira.com/browse/ZDM-244) Force the downgrade to v4 if a cluster responds with v5 during the handshake

### New Features

* [ZDM-48](https://datastax.jira.com/browse/ZDM-48) Replace now() function calls in PREPARE/EXECUTE messages
* [ZDM-252](https://datastax.jira.com/browse/ZDM-252) Replace now() function calls in BATCH messages
* [ZDM-267](https://datastax.jira.com/browse/ZDM-267) Fix proxy issue with graph requests sent by Fluent APIs in the GraphBinary protocol
* [ZDM-288](https://datastax.jira.com/browse/ZDM-288) Add a new flag to enable async reads on secondary cluster with metrics
* [ZDM-285](https://datastax.jira.com/browse/ZDM-285) Add a feature toggle to enable now() function call replacement (with false as default)
* [ZDM-319](https://datastax.jira.com/browse/ZDM-319) Add build directive to compile the proxy code with built-in profiling support
* [ZDM-262](https://datastax.jira.com/browse/ZDM-262) Client to Proxy Encryption

### Bug Fixes

* [ZDM-265](https://datastax.jira.com/browse/ZDM-265) Cluster server certificates (TLS) are not being verified

