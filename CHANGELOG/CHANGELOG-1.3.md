# Changelog

Changelog for the Cloudgate Proxy, new PRs should update the `unreleased` section.

When cutting a new release, update the `unreleased` heading to the tag being generated and date, like `## vX.Y.Z - YYYY-MM-DD` and create a new placeholder section for  `unreleased` entries.

## Unreleased

### New Features

* [ZDM-48](https://datastax.jira.com/browse/ZDM-48) Replace now() function calls in PREPARE/EXECUTE messages
* [ZDM-252](https://datastax.jira.com/browse/ZDM-252) Replace now() function calls in BATCH messages
* [ZDM-267](https://datastax.jira.com/browse/ZDM-267) Fix proxy issue with graph requests sent by Fluent APIs in the GraphBinary protocol
* [ZDM-288](https://datastax.jira.com/browse/ZDM-288) Add a new flag to enable async reads on secondary cluster with metrics