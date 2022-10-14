# Changelog

Changelog for the ZDM Proxy, new PRs should update the `unreleased` section.

When cutting a new release, update the `unreleased` heading to the tag being generated and date, like `## vX.Y.Z - YYYY-MM-DD` and create a new placeholder section for  `unreleased` entries.

## Unreleased

### New Features

* [#31](https://github.com/datastax/zdm-proxy/issues/31) Always enable proxy topology system.peers
* [#29](https://github.com/datastax/zdm-proxy/issues/29) Log proxy version during startup and add -version command line parameter
* [#28](https://github.com/datastax/zdm-proxy/issues/28) Increase default max client connections to 1000
* [#27](https://github.com/datastax/zdm-proxy/issues/27) Configuration API revamp
* [#30](https://github.com/datastax/zdm-proxy/issues/30) Metrics revamp
* [#26](https://github.com/datastax/zdm-proxy/issues/26) Change docker base image from scratch to alpine

### Improvements

* [#41](https://github.com/datastax/zdm-proxy/issues/41) Do not send system queries to secondary when dual reads are enabled
* [#39](https://github.com/datastax/zdm-proxy/issues/39) Request canceled warnings in the logs when client disconnects

### Bug Fixes

* [#38](https://github.com/datastax/zdm-proxy/issues/38) Drivers can not connect to ZDM-Proxy when ORIGIN is DSE 4.8.0

### Other

* [#32](https://github.com/datastax/zdm-proxy/issues/32) Rename product to ZDM-Proxy
* [#33](https://github.com/datastax/zdm-proxy/issues/33) Use Apache 2 license
