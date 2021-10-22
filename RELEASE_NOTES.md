# Release Notes

General release notes for the Cloudgate Proxy.

Build artifacts are available at [Docker Hub](https://hub.docker.com/repository/docker/datastax/cloudgate-proxy).

For additional details on the changes included in a specific release, see the associated CHANGELOG-x.x.md file.

## v1.2.0 - 2021-10-22

Makes authentication more configurable and resolves a potential issue related to unexpected connection closure timing.

[Changelog](CHANGELOG/CHANGELOG-1.2.md#v120---2021-10-22)

## v1.1.0 - 2021-10-15

Adds support for mTLS/TLS between the proxy and origin/target clusters.

[Changelog](CHANGELOG/CHANGELOG-1.1.md#v110---2021-10-15)

## v1.0.4 - 2021-09-29

Enables the proxy to handle statements using Custom data types.

[Changelog](CHANGELOG/CHANGELOG-1.0.md#v104---2021-09-29)

## v1.0.3 - 2021-09-28

Resolves an issue where empty batches caused client timeouts.

[Changelog](CHANGELOG/CHANGELOG-1.0.md#v103---2021-09-28)

## v1.0.2 - 2021-09-24

Resolves an issue where batched sets of prepared statements caused client timeouts.

[Changelog](CHANGELOG/CHANGELOG-1.0.md#v102---2021-09-24)

## v1.0.1 - 2021-09-22

Addresses a number of issues identified through early user testing of the initial release.

[Changelog](CHANGELOG/CHANGELOG-1.0.md#v101---2021-09-22)

### Known Issues

* [ZDM-203](https://datastax.jira.com/browse/ZDM-203) Batches with prepared statements cause driver timeouts

## v1.0.0 - 2021-09-14

The initial release of the proxy supporting dual-write capabilities for live data migration between C* compatible origin and target clusters.

[Changelog](CHANGELOG/CHANGELOG-1.0.md#v100---2021-09-14)

### Known Issues

* [ZDM-199](https://datastax.jira.com/browse/ZDM-199) Protocol negotiation for downgrades from v5 is not handled properly
* [ZDM-200](https://datastax.jira.com/browse/ZDM-200) Proxy isn't registering for protocol events
