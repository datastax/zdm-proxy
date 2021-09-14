# Release Notes

General release notes for the Cloudgate Proxy.

Build artifacts are available at [Docker Hub](https://hub.docker.com/repository/docker/datastax/cloudgate-proxy).

For additional details on the changes included in a specific release, see the associated CHANGELOG-x.x.md file.

## v1.0.0 - 2021-09-14

The initial release of the proxy supporting dual-write capabilities for live data migration between C* compatible origin and target clusters.

[Changelog](CHANGELOG-1.0.md#v100---2021-09-14)

### Known Issues

* [ZDM-199](https://datastax.jira.com/browse/ZDM-199) Protocol negotiation for downgrades from v5 is not handled properly
* [ZDM-200](https://datastax.jira.com/browse/ZDM-200) Proxy isn't registering for protocol events
