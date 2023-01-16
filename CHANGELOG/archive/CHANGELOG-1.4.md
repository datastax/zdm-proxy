# Changelog

Changelog for the Cloudgate Proxy, new PRs should update the `unreleased` section.

When cutting a new release, update the `unreleased` heading to the tag being generated and date, like `## vX.Y.Z - YYYY-MM-DD` and create a new placeholder section for  `unreleased` entries.

## Unreleased

## v.1.4.1 - 2022-09-16

### Improvements

* [ZDM-390](https://datastax.jira.com/browse/ZDM-390) Change docker base image to alpine due to GCP scan tool requirements

## v.1.4.0 - 2022-08-19

### New Features

* [ZDM-322](https://datastax.jira.com/browse/ZDM-322) Enable the proxy to also connect to clusters that use the RandomPartitioner
* [ZDM-254](https://datastax.jira.com/browse/ZDM-254) Filter result set columns of intercepted queries according to the query
* [ZDM-255](https://datastax.jira.com/browse/ZDM-255) Intercept prepared statements for system.local and system.peers tables
