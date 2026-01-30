# Changelog

Changelog for the ZDM Proxy, new PRs should update the `unreleased` section.

When cutting a new release, update the `unreleased` heading to the tag being generated and date, like `## vX.Y.Z - YYYY-MM-DD` and create a new placeholder section for `unreleased` entries.

---

## v2.4.1 - 2026-01-30

### Bug Fixes

* [#154](https://github.com/datastax/zdm-proxy/issues/154): Fix too small buffer for LZ4 decompression

---

## v2.4.0 - 2026-01-16

### New Features

* [#150](https://github.com/datastax/zdm-proxy/issues/150): CQL request tracing
* [#154](https://github.com/datastax/zdm-proxy/issues/154): Support CQL request compression
* [#157](https://github.com/datastax/zdm-proxy/pull/157): Support protocol v5
* [#157](https://github.com/datastax/zdm-proxy/pull/157): New Configuration setting to block specific protocol versions

### Improvements

* [#157](https://github.com/datastax/zdm-proxy/pull/157): Improvements to CI so we can find regressions with multiple C* versions before merging a PR