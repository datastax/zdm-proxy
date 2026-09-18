# Changelog

When adding a change, add it under an `## unreleased` section at the top (create one if it doesn't exist). When cutting a new release, rename that section to `## vX.Y.Z - YYYY-MM-DD`.

---

## v2.5.1 - 2026-09-18

### Improvements

* [#175](https://github.com/datastax/zdm-proxy/issues/175): Migrate container registry from Docker Hub to Quay.io
* [#177](https://github.com/datastax/zdm-proxy/issues/177): Publish multi-arch (`linux/amd64`, `linux/arm64`) Docker images

---

## v2.5.0 - 2026-07-21

### Improvements

* [#172](https://github.com/datastax/zdm-proxy/issues/172): Upgrade Go to 1.26.5

### New Features

* [#98](https://github.com/datastax/zdm-proxy/issues/98): Add a size limit to the proxy's prepared statement cache