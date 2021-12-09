# Release Process

All published container images can be found at [https://hub.docker.com/r/datastax/cloudgate-proxy/tags?page=1&ordering=last_updated](https://hub.docker.com/r/datastax/cloudgate-proxy/tags?page=1&ordering=last_updated).

## Official/Stable Releases

Periodically, "official"/"stable" releases with standard semantic versioning applied are released to capture milestones for the project.  This process happens in automated fashion through the use of the GH Action workflow found in [versioned-release.yml](.github/workflows/versioned-release.yml).

The `versioned-release` workflow is triggered by the push of a semantically valid release tag being pushed to the repository, for example:

```
git tag v2.0.0
git push origin v2.0.0
```

The result of that workflow is the creation and publishing of a Docker image with a series of tags applied to it, for example:

* `v2.0.0` -- A tag marking the exact version published
* `v2.x` -- A moving tag at all times marking the most recent `major` version compatible image (e.g `2.0.0`, `2.0.1`, `2.1.0`, `2.2.0`)
* `v2.0.x` -- A moving tag at all times marking the most recent `minor` version compatible image (e.g `2.0.0`, `2.0.1`, `2.0.2`, `2.0.3`)
* `latest` -- A moving tag at all times marking the most recent stable image

### Create an official `Release` in GitHub

Once the tag has been push to the repository and the build has been verified, a `Release` should be created within GitHub matching the tag.  This is a manual step that must be completed after the automation.

1. Navigate in a browser to [https://github.com/riptano/cloud-gate/releases](https://github.com/riptano/cloud-gate/releases) and select the `Draft a new release` button.
2. Select the `Choose a tag` button and select the previously pushed tag, in our example, `v1.1.0` from the dropdown.
3. Set the `Release title` to the same name as the tag, in our example, `v1.1.0`.
4. Paste the contents of the `RELEASE_NOTES` relevant to this release into text-area for `Describe the release`.
5. Click the `Publish release` button.

## Per-Merge Releases

To support easier testing workflows, every merge to the primary `master` branch will also trigger a build and release cycle.

This happens automatically and requires no manual steps.  This process is provided for through the use of the GH Action workflow found in [push-release.yml](.github/workflows/push-release.yml).

The result of this workflow is the creation and publishing of a Docker image with a series of tags applied to it, for example:

* `sha-3758gd2b` -- A tag specific to the SHA of the commit merged
* `edge` -- A moving tag that at all times marks the most recently built image
