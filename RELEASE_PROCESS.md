# Release Process

All published container images can be found at [https://hub.docker.com/r/datastax/cloudgate-proxy/tags?page=1&ordering=last_updated](https://hub.docker.com/r/datastax/cloudgate-proxy/tags?page=1&ordering=last_updated).

## Official/Stable Releases

### Before publishing an official release

Before triggering the build and publish process for an official/stable release, two files need to be updated, the `RELEASE_NOTES` and `CHANGELOG`.

The [RELEASE_NOTES.md](RELEASE_NOTES.md) file should be updated so that it contains a section for the new release.

The `CHANGELOG.md` file associated with the release (in the [CHANGELOG](CHANGELOG) folder) should be updated so that tickets in the `UNRELEASED` section are moved to a new section (in the same file) that is specific to the new release.

These changes should be done on a branch so that a PR can be opened for review prior to merging them.

### Building and publishing the docker image

Periodically, "official"/"stable" releases with standard semantic versioning applied are released to capture milestones for the project.  This process happens in automated fashion through the use of the GH Action workflow found in [release.yml](.github/workflows/release.yml).

If a semantically valid release tag is pushed then the `release.yml` workflow will consider it an official/stable release and tag the docker image with a series of tags as shown in the following example.

As an example, let's say we want to publish a `v2.0.0` official release. To do that, push a `v2.0.0` tag:

```
git tag v2.0.0
git push origin v2.0.0
```

The result of that workflow is the creation and publishing of a Docker image with a series of tags applied to it as mentioned before, for example:

* `v2.0.0` -- A tag marking the exact version published
* `v2.x` -- A moving tag at all times marking the most recent `major` version compatible image (e.g `2.0.0`, `2.0.1`, `2.1.0`, `2.2.0`)
* `v2.0.x` -- A moving tag at all times marking the most recent `minor` version compatible image (e.g `2.0.0`, `2.0.1`, `2.0.2`, `2.0.3`)
* `latest` -- A moving tag at all times marking the most recent stable image

### Create an official `Release` in GitHub

Once the tag has been pushed to the repository and the build has been verified, a `Release` should be created within GitHub matching the tag.  This is a manual step that must be completed after the automation.

1. Navigate in a browser to [https://github.com/riptano/cloud-gate/releases](https://github.com/riptano/cloud-gate/releases) and select the `Draft a new release` button.
2. Select the `Choose a tag` button and select the previously pushed tag, in our example, `v1.1.0` from the dropdown.
3. Set the `Release title` to the same name as the tag, in our example, `v1.1.0`.
4. Paste the contents of the `RELEASE_NOTES` relevant to this release into text-area for `Describe the release`.
5. Click the `Publish release` button.

## Per-Merge Releases

To support easier testing workflows, every commit that is pushed to the primary `main` branch will trigger a build and publish of a non official release.

This happens automatically and requires no manual steps.  This process is provided for through the use of the same GH Action workflow that is used for official releases, i.e., [release.yml](.github/workflows/release.yml).

The result of this workflow is the creation and publishing of a Docker image with only the `main` tag applied to it (no `latest`).

## Manually triggered test releases on any branch

To support easier testing workflows without requiring a merge to `main`, you can manually trigger the [push-release.yml](.github/workflows/push-release.yml) workflow (selecting which branch you want to use in the Github UI) which will result in the creation and publishing of a Docker image with a tag specific to the SHA of the commit from which it was built (e.g. `sha-3758gd2b`).
