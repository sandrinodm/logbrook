# Releases and container images

The manual **Release** GitHub Actions workflow sets the application version, creates and pushes the Git tag, verifies and publishes the container image, and then creates a GitHub release with generated notes. The **Release image** workflow publishes an image for a tag that already exists. Pushes and pull requests run verification only.

Images are published as `ghcr.io/<owner>/<repository>`, with both names lowercased. Each release is one image for `linux/amd64` and `linux/arm64`, with dependency notices, an SBOM, and build provenance.

## Repository setup

1. Push the repository, including `.github/workflows/create-release.yml`, to `main`. GitHub only shows the **Run workflow** button for workflows on the default branch.
2. Allow GitHub Actions to write repository contents and publish packages. Repository rules must let the release job push its version commit to `main` and create tags. The workflow doesn't bypass branch protection or force-push, so fix any rule that blocks the bot before releasing.
3. If the GHCR package already exists, connect it to this repository and give the repository's Actions access to it.

The workflows authenticate with the built-in `GITHUB_TOKEN`. Version preparation and verification get read-only repository access. Only the commit-and-tag job and the GitHub release job request `contents: write`, and only image publication requests `packages: write`. You don't need a Docker Hub account, a personal access token, or a registry secret. See [GitHub's registry authentication guidance](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-in-a-github-actions-workflow).

## Create a release

1. Commit and push the changes to `main`.
2. Open **Actions → Release → Run workflow** and select `main`.
3. Enter a version request from the table below. For the first release, `0.1.0`, use **`current`**.
4. Enable **Also mark a stable release as latest** to update both the GHCR `latest` tag and GitHub's latest release.
5. Run the workflow. It updates the version files, creates the tag, publishes the image, and creates the GitHub release.

| Version request | Result when Cargo.toml contains `0.1.0` |
| --- | --- |
| `current` | Release `0.1.0` without a version bump |
| `patch` | Release `0.1.1` |
| `minor` | Release `0.2.0` |
| `major` | Release `1.0.0` |
| `0.2.0-rc.1` | Release that exact prerelease version |

Write explicit versions without the leading `v`. The workflow rejects downgrades, build metadata such as `+build.1`, and versions whose tag already exists. To promote a prerelease, enter its stable version explicitly. A prerelease can't be marked latest. You always choose the increment; commit messages don't.

The release updates five version fields: the root `Cargo.toml`, the application's entry in `Cargo.lock`, the lockfile fingerprint in `THIRD_PARTY_NOTICES.txt`, the default image version in `Dockerfile`, and `info.version` in `src/openapi.json`. Dependency versions, API schemas, and license texts stay as they are. The existing fields must already agree on the application version, and the notices must match the lockfile, so a dependency update still needs [notice regeneration](../licenses/README.md) before you release.

The preparation job runs the version helper with read-only access and saves its changes as a patch. A separate job with write access checks that the patch changes only those fields and commits it as `chore(release): v<version>`. That job never builds or runs project code. For `current` the patch is empty and there is no version commit.

The version commit and the annotated tag go up together in one atomic, non-forced push. If `main` moved on during preparation, or the tag already exists, the run fails instead of overwriting anything. The image workflow then checks that the tag points to the prepared commit and matches the package version, and runs the full **Verify** workflow for that commit: Rust and Pino checks, notice verification, and native container builds and smoke tests for both architectures.

Only one release runs at a time. GitHub keeps at most one pending run in the queue, and a new dispatch replaces it, so start a release once and follow that run.

Publication starts only after every verification job passes. It uploads the OCI archives that verification built and tested, by digest, and combines them into a multi-platform image without rebuilding. Skopeo copies each archive with its SBOM and provenance attestations, and Docker Buildx assembles the multi-platform manifest. See [Skopeo's copy options](https://github.com/containers/skopeo/blob/main/docs/skopeo-copy.1.md) and [Docker's image-copying guidance](https://docs.docker.com/build/ci/github-actions/copy-image-registries/).

A release pushes these tags:

| Tag | Purpose |
| --- | --- |
| `0.1.0` | The release version, without the Git tag's `v` prefix |
| `sha-<full-commit>` | The source commit of this release |
| `latest` | Updated only when selected, for a stable version |

After the image is published, the last job creates the `v<version>` GitHub release with generated notes and a `docker pull` command, marked as a prerelease when the version has a prerelease suffix. The Release workflow calls the image workflow directly as a reusable workflow, so publication doesn't depend on a bot-created tag triggering another workflow. See [GitHub's reusable-workflow guidance](https://docs.github.com/en/actions/how-tos/reuse-automations/reuse-workflows) and [how the token affects workflow triggers](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow).

## Preview or prepare a version locally

The release helper is one of the Rust developer tools:

```sh
cargo xtask release-version --dry-run current
cargo xtask release-version --dry-run patch
cargo xtask release-version --dry-run 0.2.0-rc.1
```

It prints JSON with the previous version, the target version, the tag, and whether any file would change. Without `--dry-run` it updates the five version fields locally. It doesn't commit, tag, push, create a GitHub release, or publish an image; the workflow does those.

`cargo test --package logbrook-dev --lib release --locked` tests the helper and runs the release scripts against disposable local Git remotes. It covers first releases, increments, unusual but valid prereleases, tampered patches, `main` moving during a release, existing tags, rejected atomic pushes, and retried GitHub release creation with a mocked `gh`. Nothing is published.

## Retry a failed release

Use **Re-run failed jobs** in the original run. The tag is pushed before verification, so a failed build leaves the tag in place without a GitHub release. Re-running failed jobs keeps the prepared version and reuses the jobs and artifacts that succeeded. Don't delete or move the tag to get around a failure.

If the fix needs code changes, commit it and start a new release with a new version. Re-running all jobs, or starting another release with `current`, fails on the existing tag, and a fresh `patch` run creates another version. Pick one deliberately instead of retrying the whole release.

If image publication fails transiently, re-run its failed job with the same verified artifacts. If that version is already in GHCR, the job accepts it only when its manifests match exactly; a rebuild can change the provenance even from the same source. CI keeps the OCI artifacts for seven days. After that, release a new version instead of replacing a published image.

If only the GitHub release job fails, re-run it. It accepts a GitHub release that already exists for the tag, so a lost API response doesn't need another image publication.

## Publish an existing tag

The **Release image** workflow verifies and publishes an existing, pushed Git tag that matches the root application version, and can also update `latest` for a stable version. It doesn't create the tag or a GitHub release.

An error such as `couldn't find remote ref refs/tags/v0.1.0` means the tag doesn't exist on the remote. For a new release, run **Release** with `current` or an increment instead of entering a tag that hasn't been created into **Release image**.

## Make the first package public

GHCR packages start out private, even when the source repository is public. After the first successful release, open the package's **Package settings**, find **Change visibility**, and choose **Public** to allow anonymous pulls. See [GitHub's package visibility instructions](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility).

Users can then pull a version:

```sh
docker pull ghcr.io/<owner>/<repository>:0.1.0
```

Replace the placeholders with the lowercase GitHub owner and repository names. To get exactly the same image every time, pull by the digest that the pull command prints:

```sh
docker pull ghcr.io/<owner>/<repository>@sha256:<digest>
```

The Compose file still builds the image from source. The published image takes the same configuration, credentials, and data volume.
