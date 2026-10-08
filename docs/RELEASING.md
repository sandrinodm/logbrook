# Releases and container images

The manual **Release** GitHub Actions workflow prepares the application version, creates and pushes its Git tag, verifies and publishes the container image, then creates a GitHub release with generated notes. The separate **Release image** workflow remains available for publishing an existing tag. Normal pushes and pull requests run verification only.

Images use `ghcr.io/<owner>/<repository>`, with both names lowercased. A release publishes one image supporting `linux/amd64` and `linux/arm64`, including dependency notices, an SBOM, and build provenance.

## Repository setup

1. Push the repository, including `.github/workflows/create-release.yml`, to `main`. The workflow must exist on the default branch for its **Run workflow** button to appear.
2. Allow GitHub Actions to write repository contents and publish packages. Repository rules must permit the release job to push its version commit to `main` and create tags. If a rule blocks the bot, resolve that repository policy before releasing; the workflow does not bypass protection or force-push.
3. If the GHCR package already exists, connect it to this repository and grant the repository Actions access to it.

The workflows use the built-in `GITHUB_TOKEN`. Version preparation and verification have read-only repository access. Only committing/tagging and GitHub release creation request `contents: write`; only image publication requests `packages: write`. No Docker Hub account, personal access token, or registry secret is required. See [GitHub's registry authentication guidance](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-in-a-github-actions-workflow).

## Create a release

1. Commit and push the intended changes to `main`.
2. Open **Actions → Release → Run workflow** and select `main`.
3. Enter a version request from the table below. For the first `0.1.0` release, use **`current`**.
4. Enable **Also mark a stable release as latest** if it should update both the GHCR `latest` tag and GitHub's latest-release designation.
5. Run the workflow. It handles version files, the tag, image publication, and the GitHub release.

| Version request | Result when Cargo.toml contains `0.1.0` |
| --- | --- |
| `current` | Release `0.1.0` without a version bump |
| `patch` | Release `0.1.1` |
| `minor` | Release `0.2.0` |
| `major` | Release `1.0.0` |
| `0.2.0-rc.1` | Release that exact prerelease version |

Explicit versions omit the leading `v`. Downgrades, build metadata such as `+build.1`, and existing tags are rejected. To promote a prerelease, specify its intended stable version explicitly. Prereleases cannot be marked latest. Version selection is manual; Conventional Commit messages do not automatically choose an increment.

The preparation job updates five version fields: the root `Cargo.toml`, its application entry in `Cargo.lock`, the lockfile fingerprint in `THIRD_PARTY_NOTICES.txt`, the default image version in `Dockerfile`, and `info.version` in `src/openapi.json`. The image label and API document describe the application release. A version-only update leaves dependency versions, API schemas, and license texts untouched. Existing metadata must match the application version, and notices must match the lockfile; dependency updates still require [normal notice regeneration](../licenses/README.md).

Preparation builds the Rust helper with read-only repository access and saves a patch. A separate runner with write access checks that the patch changes exactly those fields, then commits it as `chore(release): v<version>`. It never builds or runs project dependencies. Checkout credentials are not persisted, Git hooks are disabled for commit/tag/push, and authentication is supplied only to the final Git step. For `current`, the patch is empty and no empty version commit is created.

The version commit and annotated tag are pushed together with an atomic, non-forced Git push. If `main` advances while preparing, or the tag already exists, preparation fails instead of overwriting another change. The image workflow checks the tag against both the prepared commit and package version, then runs Rust and Pino checks, full notice verification, and native container builds and smoke tests for both architectures.

Only one release runs at a time. GitHub keeps one pending run in this concurrency group; another dispatch replaces that pending run. Start a release once and follow its existing run instead of repeatedly dispatching it.

Publication starts only after every verification job passes. It transfers the verified OCI archives by digest and combines their manifests without rebuilding. The archive's image configuration is checked against the locally tested image before upload. Skopeo copies all manifests with digest preservation, retaining SBOM and provenance attestations; Docker Buildx assembles the multi-platform image. See [Skopeo's copy options](https://github.com/containers/skopeo/blob/main/docs/skopeo-copy.1.md) and [Docker's image-copying guidance](https://docs.docker.com/build/ci/github-actions/copy-image-registries/).

The resulting tags are:

| Tag | Purpose |
| --- | --- |
| `0.1.0` | The selected version, without the Git tag's `v` prefix |
| `sha-<full-commit>` | The source commit used by this release |
| `latest` | Updated only when explicitly selected for a stable version |

After successful image publication, the final job creates `v<version>` in GitHub Releases with generated notes and a container pull command. It marks prereleases appropriately. Image publication is a direct reusable-workflow call, so it does not depend on a bot-created tag or release triggering another workflow. See [GitHub's reusable-workflow guidance](https://docs.github.com/en/actions/how-tos/reuse-automations/reuse-workflows) and [token-trigger behavior](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow).

## Preview or prepare a version locally

The release helper is part of the Rust development tools:

```sh
cargo xtask release-version --dry-run current
cargo xtask release-version --dry-run patch
cargo xtask release-version --dry-run 0.2.0-rc.1
```

It emits JSON containing the old version, target version, tag, and whether files would change. Removing `--dry-run` updates the five version-related files locally. This helper does not commit, tag, push, create a GitHub release, or publish an image. The Actions workflow owns those steps.

`cargo test --package logbrook-dev --lib release --locked` checks the version helper and runs the actual release scripts against disposable local Git remotes. It covers initial releases, increments, unusual valid prereleases, altered patches, concurrent main updates, existing tags, atomic push rejection, and mocked GitHub-release retries. It does not publish anything externally.

## Retry a failed release

Use **Re-run failed jobs** in the original run. A tag is reserved before verification, so a failed build leaves that tag in place even though no GitHub release has been announced. Re-running failed jobs preserves the prepared version and reuses successful jobs and available artifacts. Do not delete or move the tag to work around a failure.

If code changes are needed, commit the fix and start a new release with a new version. Re-running all jobs or starting another release at `current` can encounter an existing tag; a fresh `patch` run can create another version. Choose intentionally rather than repeatedly retrying the whole release.

If image publication fails transiently, retry its failed job with the same verified artifacts. Existing image versions are accepted only when their manifests match exactly; rebuilding can change provenance even for the same source. OCI artifacts are retained for seven days. If they are no longer available, prepare a new version instead of replacing a published image.

If only GitHub release creation fails, retry that final job. An already published GitHub release for the tag is accepted, so a lost API response does not require another image publication.

## Publish an existing tag

The **Release image** workflow is an image-only path for an existing, pushed Git tag matching the root application version. It verifies and publishes that tag, with an optional stable `latest` update. It neither creates the tag nor creates a GitHub release.

An error such as `couldn't find remote ref refs/tags/v0.1.0` means the selected Git tag is absent on the remote. For a new release, use **Release** with `current` or an increment rather than entering an uncreated tag into **Release image**.

## Make the first package public

GHCR packages are initially private, even when their source repository is public. After the first successful release, open the package's **Package settings**, find **Change visibility**, and choose **Public**. This permits anonymous pulls. See [GitHub's package visibility instructions](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility).

Once public, users can pull the versioned image:

```sh
docker pull ghcr.io/<owner>/<repository>:0.1.0
```

Replace the placeholders with the actual lowercase GitHub owner and repository names. For deployments that must use exactly the same artifact every time, use the digest shown by the pull command:

```sh
docker pull ghcr.io/<owner>/<repository>@sha256:<digest>
```

The release does not change the local Compose setup, which continues to build from source. Configuration, credentials, storage, and retention work identically with the published image.
