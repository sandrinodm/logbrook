# Publishing container images

The **Release image** GitHub Actions workflow publishes to GitHub Container Registry (GHCR). It runs manually, separately from normal verification. Pushing a commit or tag does not publish an image.

Images use `ghcr.io/<owner>/<repository>`, with both names lowercased. A release publishes one image supporting `linux/amd64` and `linux/arm64`, including dependency notices, an SBOM, and build provenance.

## Repository setup

1. Push the repository, including both workflows under `.github/workflows/`, to GitHub. The release workflow must exist on the default branch for its **Run workflow** button to appear.
2. Ensure the repository or organization permits GitHub Actions to publish packages. Only the publishing job requests `packages: write`; verification jobs have read-only repository access.
3. If the GHCR package already exists, connect it to this repository and grant the repository Actions access to it.

The workflow uses the built-in `GITHUB_TOKEN`. No Docker Hub account, personal access token, or registry secret is required. See [GitHub's registry authentication guidance](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-in-a-github-actions-workflow).

## Prepare a version

Set the application version in the root `Cargo.toml`. Keep `Cargo.lock` and the distributed notices current:

```sh
cargo check --package logbrook
cargo xtask notices
cargo xtask notices --check
```

Notice generation requires the tool described in [license maintenance](../licenses/README.md). Commit the release changes, then create and push an annotated tag matching the application version. For the current `0.1.0` version:

```sh
git tag -a v0.1.0 -m 'Logbrook 0.1.0'
git push origin v0.1.0
```

Use `vX.Y.Z` for a stable version or `vX.Y.Z-rc.1` for a prerelease. Build metadata such as `+build.1` is not supported in release tags. Treat version tags as permanent; publish a new version for changed code.

## Run the release

1. Open **Actions → Release image → Run workflow**.
2. Select the default branch as the workflow source.
3. Enter the existing tag, for example `v0.1.0`.
4. Enable **Also update latest** only when this stable release should become the default image.
5. Run the workflow and wait for `prepare`, `verify`, and `publish` to succeed.

The workflow checks that the tag matches `Cargo.toml`, resolves it to an exact commit, and runs the existing verification workflow against that commit. This includes Rust and Pino checks, license notices, and native container builds and smoke tests for both architectures.

Publication starts only after every verification job passes. It transfers the verified OCI archives by digest and combines their manifests without rebuilding. The archive's image configuration is checked against the locally tested image before upload. Skopeo copies all manifests with digest preservation, retaining SBOM and provenance attestations; Docker Buildx assembles the multi-platform image. See [Skopeo's copy options](https://github.com/containers/skopeo/blob/main/docs/skopeo-copy.1.md) and [Docker's image-copying guidance](https://docs.docker.com/build/ci/github-actions/copy-image-registries/).

The resulting tags are:

| Tag | Purpose |
| --- | --- |
| `0.1.0` | The selected version, without the Git tag's `v` prefix |
| `sha-<full-commit>` | The source commit used by this release |
| `latest` | Updated only when explicitly selected for a stable version |

Prereleases cannot update `latest`. Existing version tags are accepted only when their manifest matches the verified artifacts exactly. If the publishing job fails transiently, use **Re-run failed jobs** to reuse those same artifacts. Re-running every job may produce different provenance; use a new release version rather than replacing an existing image.

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
