# Container packaging

Each release publishes this image to `ghcr.io/sandrinodm/logbrook` for
`linux/amd64` and `linux/arm64`, tagged with its version (for example `0.2.0`).
Run a published image rather than building your own unless you need an
unreleased commit; [releases](RELEASING.md) covers tags, digests and
verification. This page describes what the image contains and how to build and
check it locally.

The production image contains the Logbrook executable and its runtime libraries.
Rust, compilers, build caches, and source files stay in the build stages. The
runtime stage is a digest-pinned, multi-architecture
`gcr.io/distroless/cc-debian13:nonroot` image, which supplies glibc, the C++
runtime that bundled DuckDB needs, and CA roots. It has no shell or package
manager. See the Distroless
[image list](https://github.com/GoogleContainerTools/distroless#what-images-are-available)
and [support policy](https://github.com/GoogleContainerTools/distroless/blob/main/SUPPORT_POLICY.md)
when choosing a new base.

The Logbrook MIT license is at `/usr/share/licenses/logbrook/LICENSE`. Dependency
licenses and attribution, including Apache Arrow notices and DuckDB's bundled
native components, are in `/usr/share/licenses/logbrook/THIRD_PARTY_NOTICES.txt`.
The build fails if that file's lockfile fingerprint doesn't match `Cargo.lock`,
and CI checks its full contents. See [license maintenance](../licenses/README.md)
when updating dependencies.

Before the runtime stage, a verification stage strips unneeded ELF symbols, runs
`logbrook --version`, and checks that every shared library resolves. These checks
don't replace the smoke test under [local verification](#local-verification).

The Dockerfile's default `VERSION` build argument, used for the image's version
label, matches the version in `Cargo.toml`, so local builds carry the right
label. The release helper updates both, and CI passes the version
explicitly.

## Runtime

The process runs directly as UID/GID `10001:10001`, and Docker sends SIGTERM to
it. `/data` is writable and owned by that UID, including on a fresh named volume.
Bind mounts need the same ownership. The healthcheck runs `logbrook healthcheck`
directly, without a shell.

The Compose file sets a read-only root, a 256 MiB `/tmp` tmpfs, drops all
capabilities, disables privilege escalation, and allows 35 seconds for shutdown.
Only `/data` and `/tmp` need to be writable. Compose sets no CPU or memory limit.
The configured DuckDB memory budget still applies, but it doesn't cap the whole
process. Keep secrets and configuration out of the build context.

## Optional Forward listener

The Forward receiver stays off until a configuration file enables it. The
[Forward Compose example](../examples/forward/compose.yaml) mounts that file
read-only, binds the listener on `0.0.0.0:24224` inside the container with
`trusted_network = true`, and publishes it only on the host's `127.0.0.1:24524`.
HTTP keeps its own port. The example keeps UID 10001, the read-only root, the
`/tmp` tmpfs, dropped capabilities, and the 35-second stop deadline. Other
containers on the same network can reach a trusted listener, so use
authenticated listeners across trust boundaries. See
[Forward configuration](FORWARD.md) for TLS, secret mounts, Unix sockets, and
application logging options.

## Local verification

```sh
docker build --build-arg VCS_REF="$(git rev-parse HEAD 2>/dev/null || printf working-tree)" -t logbrook:local .
cargo xtask image-inspect --image logbrook:local --max-image-mib 110
cargo xtask container-smoke --image logbrook:local
cargo xtask check-cli --image logbrook:local
```

The same checks run against a published image: pass
`--image ghcr.io/sandrinodm/logbrook:<version>` instead of building one.

`container-smoke` runs disposable containers on a fresh volume with the Compose
security settings. It checks API authentication and routing, ingestion and
search, the healthcheck, offline Parquet archival, search after a restart, and
shutdown on SIGTERM. It removes its containers and volumes even when a check
fails.

`image-inspect` reports the uncompressed local image size and the executable
size, checks the runtime user, and checks that the image contains the project
license and the dependency notices. The 110 MiB budget leaves room for
differences between architectures and some dependency growth. The compressed
download from a registry is a different size.

## CI artifacts

CI builds and smoke-tests native `linux/amd64` and `linux/arm64` images on
matching GitHub runners. Each build exports an OCI archive with an SBOM and
maximum provenance attestations, and loads the same build into Docker for the
smoke tests. CI keeps the OCI archive as an artifact for seven days and doesn't
publish to a registry. The [release workflow](RELEASING.md) runs the same checks
for the release commit and publishes those OCI archives to GHCR. The OCI archive
keeps the attestations; loading an image into Docker's classic image store can
drop them (see
[Docker's attestation documentation](https://docs.docker.com/build/metadata/attestations/)).

Load tests use `cargo xtask container-probe CONTAINER [COUNTERS]` to read four
cgroup counters of the target container: `memory.current`, `memory.peak`,
`memory.events`, and `cpu.stat`. The probe starts a disposable, digest-pinned
Debian container in the target's PID namespace, as UID 10001 with no network, no
capabilities, and a read-only root, and reads `/proc/1/root/sys/fs/cgroup`. It
doesn't mount host filesystems or add tools to the production image. If the host
denies that access, the probe fails; report the missing counters and use Docker
Engine statistics instead, which measure differently.

Digest pins don't pick up upstream security fixes, so review and update the base
image and toolchain pins regularly. CI verifies both architectures after an
update.
