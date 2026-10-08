# Container packaging

The production image contains the Logbrook executable and its runtime libraries.
Rust, compilers, build caches, and source files stay in build stages. The final
runtime uses a digest-pinned `gcr.io/distroless/cc-debian13:nonroot` multiarchitecture
image, supplying glibc, the C++ runtime needed by bundled DuckDB, and CA roots.
It contains no shell or package manager. The Logbrook MIT license is included at
`/usr/share/licenses/logbrook/LICENSE`. Dependency licenses and attribution,
including Apache Arrow notices and DuckDB's bundled native components, are in
`/usr/share/licenses/logbrook/THIRD_PARTY_NOTICES.txt`. The build checks its
lockfile fingerprint, and CI verifies the complete generated contents. See
[license maintenance](../licenses/README.md) when updating dependencies.
Debian 13 is the currently maintained
Distroless series; see the [upstream image list](https://github.com/GoogleContainerTools/distroless#what-images-are-available)
and [support policy](https://github.com/GoogleContainerTools/distroless/blob/main/SUPPORT_POLICY.md).

The final stage depends on a verification stage that strips unneeded ELF symbols,
executes the version command, and checks shared-library resolution with build
stage tools. Stripping preserves runtime unwind information and does not change
Rust optimization, panic handling, or DuckDB compilation. Build checks are not a
substitute for the packaged runtime smoke test below.

When the application build step runs, it refreshes both Rust crate entry-point
timestamps before invoking Cargo. A shared target cache can otherwise consider
newly copied sources older than a prior build's dependency timestamps and reuse
an outdated application. Native dependency artifacts remain cached; changes to
the executable or API specification rebuild the application.

The process runs directly as UID/GID `10001:10001`; Docker forwards SIGTERM to it.
`/data` is writable and owned by that UID, including fresh named volumes. Bind
mounts must be provisioned with matching ownership. The healthcheck invokes
`logbrook healthcheck` directly and needs no shell. Compose sets a read-only root,
a 256 MiB `/tmp` tmpfs, drops all capabilities, disables privilege escalation, and
provides 35 seconds for shutdown. Compose sets no CPU or memory limit; the
configured DuckDB memory budget still applies and is not a total process cap.
The service requires only `/data` and `/tmp`
to be writable. Avoid mounting secrets or configuration into the build context.

## Local verification

```sh
docker build --build-arg VCS_REF="$(git rev-parse HEAD 2>/dev/null || printf working-tree)" -t logbrook:local .
cargo xtask image-inspect --image logbrook:local --max-image-mib 110
cargo xtask container-smoke --image logbrook:local
cargo xtask check-cli --image logbrook:local
```

The smoke test creates disposable containers and a fresh volume, verifies API
authentication, API routing, ingestion/search, the executable
healthcheck, offline Parquet archival, search after restart, and graceful SIGTERM
shutdown under the Compose security settings. It reads persisted files with
`docker cp` rather than relying on runtime inspection utilities. All owned
containers and volumes are removed even on failure.

`cargo xtask image-inspect` reports uncompressed local image bytes and executable bytes,
checks the runtime user, and verifies both the project license and dependency notices.
The 110 MiB image budget allows architecture differences and modest dependency
growth while keeping the runtime small.
Compressed registry/download size differs from this local image measurement.

## CI artifacts

CI builds and smoke-tests native `linux/amd64` and `linux/arm64` images on matching
GitHub runners. A Docker-container BuildKit builder exports an OCI archive with
SBOM and maximum provenance attestations, then loads the same cached build into
Docker for smoke tests. The OCI archive is retained for seven days as a CI
artifact; normal verification does not publish to a registry. The separate
[release workflow](RELEASING.md) creates the version tag, invokes the same checks
for its exact commit, and publishes the verified OCI artifacts to GHCR before
creating a GitHub release. The OCI exporter retains attestations,
whereas loading into the classic Docker image store can discard them; see
[Docker's attestation documentation](https://docs.docker.com/build/metadata/attestations/).

Load probes use `cargo xtask container-probe CONTAINER [COUNTERS]` to read only four fixed target cgroup
counters (`memory.current`, `memory.peak`, `memory.events`, `cpu.stat`). A disposable
digest-pinned official Debian observer joins the target PID namespace as UID
10001 with no network, capabilities, or privilege escalation and a read-only
root. It reads `/proc/1/root/sys/fs/cgroup`; it does not mount host filesystems
or add utilities to the production image. Permission failures propagate without
privilege retries. On hosts that deny this access, report the missing cgroup
counters and use Docker Engine statistics with their differing semantics.

Base and toolchain pins need regular reviewed updates. Digest pinning makes a
build's inputs explicit; it does not apply future upstream security fixes
automatically. Packaging CI verifies both architecture variants after an update.
