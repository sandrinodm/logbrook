# Developer tools

The `logbrook-dev` Rust workspace package under `xtask/` verifies Logbrook containers and generates repeatable load. Run it through the repository's `cargo xtask` alias. Developer-tool dependencies stay separate from the application package, and the runtime image contains none of these tools.

Run commands from the repository root. Native process checks use temporary data and manage their own servers. Container checks require Docker and create disposable containers and volumes. Save reports under the ignored `artifacts/` directory.

## Checks run by CI

| Check | What it verifies |
| --- | --- |
| `cargo test --package logbrook --test process` | Acknowledged events after SIGKILL, restart, archival, relocated restore, native CLI, retention precedence, per-index limits and isolation |
| `cargo test --workspace --all-targets --locked` | Server behavior, load scheduling, bounded queues and shutdown, worker failures, truncated responses, report accounting and HTTP metrics |
| `cargo xtask container-smoke --image IMAGE` | Packaged API, authentication, Parquet archival, restart and container hardening |
| `cargo xtask check-cli --image IMAGE` | Index management and queries through the packaged CLI |
| `cargo xtask image-inspect --image IMAGE --max-image-mib 110` | Local image size, executable size, runtime user, license and dependency notices |
| `cargo xtask notices --check` | Dependency licenses and attribution match the locked Linux application dependencies |
| `cargo xtask index-load --image IMAGE --duration 2 --rate 200 --query-rate 10 --output artifacts/index-smoke.json` | Configuration mounting, concurrent index traffic, live tail and container cleanup |

The [development checks](../README.md#development) and [container verification](../docs/CONTAINER.md#local-verification) list the full commands. `cargo xtask check-cli --binary target/debug/logbrook` also runs the management CLI check against a disposable native server. `cargo xtask version` reads the application version from the root manifest for image packaging.

The [release workflow](../docs/RELEASING.md) prepares a version and tag, runs these checks, publishes the verified container artifacts to GHCR, and creates a GitHub release.

Notice generation requires the pinned `cargo-about` tool. See [license maintenance](../licenses/README.md) for setup and updates. This tool is used only by maintainers and CI; source builds and Docker builds use the checked-in notices.

## Release versions

`cargo xtask release-version --dry-run patch` previews the next version without changing files. Accepts `current`, `patch`, `minor`, `major`, or an explicit SemVer value such as `0.2.0-rc.1`. Use an explicit stable version to promote a prerelease. Without `--dry-run`, it updates the application manifest, lockfile entry, and notice fingerprint, preserving dependency versions, manifest comments, and license text. Stale notices and downgrades are rejected before writing.

This helper prepares files only. The manual **Release** Actions workflow performs the commit, atomic tag push, verification, image publication, and GitHub release creation. Use `current` for the initial release of the version already in `Cargo.toml`.

## Load generators

| Command | Use |
| --- | --- |
| `cargo xtask mixed-load` | Ingestion, search and live tail against an existing disposable server |
| `cargo xtask index-load` | Concurrent ingestion and queries across two indexes on a fresh native server or container |
| `cargo xtask extended-load` | A longer container run with archived data and fixed CPU and memory limits |
| `cargo xtask container-probe CONTAINER [COUNTERS]` | Read selected fixed container resource counters |

Use `--help` on each generator to see its workload and report options. For example, run a one-minute workload across two indexes:

```sh
cargo build --package logbrook --locked --release
cargo xtask index-load --binary target/release/logbrook \
  --duration 60 --rate 1000 --query-rate 20 \
  --output artifacts/index-load.json
```

`mixed-load` writes to the server selected with `--url`; use disposable data and set `LOGBROOK_INGEST_TOKEN` and an unrestricted `LOGBROOK_READ_TOKEN`. It supports bounded batches, padding, producer/query concurrency, query rate, tail subscriber count, and tail drain time. `index-load` and `extended-load` start and clean up their own server or container. `index-load` accepts `--binary` or `--image`, plus `--duration`, `--rate`, `--query-rate`, `--batch-size`, `--padding-bytes`, and `--output`.

`index-load` counts truncated HTTP responses as failed requests. Uncertain ingestion is reported as ambiguous and never retried. Unexpected worker errors stop scheduling and appear in `worker_errors`. Workers share a 30-second drain deadline; exceeding it fails the probe and cleans up its server. Final HTTP check failures appear in `verification_errors`. Run scheduling, shutdown and accounting unit tests with `cargo test --package logbrook-dev --lib --locked`. The workspace checks also run the process-based load and cleanup tests.

Reports retain offered, acknowledged and failed-request accounting, bounded workload visibility checks, latency measurements, and server metrics. Tail measurements include arrivals and observation latency; they do not establish a delivery guarantee. Container reports distinguish Docker statistics from cgroup counters. The container probe reads only `memory.current`, `memory.peak`, `memory.events`, and `cpu.stat` through a disposable observer rather than adding inspection utilities to the runtime image.

Load results describe the selected workload. Include record sizes, concurrency, CPU and memory limits, storage, and server configuration when comparing measurements. Process crash checks exercise SIGKILL, not power loss. Size-pressure fixtures verify measured-footprint admission targets rather than filesystem hard quotas or sustained capacity; any managed-ballast fallback is reported explicitly.
