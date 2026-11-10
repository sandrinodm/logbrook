# Developer tools

`cargo xtask` runs the `logbrook-dev` package in `xtask/`. It has the container checks, the release version helper and the load generators. It is a separate workspace package, so none of its dependencies end up in the server executable or the runtime image.

Run every command from the repository root. Native checks start their own server on temporary data. Container checks need Docker and create throwaway containers and volumes. Write reports to `artifacts/`, which Git ignores.

## Checks run by CI

| Check | What it verifies |
| --- | --- |
| `cargo test --package logbrook --test process` | Acknowledged events survive SIGKILL and restart; archival, relocated restore, the native CLI, retention precedence, per-index limits and index isolation |
| `cargo test --workspace --all-targets --locked` | Server behavior, load scheduling, bounded queues and shutdown, worker failures, truncated responses, report accounting and HTTP metrics |
| `cargo xtask container-smoke --image IMAGE` | The packaged API, authentication, Parquet archival, restart and container hardening |
| `cargo xtask check-cli --image IMAGE` | Index management and queries through the packaged CLI |
| `cargo xtask image-inspect --image IMAGE --max-image-mib 110` | Image size, executable size, runtime user, and license and dependency notices |
| `cargo xtask notices --check` | The checked-in notices match the locked Linux application dependencies |
| `cargo xtask index-load --image IMAGE --duration 2 --rate 200 --query-rate 10 --output artifacts/index-smoke.json` | Configuration mounting, concurrent index traffic, live tail and container cleanup |

CI runs the process tests as part of the workspace tests. The full command lists are in [development checks](../README.md#development) and [container verification](../docs/CONTAINER.md#local-verification). `cargo xtask check-cli --binary target/debug/logbrook` runs the CLI check against a native server instead of an image. `cargo xtask version` prints the application version from the root `Cargo.toml`, which CI uses to label the image.

The [release workflow](../docs/RELEASING.md) prepares the version and tag, runs these checks, publishes the container image to GHCR and creates a GitHub release.

`cargo xtask notices` needs the pinned `cargo-about` version; [license maintenance](../licenses/README.md) covers setup and updates. Only maintainers and CI run it. Source and Docker builds use the checked-in notices.

## Release versions

`cargo xtask release-version --dry-run patch` prints the next version without changing any files. The argument is `current`, `patch`, `minor`, `major`, or an explicit SemVer version without a leading `v`, such as `0.2.0-rc.1`. To promote a prerelease, pass the stable version explicitly.

Without `--dry-run`, it writes the new version to the root `Cargo.toml`, the application entry in `Cargo.lock`, the lockfile fingerprint in `THIRD_PARTY_NOTICES.txt`, the default version in `Dockerfile` and `info.version` in `src/openapi.json`. Dependency versions, manifest comments, API schemas and license texts stay as they are. It refuses to write when the existing version fields disagree, the notices are stale or the new version is lower than the current one.

The helper only edits files. The manual **Release** Actions workflow commits them, pushes the tag, runs the checks, publishes the image and creates the GitHub release. Use `current` to release the version already in `Cargo.toml`.

`cargo test --package logbrook-dev --lib release --locked` runs the workflow's shell scripts against local throwaway Git remotes and a mocked GitHub API, so nothing is published. The tests cover accepted and rejected patches, prerelease handling, stale branches, existing tags, a failed atomic push and release retries. They need Bash, Git and jq, which the CI runners have.

## Load generators

| Command | Use |
| --- | --- |
| `cargo xtask mixed-load` | Ingestion, search and live tail against a running server you can throw away |
| `cargo xtask index-load` | Concurrent ingestion and queries across two indexes on a fresh native server or container |
| `cargo xtask forward-load` | Forward TCP sessions, ACKs and optional concurrent HTTP and query traffic on a fresh native server |
| `cargo xtask attribute-load` | Attribute search and aggregates, live tail and paired ingestion runs on hot and Parquet data |
| `cargo xtask attribute-compare` | The same queries against two images, on identical restored data |
| `cargo xtask extended-load` | A longer container run with archived data and fixed CPU and memory limits |
| `cargo xtask container-probe CONTAINER [COUNTERS]` | Reads cgroup counters from a running container |

Each generator lists its options with `--help`. For example, a one-minute run across two indexes:

```sh
cargo build --package logbrook --locked --release
cargo xtask index-load --binary target/release/logbrook \
  --duration 60 --rate 1000 --query-rate 20 \
  --output artifacts/index-load.json
```

`index-load` takes `--binary` or `--image`, plus `--duration`, `--rate`, `--query-rate`, `--batch-size`, `--padding-bytes` and `--output`. It and `extended-load` start and remove their own server or container.

`mixed-load` writes to the server at `--url` (default `http://127.0.0.1:3100`), so point it at a server whose data you don't need. Set `LOGBROOK_INGEST_TOKEN`, and set `LOGBROOK_READ_TOKEN` to a read token that can see every source. Its options cover batch size, event padding, ingest and query clients, query rate, tail subscribers and tail drain time.

`forward-load` starts a native server with a loopback Forward listener and deletes its data afterwards. `--mode` is `message`, `forward`, `packed` or `gzip`, and `--ack` requests acknowledgements. `--http-rate` and `--query-rate` add HTTP ingestion and queries alongside the Forward traffic. Start with a short mixed run:

```sh
cargo xtask forward-load --binary target/debug/logbrook \
  --duration 10 --rate 1000 --connections 100 --batch-size 100 \
  --mode forward --ack --http-rate 100 --query-rate 10 \
  --output artifacts/forward-smoke.json
```

Repeat with `--connections 1000` and `--connections 10000` to separate the cost of idle and active connections, or set `--duration 3600` for a one-hour run.

The limits for `forward-load` batches:

- A Forward batch holds up to 10,000 events and must encode to 8 MiB or less.
- With `--http-rate`, batches are limited to 1,000 events, the HTTP limit.
- `message` mode requires `--batch-size 1`.
- The server runs with one index, a 128 MiB writer queue budget and a 1,024-command writer queue. For batches of 1,000 events or more it raises the normalized envelope limit to 64 MiB. Normalizing adds overhead even to small records, so start a 10,000-event run with `--padding-bytes 0`.

### Reading a report

Reports record the parameters, the server binary's hash, the platform and raw server metric samples. Container reports keep Docker statistics and cgroup counters separate. Tail measurements record arrivals and how long events took to reach subscribers.

`forward-load` sends at the offered rate no matter how slowly the server responds. When a client queue fills, the report counts dropped work and scheduler lag. A lost ACK is counted as ambiguous and never retried. The generator gives every event a sequence ID and, after the sessions drain, pages through the index to count unique commits, duplicates, confirmed events that are missing and attempted events that never resolved. Without `--ack`, socket writes and commits seen in queries are counted separately. `passed` requires every planned event to commit and every confirmed event to be queryable, so a deliberate overload run fails but still reports its counts. `receiver_drain_seconds` and `verification_seconds` time the drain and the ID scan separately.

`index-load` counts a truncated HTTP response as a failed request, and ingestion with an unknown outcome as ambiguous; it doesn't retry either. An unexpected worker error stops the run and appears in `worker_errors`. Workers have 30 seconds to drain; a run that takes longer fails and its server is removed. Failed final HTTP checks appear in `verification_errors`.

`container-probe` reads only `memory.current`, `memory.peak`, `memory.events` and `cpu.stat`, through a separate observer container, so the runtime image needs no inspection tools.

Run the scheduling, shutdown and accounting unit tests with `cargo test --package logbrook-dev --lib --locked`. The workspace tests also run the process-based load and cleanup tests.

### Attribute queries

`attribute-load` generates deterministic run, user and session data, imports it offline, archives the requested share and starts a server on it. It checks search IDs, counts, histogram buckets and facet values against answers computed from the fixture. The `queries` profile also checks missing and single-value selections, sixteen conditions and later cursor pages.

For a short container run, build a local image and run every profile with smaller settings:

```sh
docker build -t logbrook:attributes-local .
cargo xtask attribute-load --image logbrook:attributes-local \
  --rows 10000 --runs 100 --samples 5 --warmup 1 \
  --profile all --duration 3 --live-warmup-seconds 0 --paired-repetitions 1 \
  --output artifacts/attribute-smoke.json
```

| Option | Default |
| --- | --- |
| `--rows` | 1,000,000 events |
| `--metadata-bytes` | 1,024 |
| `--archive-percent` | 90 |
| `--warmup` | 100 requests per query cell |
| `--samples` | 1,000 measured requests per query cell |
| `--profile` | `queries` |

`--profile tail` measures how quickly matching events reach live-tail subscribers. `--profile paired` alternates baseline and comparison ingestion runs on fresh identical fixtures, three pairs by default, each with 60 seconds of warmup and ten measured minutes. `--profile all` runs all three profiles.

Container runs get four CPUs, 1 GiB of memory and DuckDB's `memory_limit='512MB'`. Runs with `--binary` have no resource limits. `--hardware-provenance FILE` copies a JSON description of the host and its storage into the report; `--help` lists its fields.

Add `--promoted-string-key runId --promoted-string-key userId` to `attribute-load` or `attribute-compare` to test promoted attribute columns. The report records the server configuration in effect and uses the offline `projection-status` command to check that the fixture is fully covered.

### Compare attribute query changes

`attribute-compare` runs the same queries against two images. It imports and archives one fixture, stops the server and snapshots the data directory. It then runs baseline, candidate, candidate, baseline, each in a new container restored from the snapshot, and checks successful responses against the fixture. Failed requests are kept in the report.

```sh
cargo run --release --locked --package logbrook-dev -- attribute-compare \
  --baseline-image logbrook:attributes-local \
  --candidate-image logbrook:attributes-optimized \
  --output artifacts/attribute-compare.json
```

The defaults are one million events, 20 warmup and 50 measured requests per query cell in each of the four runs. For a quick check, add `--rows 10000 --runs 100 --warmup 1 --samples 3`. `--cpu-diagnostics` records container CPU counters around each batch of ten requests. Stop builds and other load generators while measuring. The tool doesn't clear operating-system or Docker caches between runs.

### Comparing results

A result describes one workload. When comparing measurements, include record sizes, concurrency, CPU and memory limits, storage and server configuration. The process crash tests use SIGKILL, so they don't cover power loss.
