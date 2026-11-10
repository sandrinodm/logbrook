<h1><img src="docs/assets/logbrook-banner.webp" alt="Logbrook" width="720"></h1>

[![GHCR container image](https://img.shields.io/badge/GHCR-container%20image-2496ED?logo=docker&logoColor=white)](https://github.com/sandrinodm/logbrook/pkgs/container/logbrook)

Self-hosted log search for structured application logs.

Logbrook is a single Rust executable that receives JSON logs over HTTP, stores recent events in an embedded DuckDB database, moves older events into local Parquet files, and searches both through its CLI and HTTP API. It runs on one server with one data directory and needs no external database or message queue.

> **Status:** Logbrook is pre-release software. Each [release](https://github.com/sandrinodm/logbrook/releases) publishes a container image for Linux amd64 and arm64 to `ghcr.io/sandrinodm/logbrook`, which is the recommended way to run it. You can also [build it from source](#build-from-source). APIs and storage formats may change before a stable release.

## Features

- **HTTP ingestion** of Pino-style JSON arrays or NDJSON, authenticated with ingestion tokens that each map to a source. Success is returned only after the batch is committed.
- **Named indexes**, each with its own retention period and disk-size target. The first authorized write to a new index creates it, up to a configurable index limit.
- **Search** by time range, source, service, logger, host, minimum level, message substring, and exact attribute values, with cursor pagination, counts, facets, and histograms.
- **Live tail** over server-sent events, with replay after reconnects and an explicit notice when events were missed.
- **Parquet archival** of older events, which stay searchable together with recent ones.
- **CLI** in the same executable for querying, counting, and managing indexes on a running server.
- **Fluent Forward receiver**, off by default, for Docker's `fluentd` logging driver, Fluentd, and Fluent Bit.
- **Offline import** of Pino JSON or NDJSON files.
- **Web UI** as a separate image, `ghcr.io/sandrinodm/logbrook-ui`, released with the server under the same version. See the [UI guide](ui/README.md#deploy).
- **Operations**: Prometheus metrics, health and readiness endpoints, and a Distroless container image with no shell that runs as UID 10001.

## How it works

Applications send logs to Logbrook over HTTP. Each index stores recent logs in embedded DuckDB and archives older logs as local Parquet files. The CLI and other applications query both through Logbrook's HTTP API.

<img src="docs/assets/logbrook-architecture.webp" alt="Web apps, APIs, and workers send logs to Logbrook. Recent logs are stored in DuckDB and archived to Parquet files. The CLI and other apps send queries to Logbrook and receive results from both stores." width="800">

## Quick start

These steps run the published container image on `127.0.0.1:3100`, send an event, and search it. You need Docker, plus `openssl` and `curl` on the host. Nothing persists after you remove the container; for a setup that keeps its data, use [Docker Compose](#run-with-docker-compose).

### 1. Generate credentials

Logbrook needs a separate ingestion token and read token:

```sh
export LOGBROOK_INGEST_TOKEN="$(openssl rand -hex 24)"
export LOGBROOK_READ_TOKEN="$(openssl rand -hex 24)"
```

### 2. Start the container

```sh
docker run -d --name logbrook \
  -p 127.0.0.1:3100:3100 \
  -e LOGBROOK_INGEST_TOKEN \
  -e LOGBROOK_READ_TOKEN \
  ghcr.io/sandrinodm/logbrook:latest

docker exec logbrook logbrook healthcheck
```

The image listens on port 3100 and stores data in `/data`. The `127.0.0.1:` prefix publishes the port on the loopback interface only. If the health check fails, run `docker logs logbrook`; the server writes JSON logs to stderr.

`latest` points at the newest release. For anything you keep, pin a version tag such as `ghcr.io/sandrinodm/logbrook:0.2.0`, or a digest, from the [package page](https://github.com/sandrinodm/logbrook/pkgs/container/logbrook).

### 3. Send an event

This writes one event to an index named `payments`:

```sh
curl --fail http://127.0.0.1:3100/indexes/payments/logs/ingest \
  -H "Authorization: Bearer $LOGBROOK_INGEST_TOKEN" \
  -H 'Content-Type: application/json' \
  --data "[{\"time\":$(( $(date +%s) * 1000 )),\"level\":30,\"service\":\"checkout\",\"msg\":\"Order accepted\",\"orderId\":\"demo-1\"}]"
```

The response is `{"accepted":1}`. Because `payments` did not exist yet, this write also created the index.

### 4. Search

The container already has the read token in its environment, so run the CLI inside it:

```sh
docker exec logbrook logbrook query payments --since 1h
```

The table shows the matching event. Add `--json` to see the complete record, including its `orderId` attribute.

### 5. Clean up

```sh
docker rm -f logbrook
```

## Run with Docker Compose

The root [`compose.yaml`](compose.yaml) runs the published image with a named data volume and a hardened container. Both credentials must be set in the shell that runs Compose:

```sh
export LOGBROOK_INGEST_TOKEN="$(openssl rand -hex 24)"
export LOGBROOK_READ_TOKEN="$(openssl rand -hex 24)"
docker compose up -d
docker compose exec logbrook logbrook query payments --since 1h
```

Keep both values. You need the read token to query logs, and Compose requires both whenever it recreates the container. To enable index deletion, also export `LOGBROOK_ADMIN_TOKEN` before starting Compose.

Compose uses `ghcr.io/sandrinodm/logbrook:latest` unless you set `LOGBROOK_IMAGE`, for example to a pinned release such as `ghcr.io/sandrinodm/logbrook:0.2.0`, or to an image you [built yourself](#build-from-source). The service is published on the host's `127.0.0.1:3100` and stores data in the `logs` named volume. Compose defaults to seven-day retention, a 20 GB size target per index, and at most four indexes. The [retention settings](#retention-and-disk-size) below can be exported before `docker compose up -d` to change them.

The runtime image is digest-pinned Distroless with no shell. The container runs as UID 10001 with a read-only root filesystem, no Linux capabilities, and a 35-second shutdown grace period. Compose sets no CPU or memory limit. Logbrook serves plain HTTP; put a TLS-terminating reverse proxy in front of it before exposing it beyond the local machine. See [container packaging](docs/CONTAINER.md) for image details and verification.

## Send logs from an application

Send batches to `POST /indexes/{index}/logs/ingest` as a JSON array (`application/json`) or as NDJSON (`application/x-ndjson`), with an ingestion token in the `Authorization: Bearer` header. Each event needs an integer Unix-millisecond `time`, a nonnegative integer `level`, and a string `msg`, which are Pino's defaults.

| Event field | Stored as |
| --- | --- |
| `time`, `level`, `msg` | `event_time`, `level`, `message` |
| `service`, `name`, `hostname`, `pid` | `service`, `logger`, `host`, `pid` |
| Any other field | `attributes` |

The `source` comes from the ingestion token, and the server generates each event ID. Payload fields with those names are kept in `attributes` and cannot override them.

By default, a request may contain up to 1,000 events and 1 MiB, each event up to 64 KiB, each string value up to 4,096 bytes, and nesting up to 16 levels. A batch is validated as a whole, so one invalid event rejects the entire request. Live ingestion also rejects events older than the index's retention window or more than five minutes in the future.

For a complete Node.js setup with batching, child loggers, error serialization, a paced bulk generator, and graceful shutdown, see the [Pino example](examples/pino-logger/README.md).

## Use the CLI

The `logbrook` executable is also an HTTP client for a running server. It reads the server address from `LOGBROOK_URL` (default `http://127.0.0.1:3100`). It takes the token from `--token` or `LOGBROOK_TOKEN`, and otherwise from the role variables described in [Credentials](#credentials):

```sh
logbrook indexes list                  # Size, size target, retention, readiness
logbrook indexes show payments --json  # Database, WAL, archive and temp bytes
logbrook query payments --since 1h --message error --limit 50
logbrook count payments --since 7d --min-level 40
logbrook indexes create audit
logbrook indexes delete audit --yes    # Admin token only; permanently removes data
```

With the container image, run the same commands inside the container: `docker exec logbrook logbrook ...`, or `docker compose exec logbrook logbrook ...` with Compose. They use the tokens in the container's environment. With a [source build](#build-from-source), use `target/release/logbrook`. Add `--json` for machine-readable output. The [CLI guide](docs/CLI.md) covers credentials, pagination, deletion, and use inside the container. The [query cookbook](docs/QUERY-EXAMPLES.md) has a twelve-event demo with expected results and recipes for filtering, exports, facets, histograms, and live tail.

## Configuration

Settings are resolved in this order: built-in defaults, then an optional TOML file passed with `--config`, then supported `LOGBROOK_*` environment variables.

With the container image, mount the file read-only and pass `--config`:

```sh
cp logbrook.example.toml logbrook.local.toml
# Edit logbrook.local.toml and replace every replace-with-... credential.
docker run --rm -v "$PWD/logbrook.local.toml:/etc/logbrook.toml:ro" \
  ghcr.io/sandrinodm/logbrook:latest --config /etc/logbrook.toml check-config
docker run -d --name logbrook -p 127.0.0.1:3100:3100 \
  -v "$PWD/logbrook.local.toml:/etc/logbrook.toml:ro" -v logbrook-data:/data \
  ghcr.io/sandrinodm/logbrook:latest --config /etc/logbrook.toml serve
```

The image sets `LOGBROOK_BIND=0.0.0.0:3100` and `LOGBROOK_DATA_DIR=/data`, which override the file's `bind` and `data_dir`. With a [source build](#build-from-source), run `target/release/logbrook --config logbrook.local.toml serve`. Files named `*.local.toml` are ignored by Git. Environment variables still override the file, so unset the quick-start token variables if you want the file's credentials to apply.

### Credentials

Logbrook has three roles. Each credential must contain 16 to 512 visible ASCII characters with no whitespace, and the roles must use different values. Example values beginning with `replace-with-` are rejected.

| Role | Environment | TOML | Grants |
| --- | --- | --- | --- |
| Ingestion | `LOGBROOK_INGEST_TOKEN`, with its source in `LOGBROOK_SOURCE` (default `default`) | `[ingest_tokens]`: token to source name | Writing events and creating indexes |
| Read | `LOGBROOK_READ_TOKEN`, with allowed sources in `LOGBROOK_READ_SOURCES` | `[read_tokens]`: token to a list of sources; an empty list allows all | Search, count, facets, histograms, live tail, and listing indexes; `/metrics` when unrestricted |
| Admin (optional) | `LOGBROOK_ADMIN_TOKEN` | `admin_tokens` | Listing, inspecting, creating, and deleting indexes; no access to events or metrics |

- `LOGBROOK_INGEST_TOKEN` replaces all ingestion tokens from the TOML file, and `LOGBROOK_ADMIN_TOKEN` replaces `admin_tokens`.
- `LOGBROOK_READ_TOKEN` is added to the readers from the TOML file. If the file already defines readers, also set `LOGBROOK_READ_SOURCES` to a comma-separated list, or to an empty value for all sources. Without configured readers, the environment reader can read all sources.
- Optional `[ingest_index_scopes]` and `[read_index_scopes]` tables restrict tokens to named indexes. Source scopes still apply.
- `/health` and `/ready` are public and reveal no log data.

### Indexes

- Index names have 1 to 63 lowercase letters, digits, hyphens, or underscores, and start with a letter or digit.
- The `default` index always exists. The unprefixed `/logs` routes address it.
- An index is created by the first authorized ingestion, by `PUT /indexes/{index}` (`logbrook indexes create`), or by declaring it in TOML as `[indexes.<name>]` or with `LOGBROOK_INDEXES=payments,audit`.
- At most `max_indexes` indexes may exist, four by default including `default`. Raise the limit with `LOGBROOK_MAX_INDEXES` or delete unused indexes with an admin token. The `default` index and declared indexes cannot be deleted.
- Each index lives in `<data_dir>/indexes/<name>/`. Existing index directories are discovered at startup.
- DuckDB memory, temporary-disk, and writer-queue budgets are divided across all `max_indexes` slots, so raising the limit gives each index a smaller share unless you also raise the totals.

### Retention and disk size

| Setting | TOML | Environment | Native default | Compose default |
| --- | --- | --- | --- | --- |
| Retention by event time | `retention_days` | `LOGBROOK_RETENTION_DAYS` | 7 days | 7 days |
| Disk-size target per index, decimal GB | `max_size_gb` | `LOGBROOK_MAX_SIZE_GB` | None | 20 |
| Maximum number of indexes | `max_indexes` | `LOGBROOK_MAX_INDEXES` | 4 | 4 |
| Move events to Parquet after | `archive_after_ms` | `LOGBROOK_ARCHIVE_AFTER_MS` | 1 day | 1 day |
| Maintenance interval | `maintenance_interval_secs` | `LOGBROOK_MAINTENANCE_INTERVAL_SECS` | 60 seconds | 60 seconds |

Top-level TOML values are inherited by every index, and `[indexes.<name>]` tables override them:

```toml
retention_days = 30
max_size_gb = 20

[indexes.audit]
retention_days = 90
max_size_gb = 5
```

- Limits apply to each index separately. 20 GB means 20,000,000,000 bytes.
- Changes take effect on restart. A shorter retention can expire events on the first maintenance pass; a longer one never restores expired events.
- Retention must be longer than the archive age.
- The size target counts database, WAL, archive, and temporary files. Maintenance expires the oldest events first, and ingestion returns `507` while the index remains at its target. It is not a hard ceiling: writes, maintenance, and query spill can briefly exceed it, and DuckDB can keep freed space allocated. Use a filesystem or volume quota when you need a hard limit.
- `retention_ms` (`LOGBROOK_RETENTION_MS`) and `max_size_bytes` (`LOGBROOK_MAX_SIZE_BYTES`) are also accepted. Do not set both units for the same setting in the environment.

The [operations guide](docs/OPERATIONS.md) documents every limit and setting, size-target behavior, backup and restore, upgrades, data-layout migration, and offline imports.

## HTTP API

The [API reference](docs/API.md) covers authentication, request and response formats, errors, and live tail. The [OpenAPI contract](src/openapi.json) is also served at `/openapi.json`.

| Endpoint | Purpose |
| --- | --- |
| `GET /indexes` | List the indexes the credential may access |
| `PUT /indexes/{index}` | Create an index (ingestion or admin credential); idempotent |
| `GET /indexes/{index}` | Show an index's measured size, retention, and readiness |
| `DELETE /indexes/{index}` | Permanently delete a dynamic index (admin credential) |
| `POST /indexes/{index}/logs/ingest` | Ingest a JSON array or NDJSON batch |
| `GET /indexes/{index}/logs` | Search, newest first, with an optional cursor |
| `GET /indexes/{index}/logs/count` | Count matching events |
| `GET /indexes/{index}/logs/facets` | Counts by service, logger, host, level, and source |
| `GET /indexes/{index}/logs/histogram` | Event counts per time bucket; optional `interval_ms` |
| `GET /indexes/{index}/logs/tail` | Live tail over server-sent events |
| `GET /health`, `GET /ready` | Liveness and storage readiness (public) |
| `GET /metrics` | Prometheus metrics (unrestricted read credential) |

- Search, count, facets, and histogram require `from` and `to` in Unix milliseconds. The range is UTC, includes `from`, excludes `to`, and may span at most 31 days by default (`max_query_range_ms`).
- Filters are `source` (comma-separated), `service`, `logger`, `host`, `message` (case-sensitive substring), `min_level`, and `filter` (URL-encoded JSON with exact top-level attribute matches). Live tail accepts the same filters.
- Search returns 100 events by default and at most 1,000 (`limit`). Use `next_cursor` with the same range and filters to fetch the next page. Cursors belong to one index and can expire when retention removes data.
- Event IDs are JSON strings. Reads from an unknown index return `404` and create nothing.
- The unprefixed `/logs`, `/logs/ingest`, `/logs/count`, `/logs/facets`, `/logs/histogram`, and `/logs/tail` routes are aliases for the `default` index.

## Guarantees and limitations

- **One process per data directory.** There is no clustering or replication.
- **Committed acknowledgments, with possible duplicates.** A success response means the batch was committed. If a client loses the response after commit and retries, the events are stored twice. Logbrook has no idempotency keys or deduplication; delivery also depends on the client's buffering and retry policy.
- **Crash tests kill the process.** They don't cover power loss or disk, filesystem, or VM failures.
- **Plain HTTP only.** Use a reverse proxy for TLS.
- **Structured filters only.** There is no query language or SQL endpoint.

## Build from source

Most setups don't need this: the [container image](#quick-start) is built from the same source for every release. Build it yourself to run an unreleased commit, a platform without an image, or the binary without Docker.

You need [rustup](https://rustup.rs), which installs the Rust version pinned in `rust-toolchain.toml` on the first `cargo` command, and a C++ compiler for the bundled DuckDB build. On macOS, install the Xcode command-line tools with `xcode-select --install`. If `cargo` is not found right after installing Rust, run `. "$HOME/.cargo/env"` or open a new shell.

From the repository root, build the binary and run it in the foreground with the credentials from the [quick start](#1-generate-credentials):

```sh
cargo build --package logbrook --locked --release
target/release/logbrook check-config
target/release/logbrook serve
```

The first build compiles DuckDB from source and takes several minutes. `serve` writes JSON logs to stderr; press Ctrl-C to stop it. Without a configuration file, the binary listens on `127.0.0.1:3100`, stores data in `./data`, keeps events for seven days, and has no disk-size target.

To build the container image instead and run it with Compose:

```sh
docker build -t logbrook:local .
LOGBROOK_IMAGE=logbrook:local docker compose up -d
```

## Development

Development uses the same pinned Rust toolchain as the server. Run the workspace checks from the repository root:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo build --package logbrook --locked
cargo test --workspace --all-targets --locked
```

Tests use temporary data directories and disposable servers. The process tests in `tests/process.rs` start a real server and check that acknowledged events survive SIGKILL, and cover restart, Parquet archival, restore to a new path, retention, per-index limits, and the management CLI. The release and skill-example tests need Bash, Git, jq, and OpenSSL.

`cargo xtask` runs the [developer tools](xtask/README.md): container verification and load generators. They live in the separate `logbrook-dev` package under `xtask/`, so their dependencies stay out of the server executable and image.

The Pino example has its own tests. CI checks it with Node.js 26.13.0 and npm:

```sh
(cd examples/pino-logger && npm ci && npm run format:check && npm test)
```

## Documentation

- [Concepts](docs/CONCEPTS.md): events, indexes, sources, archives, and retention.
- [API reference](docs/API.md): HTTP endpoints, authentication, payloads, and errors.
- [CLI guide](docs/CLI.md): remote commands, credentials, pagination, and index deletion.
- [Query cookbook](docs/QUERY-EXAMPLES.md): CLI and HTTP recipes with a reproducible demo dataset.
- [Fluent Forward receiver](docs/FORWARD.md): Docker logging, collector routing, authentication, and committed acknowledgments.
- [Operations and migration](docs/OPERATIONS.md): limits, retention, backups, upgrades, and imports.
- [Container packaging](docs/CONTAINER.md): image contents, hardening, and verification.
- [Releases and images](docs/RELEASING.md): versioning, Git tags, GitHub releases, and GHCR publication.
- [Pino example](examples/pino-logger/README.md): sending logs from Node.js.
- [Agent skill](skills/logbrook/SKILL.md): Docker installation, application logging, and log investigations. Copy the whole `skills/logbrook` directory into your agent's skill directory, or point the agent at its `SKILL.md` in this checkout.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, checks, and pull request guidance. Report vulnerabilities privately through the process in [SECURITY.md](SECURITY.md).

## License

Logbrook is licensed under the [MIT License](LICENSE).

Distributed images also include [third-party licenses and attribution](THIRD_PARTY_NOTICES.txt).
