# Operations and migration

## Storage ownership and durability

Only one Logbrook process may use a data root at a time. Each index lives in `indexes/<name>/` with its own `events.duckdb`, WAL, `archives/` and `temp/`. Index names are 1–63 lowercase ASCII letters, digits, hyphens or underscores, and start with a letter or digit. Index directories and files can't be symlinks.

An ingest batch is validated as a whole and the request succeeds only after the transaction commits. If a request times out or the client disconnects, the batch may or may not have committed, and retrying it can store duplicates. Logbrook has no idempotency keys.

Use the [HTTP CLI](CLI.md) to list, create, query and delete indexes while the server runs. Deletion needs a separate admin token (`LOGBROOK_ADMIN_TOKEN` or TOML `admin_tokens`), and the `default` index and indexes declared in the configuration can't be deleted. A deleted index's directory moves to `.index-trash/` and is cleaned up even if the client disconnects; failed cleanup is retried at startup or on the next deletion. `.index-trash/` is not a backup. Stop producers first, because the next ingest request recreates a deleted index.

Queries read a consistent snapshot of hot rows and archives on their own connections and don't block ingestion. Archive files a running query is reading stay on disk until it finishes.

Archive paths are stored relative to the data root, so a complete data directory can be moved. At startup Logbrook removes `.tmp` and unreferenced `.parquet` files from each archive directory, so keep other files out of it. A missing archive file, a file with an unexpected size or an unsupported archive schema is an integrity failure. The data directory path must be UTF-8 and can't contain `*`, `?`, `[` or `]`. JSON and Parquet support is built in; DuckDB never installs or loads extensions on its own.

Crash recovery is tested by killing the process with SIGKILL. Behavior on power loss depends on your disk, filesystem or VM and isn't tested.

## Fluent Forward ingestion

The optional [Fluent Forward receiver](FORWARD.md) runs in the same process, with its own TOML-only listeners, principals, routes and memory limits. Routes write only to declared indexes and apply the index's retention window. ACKs are sent after the envelope commits, and a sender that retries after a lost ACK can store duplicates. Read [delivery and compatibility](FORWARD.md#delivery-and-compatibility) and the [container example](../examples/forward/compose.yaml) before moving Docker producers.

## Limits and errors

| Default | Value |
| --- | --- |
| Request body / individual event | 1 MiB / 64 KiB |
| Events per request / field length / JSON nesting | 1,000 / 4 KiB / 16 levels |
| Non-tail HTTP handlers / TCP connections | 16 / 128 |
| Header / body idle / body total deadline | 10 / 5 / 30 seconds |
| Raw body admission budget | 16 MiB (accounts for incoming chunks and accumulated bytes) |
| Decoded request admission budget | 64 MiB |
| Writer queue | 16 messages per index / 32 MiB estimated bytes divided across index slots |
| Reader workers / DuckDB execution threads | 2 / 2 |
| DuckDB memory / temporary disk | 512 MB / 2 GB total, divided across index slots |
| Maximum indexes, including `default` | 4 |
| Query timeout / response bytes | 5 seconds / 8 MiB |
| Maximum search range / page size | 31 days / 1,000 events |
| Tail subscribers / output bytes each | 8 / 1 MiB |
| Tail fallback poll interval | 1 second; commits wake caught-up streams immediately |
| Archive chunk / event-time partition | 100,000 rows / 1 hour |
| Compaction input budget | 16 files / 128 MiB |
| Maintenance native-work deadline | 30 seconds |
| Graceful shutdown deadline | 30 seconds |

`storage.maintenance_timeout_ms` limits each maintenance job, including time spent waiting for the writer. A job that times out while committing or reconciling archives finishes that step first, so the timeout can be reported before the outcome is known and the writer stays busy until then.

The raw and decoded budgets limit memory held by HTTP request bodies. A request that stalls mid-body holds only the bytes it has sent. These budgets don't cap process memory; leave headroom for DuckDB and the rest of the process.

| Status | Meaning |
| --- | --- |
| `400` | Invalid input |
| `401` / `403` | Missing or invalid credential / credential not allowed for this source or index |
| `404` | Unknown index |
| `413` | Request or response size limit |
| `429` | Capacity pressure; includes `Retry-After: 1` |
| `503` | Storage unavailable or shutting down |
| `504` | Query deadline; the reader worker stays busy until DuckDB stops the query |
| `507` | The index has reached its disk target |

Retention is by event time. Live ingestion rejects events older than the retention window or more than `max_future_skew_ms` (default five minutes) in the future, and rechecks the stored expiry cutoff at commit. Offline imports accept historical timestamps, so configure retention to cover imported data before serving it. Expired events disappear from queries immediately, but an archive file is deleted only when every event in it has expired; partly expired files aren't rewritten. Compaction merges small files in the same event-time partition.

Set inherited defaults at the top level and per-index overrides in `[indexes.<name>]`:

```toml
retention_days = 30
max_size_gb = 20
max_indexes = 4
archive_after_ms = 86400000
maintenance_interval_secs = 60

[indexes.payments]
retention_days = 14
max_size_gb = 10

[indexes.audit]
retention_days = 90
max_size_gb = 20
```

| Setting | TOML key | Environment override | Native default |
| --- | --- | --- | --- |
| Keep events, days | `retention_days` | `LOGBROOK_RETENTION_DAYS` | 7 days |
| Keep events, milliseconds | `retention_ms` | `LOGBROOK_RETENTION_MS` | `604800000` |
| Disk target, decimal GB | `max_size_gb` | `LOGBROOK_MAX_SIZE_GB` | Disabled |
| Disk target, bytes | `storage.max_size_bytes` (global), `max_size_bytes` (index) | `LOGBROOK_MAX_SIZE_BYTES` | Disabled |
| Maximum index count | `max_indexes` | `LOGBROOK_MAX_INDEXES` | 4 |
| Precreated indexes | `[indexes.<name>]` | `LOGBROOK_INDEXES` (comma-separated names) | `default` |
| Archive age | `archive_after_ms` | `LOGBROOK_ARCHIVE_AFTER_MS` | `86400000` (1 day) |
| Maintenance interval | `maintenance_interval_secs` | `LOGBROOK_MAINTENANCE_INTERVAL_SECS` | `60` seconds |

Values must be positive integers. Retention must be longer than the archive age, so one-day retention needs a shorter archive age such as `archive_after_ms = 3600000`. Byte targets must be at least 16 MiB. At the top level of the TOML file, `retention_days` and `max_size_gb` win over `retention_ms` and `storage.max_size_bytes`; an index override or the environment may set only one unit per setting. Environment variables override top-level TOML values in either unit, and per-index settings override both. The root Compose file sets days and GB (7 days and 20 GB by default).

The disk target is a per-index retention target for measured file size, not a reservation or a hard limit. It counts the database, WAL, archives (including files held by running queries), spill files and other files Logbrook owns. Over the target, maintenance deletes the oldest event-time groups once queries release their files, and expires hot rows if needed. DuckDB reuses freed blocks but doesn't shrink its database file, even with `VACUUM` (see [DuckDB's space-reclamation documentation](https://github.com/duckdb/duckdb-web/blob/main/docs/current/operations_manual/footprint_of_duckdb/reclaiming_space.md)), so Logbrook stops evicting hot rows when that frees no space. While the index stays at its target, ingestion returns `507` until space is reclaimed or you raise the target and restart. Transactions, exports, compaction and query spills can briefly exceed the target, so monitor free disk separately and use a filesystem quota if you need a hard limit.

HTTP request, body, decoded-memory, connection and tail budgets are shared by all indexes. `storage.memory_limit`, `storage.temp_limit` and `storage.queue_bytes` are divided by `max_indexes`, counting unused slots, so creating an index doesn't add to those budgets. With the defaults, 512 MB / 4 gives each DuckDB instance 128 MB. Reader and DuckDB thread settings apply per index, so each index adds threads and connections. Set `max_indexes = 1` for a single-index deployment, or raise the total budgets when adding slots. DuckDB's memory limit doesn't cap the process's RSS.

An authenticated `POST /indexes/payments/logs/ingest` creates the index if it's missing; `PUT /indexes/payments` creates it explicitly and is idempotent. `GET /indexes` needs a read token and lists only the indexes that token can read. Reads from an unknown index return `404`. The optional `[ingest_index_scopes]` and `[read_index_scopes]` tables map an existing token to the index names it may use; a missing entry or an empty list allows every index. Source scopes still apply. The unprefixed `/logs` endpoints use `default` and check its index scope. `/metrics` needs a read token with access to every source and index. Storage metrics have an `index` label; HTTP limits and histograms are global.

DuckDB query progress is exported as `logbrook_storage_query_progress_available`, `_fraction` (0 to 1), `_rows_processed` and `_rows_total`, with a `role` label (`reader` or `maintenance`) and a `worker` label per connection. When a connection is idle or DuckDB can't estimate a query, `_available` is 0 and the other gauges are NaN. The estimates don't affect cancellation or deadlines.

`logbrook_ingested_events_total` counts events committed across all indexes since the process started. Deleting an index or expiring events doesn't lower it, and a restart resets it to zero without recounting stored events.

Policy changes apply on restart. Maintenance runs once at startup and then at the configured interval, so a shorter retention can expire existing events on that first pass. The stored expiry cutoff only moves forward: raising retention never brings back expired events, and live ingestion keeps rejecting timestamps before the cutoff. Queries use the committed cutoff, exported as `logbrook_storage_retention_before_ms`, rather than the wall clock.

If maintenance fails in a way it can recover from, reads and ingestion keep working and maintenance retries with exponential backoff, up to 15 minutes between attempts. Integrity failures and broken database connections mark the server unready and shut it down. A failed file deletion after a committed expiry is retried; the expiry stands. On shutdown, Logbrook cancels maintenance, drains the readers and checkpoints the writer. Watch free disk space, archive growth, database, WAL and temp size, pending deletions, queue occupancy, latency and failures through `/metrics` and your container monitoring. `/ready` doesn't warn about low disk space.

Each request commits in its own transaction, and Logbrook doesn't combine requests, so send events in batches for throughput.

`logbrook healthcheck` checks `/ready` without credentials and without opening storage. It reads the bind address from `--config` if given, then `LOGBROOK_BIND`. The container listens on port 3100; if you change the port, set `LOGBROOK_BIND` to match.

## Live tail

Tail follows ingestion order, not event time, so late events with old timestamps still show up. A new stream starts after the newest committed event; send `Last-Event-ID` to resume from an earlier one. Field filters run on the server. A backlog is sent in pages until the stream catches up; a large backlog alone doesn't cause a gap. A slow client slows only its own stream. If retention expires events a stream hasn't sent yet, the stream ends with a `gap` event; a stored event too large to send produces an explicit error. The expiry check covers all sources, so a reader scoped to one source can get a gap when another source's events expire.

## Back up, restore, and upgrade

1. Stop the service and wait for it to exit cleanly. Don't copy a live data directory with a plain recursive copy.
2. Copy the **entire** data directory, including any WAL and all archive files, as one backup.
3. Restore into a different empty directory, keep the files owned by the service user, and start one process on it.
4. Check event counts and a few complete events before switching producers.

For a container volume, stop the service and copy the whole volume to a separate backup location with a temporary utility container. A backup needs both the DuckDB files and the Parquet archives; neither holds all retained data alone.

Startup checks schema versions and runs migrations in a transaction. Indexes without promoted string attributes stay on schema version 3; turning them on upgrades that index to version 4. Older archive metadata is migrated before the server starts serving. Startup rejects schema versions newer than the binary supports, and a binary that only knows version 3 refuses a version 4 index. There are no downgrade migrations: upgrade a copy first, and to roll back, restore the pre-upgrade backup together with the previous binary.

## Promoted string attributes

For top-level string attributes you filter on often, configure a small set of physical columns:

```toml
[storage]
promoted_string_keys = ["tenantId", "userId", "requestId"]
```

`indexes.<name>.promoted_string_keys` overrides the global list for one index. You can promote up to eight distinct keys, each 1–256 UTF-8 bytes. Only string equality filters on those keys get faster; numeric and boolean filters and other keys use the JSON path as before. Query results, live tail, authorization and cursors behave the same.

Existing hot rows aren't covered and keep using JSON filtering; new rows get the columns. Export and compaction backfill older data over time. Archive files above the compaction byte limit, and rows with malformed JSON, stay on the JSON path. Missing, null and non-string values are stored as NULL, distinct from uncovered rows.

An index is activated the first time it opens with a non-empty key set, and from then on its keys are fixed. The same keys in a different order are accepted. Omitting the setting or setting `[]` keeps the recorded keys rather than removing them, and a different non-empty set is rejected at startup. To change the keys, create a new index and reimport. An empty per-index override stops inheritance of the global list only for an index that isn't activated yet.

Promoted archives carry SHA-256 digests, checked at startup and around query and compaction reads. Never modify archive files in place: a mismatch fails the operation and can mark the index unready. The digests catch accidental changes, not someone who can also rewrite the database and its manifest. Hashing adds some CPU cost to writes and archive reads.

Stop the server before inspecting an index offline:

```sh
logbrook --config logbrook.local.toml --index payments projection-status
```

The JSON output shows the schema version, the recorded keys, hot and archived row counts, how many of each are covered, and the number of archive files written before schema version 4. With a non-empty key set configured, the command activates an index that isn't activated yet, so take an offline backup first. It verifies the archives it reads, uses the normal query deadline, and has no HTTP equivalent.

## Migrate an older flat Logbrook data directory

Logbrook refuses to start on the flat `<data_dir>/events.duckdb` layout rather than create an empty `default` index next to it. Stop the old process and take a complete offline backup. Create `<data_dir>/indexes/default/` and move `events.duckdb`, `events.duckdb.wal` if present, `archives/` and `temp/` into it together. Don't change the archive paths stored in the database. Restart with the data root still set to `<data_dir>` and check event counts. To roll back, restore the backup together with the previous binary.

## Import Pino events

Stop the destination service before importing. Use the same config file or environment variables as the server. A JSON array must fit in one request; NDJSON is streamed in bounded batches.

```sh
target/release/logbrook --config logbrook.local.toml --index payments import events.json --source checkout
target/release/logbrook --config logbrook.local.toml --index payments import events.ndjson --source checkout --ndjson
```

Offline import and archive commands take `--index <name>` (default `default`). Each batch commits on its own. If a later batch is invalid or the process stops, earlier batches stay, and rerunning the whole import duplicates them. For a clean retry, import into a new data directory. Imports keep each event's time, so set retention to cover the imported history before starting the server.

## Archive events offline

Stop the service first. To move hot events older than a cutoff into Parquet archives:

```sh
target/release/logbrook --config logbrook.local.toml archive --before 1791321600000
```

`--before` is an exclusive Unix-millisecond cutoff. Archiving doesn't delete anything; archived events stay searchable.
