# Operations and migration

## Storage ownership and durability

One process owns the data root. Each index has its own `indexes/<name>/events.duckdb`, WAL, `archives/` and `temp/` directories. Index names contain 1–63 lowercase ASCII letters, digits, hyphens or underscores, starting with a letter or digit. Managed index directories and files cannot be symlinks. Ingest batches validate atomically and return success after appender flush and transaction commit. A timeout or disconnected client can have an ambiguous result: retrying may duplicate events. Logbrook does not implement producer idempotency keys.

Use the [HTTP CLI](CLI.md) to list index sizes, create indexes, query events, or delete dynamic indexes while the server runs. Configure a separate optional `LOGBROOK_ADMIN_TOKEN` (or TOML `admin_tokens`) for deletion. The default index and indexes declared in server configuration are protected. Deletion drains only the selected index, moves its directory into reserved `.index-trash/` storage, and completes cleanup independently of the client connection. Failed cleanup is retried on startup or a later deletion; this directory is not a recoverable backup. Stop producers first when removing an index permanently because ingestion can recreate a missing index.

The database allocates persistent IDs in the same transaction as events. Readers use separate connections and a transaction containing both hot data and the archive manifest. Short snapshot-registration and manifest-publication critical sections establish generation leases. A long reader keeps its files alive without holding the writer lock; newer readers do not indefinitely pin retired files. Maintenance reclaims eligible files outside the publication lock. Export, compaction and expiry discovery run on the maintenance connection; short manifest/deletion transactions use the writer.

Archive files use relative manifest paths so a complete data directory can be relocated. Temporary or unreferenced `.tmp`/`.parquet` files in the dedicated archive directory are removed at startup; failed unpublished exports are also cleaned during operation. Missing committed files, unexpected byte lengths, or unsupported archive schemas are integrity failures. Data/import paths containing DuckDB glob characters are rejected. Keep unrelated files out of the archive directory. Bundled JSON/Parquet support is included; automatic extension installation and loading are disabled.

The native tests and SIGKILL test exercise application errors and process crashes. They do not establish power-loss guarantees for the underlying disk, filesystem, or virtual machine.

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

`storage.maintenance_timeout_ms` applies from the start of each maintenance job, including waits for writer capacity and snapshot registration, and native export, retention, publication, and checkpoint work. Cancellation is cooperative. Transaction commit and archive reconciliation finish without interruption to preserve durability. A timeout at that boundary can arrive before the final outcome is known, and the writer remains occupied until finalization completes.

Raw body permits are acquired incrementally without waiting while holding partial reservations. A bounded RawValue visitor checks event counts and wire sizes, then a structural preflight reserves a conservative JSON-node/string allocation allowance before constructing decoded values. The decoded permit follows the admitted batch through writer completion, including client disconnects. A stalled chunked request does not reserve the entire decoded budget. This is bounded conservative accounting, not an exact allocator or total-RSS cap; native database memory and process overhead require additional headroom.

`400` indicates invalid input; `413` indicates request/response size limits; `507` indicates the selected index has reached its disk target; `404` indicates an unknown index; `401`/`403` indicate authentication/scope errors; `429` includes `Retry-After: 1` for capacity pressure; `503` indicates unavailable storage/shutdown; `504` indicates a query deadline. A query's worker remains occupied until the native operation finishes or interruption completes. Body reception has a 5-second idle and 30-second total deadline while holding its incremental raw-byte permits.

Retention is by event time. HTTP ingestion validates against the configured live window and atomically rechecks the persisted cutoff at commit, so enlarging configured retention cannot admit permanently invisible events. The default future skew allowance is five minutes. Offline imports permit historical timestamps; configure retention before serving imported data. Expired rows in mixed-age files become invisible immediately; physical removal still depends on file expiry. Bounded compaction consolidates small files in the same event-time partition. Physical size retention also expires oldest data, subject to the file-allocation behavior below. Partial-expiry archive rewrites are not implemented.

Configure inherited defaults at the top level and per-index overrides in `[indexes.<name>]`:

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

Values must be positive integers; retention must exceed archive age. One-day retention therefore needs a shorter archive age, such as `archive_after_ms = 3600000`. Byte targets must be at least 16 MiB. Day and GB convenience keys take precedence over their canonical global TOML values; supplying both units in an index override or both corresponding environment variables is rejected. An environment value overrides the inherited TOML default regardless of unit. Explicit per-index policies override these inherited defaults. Compose forwards days/GB (7 days/20 GB by default); use its days setting instead of the older milliseconds setting.

Size is a **per-index measured-footprint retention target**, not a filesystem reservation or exact disk ceiling. It includes database, WAL, archives (including files pinned by readers), spill and other owned files. Maintenance retires oldest event-time groups and waits for active reader leases before reclaiming their files. It attempts bounded hot-row expiry/checkpointing when necessary. DuckDB can reuse freed blocks without shrinking its database file; Logbrook stops repeated destructive hot-row eviction if it makes no physical progress. `VACUUM` is not a file-shrink operation; see [DuckDB’s space-reclamation documentation](https://github.com/duckdb/duckdb-web/blob/main/docs/current/operations_manual/footprint_of_duckdb/reclaiming_space.md). If storage remains at the target, ingestion returns `507` until bytes are reclaimed or the configured target is raised and the service restarted. Monitor physical disk headroom independently: a transaction, export, compaction or query spill can temporarily exceed the target. A filesystem quota is required for an exact hard ceiling. Increasing limits never resurrects expired data.

HTTP request, body, decoded-memory, connection and tail budgets are shared across all indexes. `storage.memory_limit`, `storage.temp_limit` and `storage.queue_bytes` are divided by `max_indexes`, including unused slots, so creating an index does not multiply those configured budgets. Default 512 MB / 4 yields 128 MB per DuckDB instance. Reader and execution thread settings are per index; more indexes increase native thread and connection overhead. Set `max_indexes = 1` for a single-index deployment or increase total budgets when adding slots. DuckDB memory limits do not cap total process RSS.

An authenticated `POST /indexes/payments/logs/ingest` creates a missing index; `PUT /indexes/payments` explicitly creates it and is idempotent. `GET /indexes` requires a reader and lists only indexes it may access. Unknown-index reads return `404`. Optional `[ingest_index_scopes]` and `[read_index_scopes]` maps associate an existing token with allowed index names; a missing entry or empty list grants all indexes. Existing source scopes still apply. The unprefixed `/logs` endpoints target `default` and enforce its index scope. Global metrics require unrestricted source and index access. Storage metric series carry an `index` label; HTTP request limits and histograms remain global.

`logbrook_ingested_events_total` counts events committed across all indexes since this server process started. Deleting an index or expiring its events does not decrease the counter. Restarting the server resets it to zero; existing stored events are not counted again.

Changes take effect on restart, with maintenance starting immediately and then repeating at the configured interval. A shorter retention can expire existing events on that first pass. The persisted expiry cutoff only advances: increasing retention never resurrects expired data, and live ingestion still rejects timestamps earlier than that cutoff. Queries use the committed cutoff, which is exposed as `logbrook_storage_retention_before_ms`; they do not promise physical deletion at the exact wall-clock expiry millisecond. A file containing both retained and expired events remains until fully expired. Archival itself preserves searchability and is independent of deletion.

`cargo test --package logbrook --test process retention_override_reclaims_archives_and_persists_monotonic_cutoff --locked` verifies TOML/environment precedence, restart behavior, archived-event expiry, physical removal of fully expired files, recent-event survival and the monotonic cutoff using disposable data.

Recoverable maintenance failures keep reads and ingestion available and retry with bounded exponential backoff, up to fifteen minutes. Fatal integrity/invalidated-connection failures stop readiness and trigger supervised shutdown. Post-commit unlink failures are retried; they do not undo committed retention. Shutdown cancels maintenance, drains/stops readers, then checkpoints the writer. Monitor filesystem space, archive growth, database/WAL/temp size, deferred unlinks, queue occupancy, latency and failures through protected metrics and container monitoring. `/ready` is not a predictive free-disk monitor.

Transactions currently commit one request at a time. Producers should send bounded batches for throughput. There is no across-request batching delay or coalescing yet. Performance tuning must measure mixed ingestion, search, and archive work together.

`logbrook healthcheck` probes `/ready` without requiring credentials or opening storage. It reads the bind address from optional `--config` and then `LOGBROOK_BIND`. Container defaults use port 3100; override `LOGBROOK_BIND` consistently when changing the container's port.

## Live tail

Tail uses persistent ingestion IDs, so retained late events are included even when their event time is old. A new stream starts at the current ingestion high-water mark. Supply `Last-Event-ID` to replay newer retained events. Field filters are applied on the server. Results are paged using actual encoded byte cost and drained immediately while data remains; a 1,000-event backlog is not itself a gap. Commit notifications wake caught-up streams, with a fallback poll for retention/shutdown. Downstream demand supplies backpressure and each subscriber holds a bounded page. Retention expiry sends a terminal `gap`; an oversized stored event can produce an explicit size/error response. The retention watermark is deliberately conservative across sources, so a scoped reader can receive a gap after another source expires. A gap never promises that expired data remains recoverable.

## Back up, restore, and upgrade

1. Stop the service and wait for clean exit. Do not copy a live data directory with a plain recursive copy.
2. Copy the **entire** data directory, including any WAL and all manifest-referenced archive files, as one consistent backup.
3. Restore into a different empty directory, keep its files owned by the service UID, and start one process pointing at it.
4. Verify event counts and representative complete events before switching producers.

For a container volume, stop the service and use a temporary utility container to copy the entire volume to a separate backup destination. Backups must include both DuckDB and Parquet; neither alone contains the complete retained dataset.

Schema versions are checked at startup and migrations run transactionally. The current schema is version 3: it adds archive ID bounds, byte sizes and schema versions to version 2's retention watermark. Older archive metadata is backfilled from immutable files before serving. Explicit canonical projections and name-based archive mapping preserve missing nullable columns. Newer schemas are rejected. Upgrade on a copy first; for rollback, restore the pre-upgrade data copy and prior binary together. No downgrade migration is provided.

## Migrate an older flat Logbrook data directory

Startup refuses the old `<data_dir>/events.duckdb` layout instead of silently creating an empty default index. Stop the old process and make a complete offline backup first. Create `<data_dir>/indexes/default/` and move `events.duckdb`, `events.duckdb.wal` if present, `archives/`, and `temp/` together into it. Do not modify relative archive paths inside the database. Restart with the data root still pointing to `<data_dir>`, then verify event counts. A rollback requires restoring the backup and prior binary together.

## Import Pino events

Stop the destination service before running import commands. Use the same config file or supported environment variables as the server. JSON arrays must fit one request; NDJSON is streamed in bounded batches.

```sh
target/release/logbrook --config logbrook.local.toml --index payments import events.json --source checkout
target/release/logbrook --config logbrook.local.toml --index payments import events.ndjson --source checkout --ndjson
```

Offline import and archive commands accept `--index <name>` (default `default`) and use the same named folder layout. Each batch commits independently. If a later batch is invalid or the process stops, prior batches remain. Restarting the whole import may duplicate those events. Use a new destination directory for a clean retry. Imports preserve event time, so configure retention to cover the imported history before starting the server.

## Import a legacy DuckDB dataset

`import-legacy` reads a DuckDB database containing a `logs` table and, optionally, a directory of Parquet files with the same columns. It is a schema-specific importer, not an arbitrary SQL migration tool. The expected columns are:

```sql
CREATE TABLE logs (
    timestamp TIMESTAMP,
    level INTEGER,
    service VARCHAR,
    name VARCHAR,
    hostname VARCHAR,
    pid BIGINT,
    msg VARCHAR,
    metadata JSON
);
```

`timestamp`, `level`, and `msg` must contain valid, non-null values for every imported event. Timestamps represent UTC instants and are converted to Unix milliseconds. `metadata` must be a JSON object or null. The other fields may be null, and additional table columns are ignored. The ordinary event-size, field-size, nesting, and severity validation rules still apply.

Stop the source writer and obtain a consistent copy of the database, its WAL if present, and any archives. Import the copy into a new, offline Logbrook directory:

```sh
target/release/logbrook --config logbrook.local.toml import-legacy \
  /absolute/path/to/offline-copy/logs.db \
  --archives /absolute/path/to/offline-copy/partitions \
  --source legacy
```

The importer opens the source database read-only and reads regular Parquet files recursively. Paths containing symlinks or glob characters are rejected; on macOS prefer canonical `/private/tmp/...` over the `/tmp` symlink. Core columns become canonical event fields and override colliding values in metadata. Other metadata fields are preserved as attributes. Source identity comes from `--source`, and Logbrook assigns new event IDs. Unique temporary import directories are removed on success and failure.

If an event exists in both the database and an archive, both copies are imported. The importer does not deduplicate or modify source files. Each batch commits independently, so retrying after a partial failure can also create duplicates. Compare totals by day/service and sample full records before switching producers to the new destination. The separate legacy reader has its own configured native memory budget, so an offline import needs more headroom than ordinary ingestion.

To force an offline archive round trip for verification:

```sh
target/release/logbrook --config logbrook.local.toml archive --before 1791321600000
```

`before` is an exclusive Unix-millisecond cutoff. This command does not delete retained history; it moves matching hot events into the searchable archive.
