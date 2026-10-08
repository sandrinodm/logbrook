# Logbrook CLI

The `logbrook` executable includes the server, offline maintenance commands, and an HTTP client for the running server. Remote commands never open the local data directory or load server configuration. `--config` belongs to server/offline commands; remote commands reject it. This makes the client usable from another machine without access to database files.

## Connect and authenticate

```sh
export LOGBROOK_URL=https://logs.example.com
export LOGBROOK_READ_TOKEN='<your read credential>'
export LOGBROOK_ADMIN_TOKEN='<your separate management credential>'
logbrook indexes list
```

The URL defaults to `http://127.0.0.1:3100`. Both HTTP and HTTPS are supported; HTTPS validates the certificate and hostname. A base URL path is supported for a reverse proxy mounted below a prefix. Redirects are rejected, so a redirected request cannot forward credentials to a different endpoint. The client rejects URL credentials, query strings, and fragments.

Configure the admin credential on the server using `LOGBROOK_ADMIN_TOKEN` or top-level TOML `admin_tokens = ["<random credential>"]`, then restart. Compose forwards the optional environment variable when present. Admin credentials must differ from read and ingestion credentials; existing read/ingestion tokens acquire no deletion authority. Without an admin token, deletion is unavailable.

`--token` overrides `LOGBROOK_TOKEN`, which overrides the role-specific variables below. Prefer environment variables so credentials do not appear in shell history or process arguments. The CLI never writes credentials to disk.

| Command | Role-specific credential selection |
| --- | --- |
| `indexes list`, `indexes show` | `LOGBROOK_ADMIN_TOKEN`, then `LOGBROOK_READ_TOKEN` |
| `indexes create` | `LOGBROOK_ADMIN_TOKEN`, then `LOGBROOK_INGEST_TOKEN` |
| `indexes delete` | `LOGBROOK_ADMIN_TOKEN` |
| `query`, `count` | `LOGBROOK_READ_TOKEN` |

Admin credentials authorize index management only. They cannot read log events, ingest events, or access global metrics. Read and ingestion credentials retain their configured index/source scopes.

## Indexes and sizes

```sh
logbrook indexes create checkout
logbrook indexes list
logbrook indexes show checkout
logbrook indexes show checkout --json
```

Create is idempotent and uses the server's effective policy. It respects `max_indexes` and does not change retention configuration. List/show display measured size, configured size target, retention, and readiness. JSON includes exact `size_bytes`, `database_bytes`, `wal_bytes`, `archive_bytes`, `temp_bytes`, and `other_bytes`, plus `max_size_bytes` (null when unlimited), `retention_ms`, and `ready`.

Sizes are fresh sums of physical file lengths, including unpublished/unknown files; they are not event JSON sizes or filesystem allocated blocks. Measurements can change during concurrent ingestion and maintenance. Readers limited to particular sources receive index names only, so sizes cannot reveal other sources' data volume. Human output displays unavailable fields as `-`.

## Search, count, and pagination

For a runnable demo, expected results, and detailed CLI/HTTP recipes, see the [query cookbook](QUERY-EXAMPLES.md).

```sh
logbrook query checkout --since 30m --service api --min-level 40 --limit 50
logbrook query checkout --since 1d --message 'request failed' --json
logbrook count checkout --since 7d --source production
```

Query/count default to the last hour. `--since` accepts a positive integer followed by `ms`, `s`, `m`, `h`, `d`, or `w`. Alternatively, supply both `--from` and `--to` as Unix milliseconds; the range is inclusive at the start and exclusive at the end. Server retention and maximum-range policies still apply.

Filters: `--source` (comma-separated), `--service`, `--logger`, `--host`, `--message` (case-sensitive substring), and `--min-level` (numeric severity). Search additionally accepts `--limit` and `--cursor`; count returns the number of matching retained events. These commands use structured HTTP filters, not arbitrary SQL.

Query displays a table with the effective time range and any next-page cursor. With `--json`, it returns the server object containing `events` and `next_cursor`, including complete event attributes. It fetches one page and does not silently scan every result. For pagination, use the same explicit bounds and filters on every request:

```sh
logbrook query checkout --from 1791334800000 --to 1791338400000 --limit 100 --json
logbrook query checkout --from 1791334800000 --to 1791338400000 --limit 100 --cursor '<next_cursor>' --json
```

`--cursor` requires explicit `--from` and `--to`; recalculating a relative range would change the search. Named-route cursors belong to their index and may expire when retention removes data. Reusing the same bounds and filters is the client's responsibility; the cursor does not encode the full filter set.

## Delete an index

```sh
logbrook indexes delete checkout --yes
```

This permanently removes the index's events, database, WAL, archives, and temporary files. The command requires `--yes` before it sends a request, as well as server-side admin authentication. The mandatory `default` index cannot be deleted. An index declared in TOML or `LOGBROOK_INDEXES` is protected: first remove that declaration and restart, then delete the discovered index.

Deletion stops admission for the selected index, drains its native workers, and moves its directory into the reserved `<data_dir>/.index-trash/` area before cleanup. An admitted deletion continues if the client disconnects. Other indexes remain usable. Startup retries cleanup of recognized deletion remnants; keep unrelated files out of this reserved directory.

Stop producers before permanent removal: later authorized ingestion may recreate the same name with a fresh empty index. A deleted name also becomes available for explicit creation. If a request times out or returns a filesystem error, inspect the index state before retrying; the operation may already have removed the index while cleanup remains pending.

## Output and deadlines

List/show/query use a human-readable table by default; count prints a number. `--json` emits the complete server response as JSON for every remote command. Command results use stdout and errors use stderr with nonzero exit status. A closed output pipe is handled gracefully.

The default remote deadline is 30 seconds. Set `--timeout 60` for a slower server (supported range 1–300 seconds). Responses are capped at 16 MiB, including streamed/chunked bodies. Reduce the page size if a query exceeds this client limit. Requests are not automatically retried.

## Containers

The production image contains the same executable. Run management commands against the running Compose service without opening its database directly:

```sh
docker compose exec logbrook logbrook indexes list
docker compose exec logbrook logbrook query checkout --since 1h
docker compose exec logbrook logbrook indexes delete checkout --yes
```

These commands inherit the container's configured role credentials. They use HTTP through loopback; no shell or extra runtime tools are required. Existing `serve`, `check-config`, `healthcheck`, `import`, and `archive` commands retain their server/offline behavior.
