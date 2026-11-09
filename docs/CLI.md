# Logbrook CLI

The `logbrook` executable contains the server, offline maintenance commands, and an HTTP client for a running server. The client commands (`indexes`, `query` and `count`) talk to the server over HTTP and never open the data directory or read server configuration, so you can run them from another machine. They reject `--config`, which only applies to server and offline commands.

## Connect and authenticate

```sh
export LOGBROOK_URL=https://logs.example.com
export LOGBROOK_READ_TOKEN='<your read credential>'
export LOGBROOK_ADMIN_TOKEN='<your separate management credential>'
logbrook indexes list
```

The server URL comes from `--url`, then `LOGBROOK_URL`, and defaults to `http://127.0.0.1:3100`. HTTPS URLs are checked against the certificate and hostname. The URL can include a path, for a server mounted below a prefix behind a reverse proxy, but not credentials, a query string or a fragment. The client does not follow redirects, so a redirect cannot send your token to another host.

To enable index deletion, configure an admin token on the server with `LOGBROOK_ADMIN_TOKEN` or the top-level TOML setting `admin_tokens = ["<random credential>"]`, then restart. Compose passes `LOGBROOK_ADMIN_TOKEN` through when it is set. The admin token must differ from every read and ingestion token, and read and ingestion tokens can never delete. Without an admin token, nothing can delete an index.

`--token` takes precedence over `LOGBROOK_TOKEN`, which takes precedence over the role-specific variables below. Prefer the environment variables, which keep tokens out of shell history and process listings. The CLI never writes credentials to disk.

| Command | Role-specific credential selection |
| --- | --- |
| `indexes list`, `indexes show` | `LOGBROOK_ADMIN_TOKEN`, then `LOGBROOK_READ_TOKEN` |
| `indexes create` | `LOGBROOK_ADMIN_TOKEN`, then `LOGBROOK_INGEST_TOKEN` |
| `indexes delete` | `LOGBROOK_ADMIN_TOKEN` |
| `query`, `count` | `LOGBROOK_READ_TOKEN` |

An admin token manages indexes and nothing else: it cannot read or ingest events or scrape metrics. Read and ingestion tokens keep their configured index and source restrictions.

## Indexes and sizes

```sh
logbrook indexes create checkout
logbrook indexes list
logbrook indexes show checkout
logbrook indexes show checkout --json
```

`create` is idempotent. The new index gets the server's configured policy, counts toward `max_indexes`, and cannot change retention. `list` and `show` print each index's size, size limit, retention and readiness; `show` adds a size breakdown. With `--json` you get the exact `size_bytes`, `database_bytes`, `wal_bytes`, `archive_bytes`, `temp_bytes` and `other_bytes`, plus `max_size_bytes` (null when there is no limit), `retention_ms` and `ready`.

Sizes are the summed lengths of the index's files, measured on each request, so they change while the index is written. They are not the size of the event JSON or the disk blocks allocated. A read token restricted to particular sources sees index names only, so it cannot learn how much data other sources store. The table shows a missing value as `-`.

## Search, count, and pagination

The [query cookbook](QUERY-EXAMPLES.md) has a runnable demo with expected results and more CLI and HTTP recipes.

```sh
logbrook query checkout --since 30m --service api --min-level 40 --limit 50
logbrook query checkout --since 1d --message 'request failed' --json
logbrook count checkout --since 7d --source production
```

`query` and `count` cover the last hour by default. `--since` takes a positive integer with an `ms`, `s`, `m`, `h`, `d` or `w` suffix. Instead of `--since`, you can pass both `--from` and `--to` as Unix milliseconds; `from` is inclusive and `to` is exclusive. The server's retention and maximum range still apply.

| Filter | Matches |
| --- | --- |
| `--source` | One source or a comma-separated list |
| `--service`, `--logger`, `--host` | Exact value |
| `--message` | Case-sensitive substring |
| `--min-level` | `level` greater than or equal to this number |
| `--filter '<JSON>'` | Top-level attribute equality; see [attribute equality](API.md#attribute-equality) |

`query` also takes `--limit` (1 to 1,000, default 100) and `--cursor`. `count` prints the number of matching events. The CLI checks the `--filter` JSON before sending the request.

`query` prints a table with the time range searched, one row per event and, when there is another page, its cursor. With `--json` it prints the server response, `events` and `next_cursor`, with every event attribute. It fetches one page per run. To page through results, repeat the same explicit bounds and filters and add the cursor:

```sh
logbrook query checkout --from 1791334800000 --to 1791338400000 --limit 100 --json
logbrook query checkout --from 1791334800000 --to 1791338400000 --limit 100 --cursor '<next_cursor>' --json
```

`--cursor` requires `--from` and `--to`, because a relative `--since` window would move between runs. The cursor only works with the same index, bounds, filters, sources and `--limit`; changing any of them returns `400`. It can also expire when retention removes data.

## Delete an index

```sh
logbrook indexes delete checkout --yes
```

This permanently removes the index's events, database, WAL, archives and temporary files. Without `--yes` the command stops before sending anything, and the server also requires an admin token. The mandatory `default` index cannot be deleted. An index declared in TOML or `LOGBROOK_INDEXES` is protected: remove the declaration and restart, then delete it.

Other indexes stay available during a deletion, and once the server has accepted one it finishes even if the client disconnects. The server moves the index into `<data_dir>/.index-trash/` before removing it and finishes interrupted cleanups at startup, so keep other files out of that directory.

Stop producers first: a later ingestion with an authorized token recreates the name as a new, empty index. If the request times out or returns a filesystem error, check whether the index still exists before retrying, since the server may already have removed it and still be cleaning up.

## Offline projection status

With the server stopped, run this against its configuration to see how much of an index is covered by promoted string attributes:

```sh
logbrook --config logbrook.local.toml --index payments projection-status
```

It prints JSON with `schema_version`, `promoted_string_keys`, `hot_rows`, `hot_covered_rows`, `archived_rows`, `archived_covered_rows` and `legacy_archive_files`. The command opens the index directly instead of calling the HTTP API, with the same startup validation as the server, so a configured key set activates on an index that has none yet. See [promoted string attributes](OPERATIONS.md#promoted-string-attributes) for activation, backups and downgrades.

## Output and deadlines

`indexes list`, `indexes show` and `query` print a table by default, and `count` prints a number. `--json` prints the full server response for every client command. Results go to stdout; errors go to stderr with a nonzero exit status. Closing the output pipe early (for example with `head`) is not an error.

Requests time out after 30 seconds by default; set `--timeout 60` for a slower server (1 to 300 seconds). The client refuses responses over 16 MiB, so lower `--limit` if a page is larger. Failed requests are not retried.

## Containers

The production image contains the same executable. Run client commands inside the running Compose service; they reach the server over loopback and do not open its database:

```sh
docker compose exec logbrook logbrook indexes list
docker compose exec logbrook logbrook query checkout --since 1h
docker compose exec logbrook logbrook indexes delete checkout --yes
```

These commands use the tokens set in the container's environment and need no shell or other tools in the image. The `serve`, `check-config`, `healthcheck`, `import`, `archive` and `projection-status` commands are server and offline commands; see [operations](OPERATIONS.md).
