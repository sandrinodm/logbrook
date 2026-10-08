# Query logs and derive insights

## Define the investigation

Choose an existing index, time window, and read credential. List indexes if needed. Use explicit UTC bounds for reproducible comparisons. Convert to Unix milliseconds; `from` is inclusive and `to` exclusive. The default maximum span is 31 days, even when retention is longer. Split longer investigations into adjacent windows.

All searches and aggregates include both recent DuckDB records and retained Parquet archives. A result describes what the credential may see and retention still holds, not necessarily everything an application produced.

## CLI

The running container already contains the CLI. With the bundled Compose deployment:

```sh
docker compose exec -T logbrook logbrook indexes list
docker compose exec -T logbrook logbrook indexes show apps --json
docker compose exec -T logbrook logbrook query apps --since 1h --service checkout --min-level 50 --limit 50 --json
docker compose exec -T logbrook logbrook count apps --since 1h --service checkout --min-level 50
```

For Docker, replace `docker compose exec -T logbrook` with `docker exec logbrook`. If the executable is installed locally, invoke `logbrook` directly with `LOGBROOK_URL` and `LOGBROOK_READ_TOKEN` exported. Container `exec` inherits its configured credentials; for a source-scoped investigation, use the intended reader rather than silently broadening access through the server's unrestricted token.

`--token` overrides `LOGBROOK_TOKEN`, which overrides role-specific variables. An accidentally exported admin/ingest token in `LOGBROOK_TOKEN` will break queries. Remote CLI commands use HTTP and reject server `--config`.

`query` fetches one page; `count` counts the entire matching retained window. Use `--from` and `--to` for comparisons or pagination. `--cursor` requires both explicit bounds; reuse the same index and filters.

## HTTP helpers and supported filters

Use `curl` and `jq` for facets, histograms, exports, or when no local CLI is available. Set the intended URL, index, and read credential first. Capture the window once:

```sh
export LOGBROOK_URL=http://127.0.0.1:3100
export LOGBROOK_INDEX=apps
export LOGBROOK_TO_MS=$(jq -nr 'now * 1000 | floor')
export LOGBROOK_FROM_MS=$((LOGBROOK_TO_MS - 3600000))

logs_get() {
  endpoint=$1
  shift
  curl --fail-with-body --silent --show-error --get \
    "${LOGBROOK_URL%/}/indexes/$LOGBROOK_INDEX/logs$endpoint" \
    -H "Authorization: Bearer $LOGBROOK_READ_TOKEN" \
    --data-urlencode "from=$LOGBROOK_FROM_MS" \
    --data-urlencode "to=$LOGBROOK_TO_MS" "$@"
}
```

HTTP has no `since` parameter. Common filters are `source` (comma-separated OR list), `service`, `logger`, `host` (exact case-sensitive values), `message` (literal case-sensitive substring), and `min_level` (inclusive minimum severity). Filters combine with AND. Only search accepts `limit`/`cursor`; only histogram accepts `interval_ms`. Unknown parameters return `400`.

## Find errors and affected services

```sh
logs_get '' --data-urlencode 'min_level=50' --data-urlencode 'limit=50' | jq .
logs_get /count --data-urlencode 'min_level=50' | jq .
logs_get /facets --data-urlencode 'min_level=50' | jq '{services, hosts, loggers, levels}'
```

Facet arrays contain `value`/`count` pairs, up to 100 non-null values per dimension, ranked by count. They are bounded rankings, not an exhaustive group-by export. Missing fields do not appear. Pino error and fatal levels are conventionally 50 and 60; `min_level=50` includes both.

## Measure error-log share

Use full-window counts with identical scope and bounds:

```sh
total=$(logs_get /count --data-urlencode 'service=checkout' | jq -er '.count')
errors=$(logs_get /count --data-urlencode 'service=checkout' --data-urlencode 'min_level=50' | jq -er '.count')
jq -n --argjson total "$total" --argjson errors "$errors" \
  '{total: $total, errors: $errors, error_log_percent: (if $total == 0 then null else 100 * $errors / $total end)}'
```

This measures the share of **log events** at error level or above. It is not a failed-request rate unless the application's logging contract emits exactly one representative event per request. Counts use separate snapshots, so concurrent writes, late arrivals, or retention can affect comparisons. Report a zero denominator as unavailable, not 0% success or failure.

## Locate spikes and compare periods

```sh
logs_get /histogram --data-urlencode 'service=checkout' \
  --data-urlencode 'min_level=50' --data-urlencode 'interval_ms=60000' | jq .
```

Buckets are anchored to the requested `from` and contain only nonempty intervals. Fill absent buckets with zeros when plotting; the final bucket can be shorter. At most 2,000 possible buckets are allowed. Compare equal-duration windows and the same filters; normalize counts by duration when windows differ. Drill into a spike using its exact bounds and inspect representative events before suggesting a cause.

## Inspect attributes and export every page

The server does not filter arbitrary attributes such as `requestId` or aggregate `durationMs`. Narrow the time/service window, export every page, and analyze locally. This writes to a new temporary file; stop on a request failure rather than treating a partial export as complete:

```sh
export LOGBROOK_EXPORT=$(mktemp)
(
  set -eu
  set -o pipefail
  cursor=''
  while :; do
    args=(--data-urlencode 'service=checkout' --data-urlencode 'limit=500')
    if [ -n "$cursor" ]; then
      args+=(--data-urlencode "cursor=$cursor")
    fi
    page=$(logs_get '' "${args[@]}")
    printf '%s\n' "$page" | jq -e '.events | type == "array"' > /dev/null
    printf '%s\n' "$page" | jq -c '.events[]' >> "$LOGBROOK_EXPORT"
    cursor=$(printf '%s\n' "$page" | jq -r '.next_cursor // empty')
    [ -n "$cursor" ] || break
  done
)
```

Only use the file as a complete export when the loop succeeds. Keep bounds and filters unchanged; search cursors differ from tail IDs. Pages exclude events ingested after the first page, while retention can still remove history or expire the cursor. For a field lookup:

```sh
jq -c 'select(.attributes.requestId == "req-123")' "$LOGBROOK_EXPORT"
```

For latency summaries, select numeric `attributes.durationMs` from the intended operation, state its units, sample count, missing-field count, and time range, and calculate percentiles locally using a defined method. The API exposes event-count histograms, not latency percentiles. Treat partial exports as samples and avoid extrapolating them into complete totals.

## Follow new errors

Tail uses a separate unbounded streaming request; the bounded `logs_get` helper is inappropriate:

```sh
curl --fail-with-body --silent --show-error --no-buffer --get \
  "${LOGBROOK_URL%/}/indexes/$LOGBROOK_INDEX/logs/tail" \
  -H "Authorization: Bearer $LOGBROOK_READ_TOKEN" \
  --data-urlencode 'service=checkout' --data-urlencode 'min_level=50'
```

Without `Last-Event-ID`, tail begins at the current ingestion position, not the start of stored history. Save the exact SSE ID from `log` and `cursor` frames and reconnect with it as `Last-Event-ID`. A `gap` means reconcile retained history with search; retrying the same ID cannot recover expired data. Tail accepts field filters, not time bounds, page limits, or search cursors.

## Explain the result

Report the index, explicit window and timezone, filters, counts, and a few relevant event IDs/timestamps. Separate evidence (for example, timeout logs concentrated on one host) from hypotheses (for example, that host caused the incident). Note scope restrictions, top-100 facet truncation, incomplete pagination, missing fields, delivery drops/duplicates, and expired history when they affect the conclusion. Empty results mean no matching retained visible events, not proof that nothing happened.
