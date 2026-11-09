# Query logs and derive insights

## Define the investigation

Choose an existing index, a time window and a read credential, listing the indexes first if needed. Use explicit UTC bounds so comparisons can be repeated, converted to Unix milliseconds: `from` is inclusive and `to` exclusive. One request spans at most 31 days by default, even when retention is longer, so split longer investigations into adjacent windows.

Searches and aggregates cover recent DuckDB records and retained Parquet archives together. A result covers what the credential may see and retention still holds, which may be less than everything the application logged.

## CLI

The running container already contains the CLI. With the bundled Compose deployment:

```sh
docker compose exec -T logbrook logbrook indexes list
docker compose exec -T logbrook logbrook indexes show apps --json
docker compose exec -T logbrook logbrook query apps --since 1h --service checkout --min-level 50 --limit 50 --json
docker compose exec -T logbrook logbrook count apps --since 1h --service checkout --min-level 50
```

For plain Docker, replace `docker compose exec -T logbrook` with `docker exec logbrook`. With a local `logbrook` executable, export `LOGBROOK_URL` and `LOGBROOK_READ_TOKEN` and run it directly. `exec` inside the container uses the server's own credentials; for an investigation limited to certain sources, use that scoped reader instead of quietly widening access with the server's unrestricted token.

`--token` overrides `LOGBROOK_TOKEN`, which overrides the role-specific variables, so an admin or ingestion token left in `LOGBROOK_TOKEN` breaks queries. Remote CLI commands talk HTTP and reject the server's `--config`.

`query` fetches one page; `count` counts every match in the retained window. Use `--from` and `--to` for comparisons and pagination. `--cursor` needs both bounds; reuse the same index, filters, bounds and page size. The server rejects a cursor used with a different selection, and any cursor it rejects means starting a new search.

## HTTP helpers and supported filters

Use `curl` and `jq` for facets, histograms and exports, or when there is no local CLI. Set the URL, index and read credential first, and fix the window once:

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

HTTP has no `since` parameter. The filters are `source` (a comma-separated list, matched as OR), `service`, `logger` and `host` (exact, case-sensitive values), `message` (a literal, case-sensitive substring) and `min_level` (inclusive minimum severity). Attribute equality goes in `filter` as URL-encoded JSON, with at most 16 conditions, 256-byte keys, 1,024-byte strings and 4,096 bytes of decoded JSON; a smaller configured field limit also applies to keys and strings. Filters combine with AND. Only search accepts `limit` and `cursor`, and only histogram accepts `interval_ms`. Unknown parameters return `400`.

## Find errors and affected services

```sh
logs_get '' --data-urlencode 'min_level=50' --data-urlencode 'limit=50' | jq .
logs_get /count --data-urlencode 'min_level=50' | jq .
logs_get /facets --data-urlencode 'min_level=50' | jq '{services, hosts, loggers, levels}'
```

Each facet array holds `value` and `count` pairs for the 100 most frequent non-null values of that field, so it isn't a full group-by. Events missing the field don't appear. Pino's error and fatal levels are 50 and 60, and `min_level=50` includes both.

## Measure error-log share

Use full-window counts with identical scope and bounds:

```sh
total=$(logs_get /count --data-urlencode 'service=checkout' | jq -er '.count')
errors=$(logs_get /count --data-urlencode 'service=checkout' --data-urlencode 'min_level=50' | jq -er '.count')
jq -n --argjson total "$total" --argjson errors "$errors" \
  '{total: $total, errors: $errors, error_log_percent: (if $total == 0 then null else 100 * $errors / $total end)}'
```

This is the share of **log events** at error level or above. It is a failed-request rate only if the application logs exactly one representative event per request. The two counts are separate snapshots, so concurrent writes, late arrivals or retention can skew the ratio. Report a zero total as unavailable, not as 0% success or failure.

## Locate spikes and compare periods

```sh
logs_get /histogram --data-urlencode 'service=checkout' \
  --data-urlencode 'min_level=50' --data-urlencode 'interval_ms=60000' | jq .
```

Buckets start at the requested `from`, and only nonempty ones are returned. Fill the gaps with zeros when plotting; the last bucket can be shorter. The window may span at most 2,000 buckets. Compare windows of equal length with the same filters, or divide counts by duration when lengths differ. Query a spike with its exact bounds and read representative events before suggesting a cause.

## Inspect attributes and export every page

For exact top-level lookups, use `--filter '{"attributes":[{"key":"requestId","op":"eq","value":"req-123"}]}'` or the same JSON in HTTP `filter`. Conditions combine with AND on search, count, facets, histogram and tail. Keys and strings match exactly and case-sensitively, and a number never matches a string. Nested attributes, ranges and aggregation by attribute aren't supported. For those, narrow the time and service window, export every page and analyze the file locally. This writes to a new temporary file and stops on the first failed request, so a partial export is never mistaken for a complete one:

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

Treat the file as complete only when the loop succeeds. Keep bounds and filters unchanged between pages, and don't use tail IDs as search cursors. Later pages leave out events ingested after the first page, and retention can still delete history or expire the cursor mid-export. To look up a field:

```sh
jq -c 'select(.attributes.requestId == "req-123")' "$LOGBROOK_EXPORT"
```

For latency, select numeric `attributes.durationMs` for the operation in question, state its unit, sample count, number of events missing the field and time range, and compute percentiles locally with a named method. The API's histograms count events; they don't compute latency percentiles. Treat a partial export as a sample and don't extrapolate totals from it.

## Follow new errors

Tail is a streaming request with no end, so don't use the bounded `logs_get` helper:

```sh
curl --fail-with-body --silent --show-error --no-buffer --get \
  "${LOGBROOK_URL%/}/indexes/$LOGBROOK_INDEX/logs/tail" \
  -H "Authorization: Bearer $LOGBROOK_READ_TOKEN" \
  --data-urlencode 'service=checkout' --data-urlencode 'min_level=50'
```

Without `Last-Event-ID`, tail starts at the latest committed event, not at the start of stored history. Save the exact SSE `id` from `log` and `cursor` frames and reconnect with it as `Last-Event-ID`. A `gap` frame means some events can't be replayed; fill the hole from retained history with a search, because retrying the same ID won't recover expired data. Tail accepts field and attribute filters, but not time bounds, page limits or search cursors. Resuming with different filters, or with a plain numeric ID, returns `400`. Records from Forward keep application fields under `attributes.application`, which top-level equality filters can't reach.

## Explain the result

Report the index, the explicit window and timezone, the filters, the counts and a few relevant event IDs and timestamps. Separate evidence (timeout logs concentrated on one host) from hypotheses (that host caused the incident). Mention source scope, the 100-value facet cutoff, incomplete pagination, missing fields, dropped or duplicated deliveries and expired history whenever they affect the conclusion. An empty result means no matching events are retained and visible to the credential, not that nothing happened.
