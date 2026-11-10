# Query cookbook

Examples for the Logbrook CLI and HTTP API, from finding one error to exporting an incident window. Search, count, facets and histograms include archived events as well as recent ones.

## 1. Connect and choose an index

The examples need Bash or Zsh, `jq`, a running server and a read token. The HTTP examples also use `curl`. Run commands with repository paths from the repository root.

```sh
export LOGBROOK_URL=http://127.0.0.1:3100
export LOGBROOK_READ_TOKEN='<your configured read token>'
export QUERY_INDEX=checkout

logbrook indexes list
logbrook indexes show "$QUERY_INDEX" --json
```

If `logbrook` is not on PATH, run the CLI from the server's container image. For a container named `logbrook`, as in the [quick start](../README.md#quick-start), define this function for the current shell. It passes your token variables into the container, so the examples' tokens apply rather than the server's:

```sh
logbrook() {
  docker exec -i -e LOGBROOK_TOKEN -e LOGBROOK_READ_TOKEN -e LOGBROOK_INGEST_TOKEN -e LOGBROOK_ADMIN_TOKEN \
    logbrook logbrook "$@"
}
```

Inside the container, the CLI reaches the server at its default `http://127.0.0.1:3100`. With Compose, use `docker compose exec` with the same flags and the service name `logbrook`. With a [source build](../README.md#build-from-source), use `logbrook() { ./target/release/logbrook "$@"; }` instead. If you already have an index with data, you can skip the demo below.

Queries use `LOGBROOK_READ_TOKEN` unless `--token` or `LOGBROOK_TOKEN` is set, so unset those if they hold a different token. Admin tokens manage indexes but cannot query events. Each request reads one index, and index names have no wildcards.

## 2. Optional demo: twelve events with known answers

The expected results below assume a fresh index. You need an ingestion token allowed to create it, a read token allowed to read it and the ingestion token's source, and a free index slot (`max_indexes` defaults to four, including `default`). Running the ingestion twice stores duplicates, so use a new index name for each run.

```sh
export LOGBROOK_INGEST_TOKEN='<your configured ingestion token>'
export QUERY_INDEX="query-demo-$(date +%s)"
export QUERY_TO_MS=$(jq -nr 'now * 1000 | floor')
export QUERY_FROM_MS=$((QUERY_TO_MS - 3600000))

jq -n --argjson until "$QUERY_TO_MS" '
  # Minutes before the upper bound, severity, service, logger, host, message, attributes.
  [
    [50, 30, "api", "http", "api-1", "GET /health 200",
     {statusCode: 200, durationMs: 8}],
    [45, 20, "api", "http", "api-1", "cache hit for user usr-7",
     {userId: "usr-7"}],
    [40, 40, "api", "http", "api-2", "upstream timeout retry=1",
     {statusCode: 504, durationMs: 1200}],
    [35, 50, "api", "http", "api-1", "payment declined order=ord-42",
     {statusCode: 402, durationMs: 210, orderId: "ord-42"}],
    [30, 50, "worker", "jobs", "worker-1", "job failed order=ord-42",
     {jobId: "job-9", orderId: "ord-42"}],
    [25, 30, "auth", "security", "auth-1", "login succeeded user=usr-7",
     {userId: "usr-7"}],
    [20, 50, "api", "http", "api-2", "upstream timeout provider=stripe",
     {statusCode: 504, durationMs: 5200}],
    [15, 60, "worker", "jobs", "worker-1", "worker stopped unexpectedly",
     {jobId: "job-9"}],
    [10, 40, "api", "http", "api-1", "rate limit 100% reached",
     {statusCode: 429, durationMs: 2}],
    [5, 30, "api", "http", "api-2", "GET /orders 200",
     {statusCode: 200, durationMs: 45}],
    [2, 50, "api", "http", "api-1", "Timeout talking to inventory",
     {statusCode: 503, durationMs: 3100}],
    [1, 30, "api", "http", "api-1", "checkout completed order=ord-43",
     {statusCode: 200, durationMs: 90, orderId: "ord-43"}]
  ]
  | to_entries
  | map(
      .key as $number
      | .value as $row
      | {
          time: ($until - $row[0] * 60000),
          level: $row[1], service: $row[2], name: $row[3],
          hostname: $row[4], msg: $row[5],
          requestId: ("req-" + (("00" + (($number + 1) | tostring))[-3:]))
        } + $row[6]
    )
' |
  curl --fail-with-body --silent --show-error \
    "${LOGBROOK_URL%/}/indexes/$QUERY_INDEX/logs/ingest" \
    -H "Authorization: Bearer $LOGBROOK_INGEST_TOKEN" \
    -H 'Content-Type: application/json' \
    --data-binary @-
```

Expected response: `{"accepted":12}`. The timestamps run from 50 minutes to one minute before `QUERY_TO_MS`, which fits the default seven-day retention. A retention shorter than 50 minutes rejects the batch.

| Fixture fields | Values |
| --- | --- |
| Services | `api` (9 events), `worker` (2), `auth` (1) |
| Loggers | `http`, `jobs`, `security` |
| Hosts | `api-1`, `api-2`, `worker-1`, `auth-1` |
| Levels | 20 (1), 30 (4), 40 (2), 50 (4), 60 (1) |
| Messages | Two contain lowercase `timeout`; one contains capitalized `Timeout` |
| Custom attributes | `requestId`, and where applicable `statusCode`, `durationMs`, `orderId`, `userId`, `jobId` |

The fixture sends Pino-style `time`, `msg`, `name` and `hostname`, which search returns as `event_time`, `message`, `logger` and `host`. Other fields go under `attributes`, so `requestId` becomes `attributes.requestId`. `source` comes from the ingestion token, not the payload, and the server assigns each event an `id` as a JSON string.

## 3. Read recent events

```sh
# Last hour, newest first; default page size is 100.
logbrook query "$QUERY_INDEX"

# Last 15 minutes, up to 20 events.
logbrook query "$QUERY_INDEX" --since 15m --limit 20

# Complete event objects, including attributes.
logbrook query "$QUERY_INDEX" --since 1h --json

# A longer retained window, including archived events if present.
logbrook query "$QUERY_INDEX" --since 7d --limit 100
```

Results are newest first by `event_time`, then by descending `id` for ties, and the order cannot be changed. `--since` takes a positive integer with an `ms`, `s`, `m`, `h`, `d` or `w` suffix, such as `500ms`, `15m`, `2h` or `1w`, and is measured from the moment each command runs. Events already removed by age or size retention are not returned.

## 4. Freeze an investigation window

Comparisons and pagination need fixed `[from, to)` bounds. If you seeded the demo, keep its `QUERY_FROM_MS` and `QUERY_TO_MS`. For existing data, capture the last hour once:

```sh
export QUERY_TO_MS=$(jq -nr 'now * 1000 | floor')
export QUERY_FROM_MS=$((QUERY_TO_MS - 3600000))
```

`from` is inclusive and `to` is exclusive, so an event exactly at `to` belongs to the next window. Both are Unix **milliseconds**, not seconds. The CLI needs both bounds together and rejects them combined with `--since`.

The rest of the examples use these helpers for the index and bounds:

```sh
query_window() {
  logbrook query "$QUERY_INDEX" \
    --from "$QUERY_FROM_MS" --to "$QUERY_TO_MS" "$@"
}
count_window() {
  logbrook count "$QUERY_INDEX" \
    --from "$QUERY_FROM_MS" --to "$QUERY_TO_MS" "$@"
}

query_window --limit 50
count_window                  # Demo: 12
```

To look at a specific incident, convert its UTC start and end with `jq`, which avoids platform-specific `date` flags:

```sh
export QUERY_FROM_MS=$(jq -nr '"2026-10-07T14:00:00Z" | strptime("%Y-%m-%dT%H:%M:%SZ") | mktime * 1000')
export QUERY_TO_MS=$(jq -nr '"2026-10-07T14:30:00Z" | strptime("%Y-%m-%dT%H:%M:%SZ") | mktime * 1000')
query_window --json
```

The helpers read these variables, so set the demo's bounds back before comparing its expected counts. A query can span at most 31 days by default; split longer windows or raise the server's maximum.

## 5. Find warnings and errors

```sh
# Warning and above: demo has 7.
query_window --min-level 40
count_window --min-level 40

# Error and above, including fatal: demo has 5.
query_window --min-level 50
count_window --min-level 50 --json    # {"count":5}

# Fatal and above: demo has 1.
query_window --min-level 60
```

The examples use Pino's level numbers: `10=trace`, `20=debug`, `30=info`, `40=warn`, `50=error`, `60=fatal`. Logbrook stores any nonnegative integer level. `--min-level 50` means `level >= 50`, not exactly 50, and the server has no exact-level or maximum-level filter.

## 6. Narrow an incident by service, logger, host, and source

```sh
# One service: demo has 9.
query_window --service api

# API errors and fatal events: demo has 3.
query_window --service api --min-level 50

# Errors on one API host: demo has 2.
query_window --service api --logger http --host api-1 --min-level 50

# Background-worker errors: demo has 2, including the fatal event.
query_window --service worker --logger jobs --min-level 50

# Source examples for a server configured with these source names.
query_window --source production
query_window --source production,staging --service api
```

Different filters combine with **AND**. `service`, `logger` and `host` match exact, case-sensitive values: `--service api` does not match `api-worker` or `API`. Only `source` takes a comma-separated list, meaning production **OR** staging; don't put spaces around the commas. A comma in any other filter is a literal character.

Without `--source`, a query searches every source your read token allows. Naming a source outside that list returns `403`. The source examples match demo events only if the demo's ingestion token writes to one of those sources.

## 7. Search messages literally

```sh
# Literal, case-sensitive substring: demo has 2.
query_window --message timeout

# Capitalized variant: demo has 1.
query_window --message Timeout

# Correlate an order mentioned by two different services: demo has 2.
query_window --message ord-42

# Quote spaces and shell punctuation: demo has 1.
query_window --message 'rate limit 100% reached'

# Combine text with other filters: demo has 1.
query_window --service api --min-level 50 --message timeout
```

`--message` matches a literal substring. `%`, `_`, `*`, `|` and parentheses are ordinary characters, so `--message 'timeout|declined'` looks for that exact text, and `service:api AND level:50` is just a string. Top-level attribute equality goes through `--filter`. For case-insensitive matching, OR and NOT, nested attributes or ranges, use the export recipes below.

## 8. Count the full matching window

```sh
count_window                                # Demo: 12
count_window --service api                   # Demo: 9
count_window --message ord-42                # Demo: 2
count_window --service api --min-level 50     # Demo: 3
```

The server counts every matching event in the window, whatever the search page size. Each search, count, facet or histogram request reads the data at its own point in time, so two requests with identical bounds can disagree while ingestion or retention runs.

## 9. Fetch the next page

```sh
page=$(query_window --limit 3 --json)
printf '%s\n' "$page" | jq '.events'
cursor=$(printf '%s\n' "$page" | jq -r '.next_cursor // empty')

if [ -n "$cursor" ]; then
  query_window --limit 3 --cursor "$cursor" --json
fi
```

The first demo page holds `req-012`, `req-011` and `req-010`, and the next holds `req-009`, `req-008` and `req-007`. Stop when `next_cursor` is null. Treat it as opaque: there are no offsets, and you can't build a cursor from event IDs.

Every page request must repeat the index, time bounds, filters and page size, and the CLI requires explicit bounds with `--cursor`. If the index, route style, bounds, filters, sources or page size change, the server returns `400`. Listing the same sources or attribute conditions in a different order is fine.

Pages only include events ingested before the first page, even if newer events fall inside the window; start a new search to see them. Retention can still remove events between pages or expire the cursor.

## 10. Export every page as NDJSON

This loop writes every page to `query-export.ndjson` in the current directory, replacing any existing file, and stops after the last page. If a request fails, the loop exits nonzero and leaves a partial file; delete it before retrying.

```sh
(
  set -eu
  cursor=''
  while :; do
    if [ -n "$cursor" ]; then
      page=$(query_window --limit 3 --cursor "$cursor" --json)
    else
      page=$(query_window --limit 3 --json)
    fi
    printf '%s\n' "$page" | jq -c '.events[]'
    cursor=$(printf '%s\n' "$page" | jq -r '.next_cursor // empty')
    [ -n "$cursor" ] || break
  done
) > query-export.ndjson

wc -l < query-export.ndjson              # Demo: 12
```

The demo uses three events per page so you can see it paginate. For real exports, use a larger page size, up to the server's maximum (1,000 by default). Events that retention removes before their page is read are not exported.

## 11. Filter attributes and inspect exports

When equality on a top-level attribute is enough, filter on the server. The filter applies before paging, and the count covers the whole window:

```sh
query_window --filter '{"attributes":[{"key":"requestId","op":"eq","value":"req-004"}]}' --json
count_window --filter '{"attributes":[{"key":"requestId","op":"eq","value":"req-004"}]}' --json
```

Both find the one matching demo event. The same `filter` JSON works on the HTTP count, histogram, facets and tail endpoints. Strings are case-sensitive, a number never matches a string, and missing or null attributes never match. Only literal top-level keys and `eq` are supported; see [attribute equality](API.md#attribute-equality) for limits and how numbers compare.

Everything else runs on the finished export with `jq`, on your machine. Run it on the full export from section 10, not on a single page, when you need results for the whole window.

```sh
# Exact request identifier: demo has 1 event.
jq -c 'select(.attributes.requestId == "req-004")' query-export.ndjson

# Exact error level, excluding fatal: demo has 4.
jq -c 'select(.level == 50)' query-export.ndjson

# Case-insensitive message matching: demo has 3.
jq -c 'select(.message | test("timeout"; "i"))' query-export.ndjson

# OR across two services: demo has 11.
jq -c 'select(.service == "api" or .service == "worker")' query-export.ndjson

# Exclude health-check messages: demo has 11.
jq -c 'select(.message | contains("/health") | not)' query-export.ndjson

# Numeric attribute: demo has 3 events taking at least one second.
jq -c 'select((.attributes.durationMs // 0) >= 1000)' query-export.ndjson

# JSON projection for another tool; timestamps stay in milliseconds, IDs stay strings.
jq -c '{id, event_time, service, message, requestId: .attributes.requestId}' query-export.ndjson

# CSV projection, with a header and proper quoting.
printf '%s\n' 'id,event_time,service,level,message'
jq -r '[.id, .event_time, .service, .level, .message] | @csv' query-export.ndjson
```

The demo's `durationMs` values are numbers. If your producers send the same attribute with different types, check the type before comparing numbers. For a small export, you can also group on the client, here counting slow requests per service:

```sh
jq -s 'map(select((.attributes.durationMs // 0) >= 1000))
  | group_by(.service)
  | map({service: .[0].service, slow_requests: length})' query-export.ndjson
```

Demo result: `[{"service":"api","slow_requests":3}]`. `jq -s` loads the whole export into memory; for large windows, use server-side facets when the built-in fields are enough.

## 12. Use the HTTP API directly

This helper sends a GET to the index set in `QUERY_INDEX`, with the fixed time window and every parameter URL-encoded:

```sh
logs_get() {
  curl --fail-with-body --silent --show-error --get \
    "${LOGBROOK_URL%/}/indexes/$QUERY_INDEX/logs$1" \
    -H "Authorization: Bearer $LOGBROOK_READ_TOKEN" \
    --data-urlencode "from=$QUERY_FROM_MS" \
    --data-urlencode "to=$QUERY_TO_MS" "${@:2}"
}

# Same search as CLI service+severity+message filtering; demo has 1 event.
logs_get '' --data-urlencode 'service=api' \
  --data-urlencode 'min_level=50' --data-urlencode 'message=timeout' \
  --data-urlencode 'limit=20' | jq .

# Literal punctuation is encoded correctly; demo has 1.
logs_get '' --data-urlencode 'message=rate limit 100% reached' | jq .

# Full-window count; demo has 3.
logs_get /count --data-urlencode 'service=api' \
  --data-urlencode 'min_level=50' | jq .
```

The HTTP parameter is `min_level`, where the CLI option is `--min-level`. Every HTTP search, count, facet and histogram request needs `from` and `to`; HTTP has no `since`. On the last page, `next_cursor` is null. A search with no matches returns an empty `events` array, and a count returns `{"count":0}`.

## 13. Discover services, hosts, loggers, sources, and levels

The CLI has no `facets` command, so these use HTTP.

```sh
logs_get /facets | jq .
logs_get /facets | jq '.services'

# Facets among error/fatal events; demo: api=3, worker=2.
logs_get /facets --data-urlencode 'min_level=50' | jq '.services'

# Hosts among API events; demo: api-1=6, api-2=3.
logs_get /facets --data-urlencode 'service=api' | jq '.hosts'
```

Unfiltered demo service facets:

```json
[
  {"value":"api","count":9},
  {"value":"worker","count":2},
  {"value":"auth","count":1}
]
```

The response has `services`, `loggers`, `hosts`, `levels` and `sources`, each with up to 100 non-null values sorted by descending count. Level values are strings here, such as `"50"`. Events without an optional field are left out of that field's list, so a list does not always add up to the total. Filters apply before grouping: with `service=api`, the service facet itself only shows `api`.

## 14. Chart event volume over time

The CLI has no histogram command either. Request five-minute buckets over the fixed hour:

```sh
logs_get /histogram --data-urlencode 'interval_ms=300000' | jq .

# Error/fatal volume with one-minute buckets; demo total is 5.
logs_get /histogram --data-urlencode 'interval_ms=60000' \
  --data-urlencode 'min_level=50' | jq '.buckets'

# Sum the nonempty buckets; demo: 12.
logs_get /histogram --data-urlencode 'interval_ms=300000' |
  jq '[.buckets[].count] | add // 0'
```

The response looks like `{"buckets":[{"time":1791370000000,"count":3},...]}`. `time` is the bucket start in Unix milliseconds, counted from the exact `from` you sent, and buckets come in ascending order. Empty buckets are left out, so fill them with zeros when charting. The last bucket can be shorter than the interval.

A request can cover at most 2,000 buckets. One day of one-minute buckets fits (1,440); seven days does not. Use a larger interval, such as `900000` (15 minutes, 672 buckets over seven days), or split the window. Without `interval_ms`, the server picks one minute, or a longer interval if one minute would exceed 2,000 buckets.

## 15. Watch new events with live tail

The CLI has no `tail` command; live tail is a Server-Sent Events stream over HTTP. This command stays open until you press Ctrl-C:

```sh
curl --fail-with-body --silent --show-error --no-buffer --get \
  "${LOGBROOK_URL%/}/indexes/$QUERY_INDEX/logs/tail" \
  -H "Authorization: Bearer $LOGBROOK_READ_TOKEN" \
  --data-urlencode 'service=api' --data-urlencode 'min_level=50'
```

Without a resume header, tail only sends events ingested after it connects, not the demo events already stored. In another terminal with the same URL, index and ingestion token, send an event:

```sh
jq -n '[{time: (now * 1000 | floor), level: 50, service: "api", msg: "live timeout demonstration"}]' |
  curl --fail-with-body --silent --show-error \
    "${LOGBROOK_URL%/}/indexes/$QUERY_INDEX/logs/ingest" \
    -H "Authorization: Bearer $LOGBROOK_INGEST_TOKEN" \
    -H 'Content-Type: application/json' --data-binary @-
```

This adds a thirteenth event to the demo index. Its time is after the fixed window's `to`, so the expected counts above don't change, though a later `--since` query can include it.

A `log` frame carries an event as JSON plus an SSE `id`. A `cursor` frame also has an `id` and means the stream has caught up. Keepalive comments can appear between frames. To resume later, save the SSE ID of the last frame of either type you processed:

```sh
export LAST_TAIL_ID='<exact id from the SSE stream>'
curl --fail-with-body --silent --show-error --no-buffer --get \
  "${LOGBROOK_URL%/}/indexes/$QUERY_INDEX/logs/tail" \
  -H "Authorization: Bearer $LOGBROOK_READ_TOKEN" \
  -H "Last-Event-ID: $LAST_TAIL_ID" \
  --data-urlencode 'service=api' --data-urlencode 'min_level=50'
```

Use the same index and filters, or the server returns `400`. The SSE ID is an opaque token that starts with the index name; send it unchanged. It is not a search `next_cursor` or the event's JSON `id`. On resume, the server replays matching events still retained after that point, then follows new ones. Tail follows ingestion order, so an event that arrives late with an older timestamp still shows up.

Tail takes the field filters but rejects `from`, `to`, `since`, `limit`, `cursor` and `interval_ms`, which is why it doesn't use `logs_get`. A `gap` or `error` frame ends the stream. After a gap, search the retained history before reconnecting, because resuming again cannot recover expired events. `curl` doesn't save SSE IDs or reconnect for you.

## What is supported, and common errors

| Need | Supported approach |
| --- | --- |
| Newest matching retained events | CLI `query` or HTTP `GET /indexes/{index}/logs` |
| Full-window matching count | CLI `count` or HTTP `/logs/count` |
| Top services/hosts/loggers/levels/sources | HTTP `/logs/facets` |
| Event counts per time bucket | HTTP `/logs/histogram` |
| New events plus retained sequence replay | HTTP `/logs/tail` with optional `Last-Event-ID` |
| Top-level attribute equality | `--filter` or HTTP `filter` on every query endpoint |
| Nested attributes, ranges, OR/NOT, regex, exact severity | Complete bounded export, then a client-side tool such as `jq` |
| SQL, Lucene/KQL, joins, cross-index wildcard search | Not supported |

| Symptom | Check |
| --- | --- |
| `400` invalid/excessive time range | Use milliseconds, `from < to`, and a window within the configured maximum. |
| `400` unknown query field | Only search accepts `limit` and `cursor`, and only histogram accepts `interval_ms`. |
| `401` | Use a configured read token; an admin or ingestion token cannot query. Check `LOGBROOK_TOKEN` overrides in the CLI. |
| `403` | The token must allow both the index and every requested source. |
| `404` | The index must already exist; reads do not create it. |
| `413` or CLI 16 MiB response-limit error | Narrow the filters or window, or lower the page size. The server's own response limit (8 MiB by default) also applies. |
| `429` | The server is at capacity; wait for `Retry-After` and retry with backoff. The CLI does not retry on its own. |
| `503` | Storage is unavailable or shutting down; inspect readiness and server diagnostics. |
| `504` | Narrow the scan or inspect the server's query budget. Raising CLI `--timeout` does not raise the server deadline. |
| Cursor expired / tail `gap` | Retention removed the position the cursor pointed to, or (tail only) an event exceeded the output limit. Search the retained history and start again. |

See the [CLI guide](CLI.md), [API reference](API.md), [OpenAPI contract](../src/openapi.json) and [operations guide](OPERATIONS.md) for configuration and limits. To remove the demo index when you are done, run `logbrook indexes delete "$QUERY_INDEX" --yes` with an admin token; this permanently deletes it.
