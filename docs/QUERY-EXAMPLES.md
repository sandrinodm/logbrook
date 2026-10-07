# Query cookbook

Copyable examples for the current Logbrook CLI and HTTP API, from finding one error to exporting an incident window. Search, count, facets, and histograms automatically include retained hot events and Parquet archives in the selected index.

## 1. Connect and choose an index

These examples use Bash or Zsh, a running server, and a read credential. HTTP examples use `curl`; JSON processing uses `jq`. The inline demo and UTC date conversion also use `jq`. Run repository-relative commands from the Logbrook repository root.

```sh
export LOGBROOK_URL=http://127.0.0.1:3100
export LOGBROOK_READ_TOKEN='<your configured read token>'
export QUERY_INDEX=checkout

logbrook indexes list
logbrook indexes show "$QUERY_INDEX" --json
```

If `logbrook` is not on PATH, define this function for the current shell after building the binary:

```sh
logbrook() { ./target/debug/logbrook "$@"; }
```

Use `target/release/logbrook` in that function for a release build. You can use an existing populated index and skip the demo below. With Compose, `docker compose exec logbrook logbrook query checkout --since 1h` uses the same CLI inside the running container.

Queries use `LOGBROOK_READ_TOKEN`. An explicit `--token` or `LOGBROOK_TOKEN` overrides it; clear those overrides if they select the wrong credential. Admin credentials manage indexes but cannot query events. A request addresses one index; index names do not accept wildcards.

## 2. Optional demo: twelve events with known answers

Use a fresh index so the expected results below are reproducible. You need an ingestion credential permitted to create that index, a read credential permitted to read it and its source, and a free index slot. Default `max_indexes` is four, including `default`. Repeating the ingestion appends duplicates, so generate a new index for a fresh run.

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

Expected acknowledgment: `{"accepted":12}`. Timestamps span 50 minutes to one minute before `QUERY_TO_MS`, so they work with the default seven-day retention window. A custom retention period shorter than 50 minutes will reject this fixture.

| Fixture fields | Values |
| --- | --- |
| Services | `api` (9 events), `worker` (2), `auth` (1) |
| Loggers | `http`, `jobs`, `security` |
| Hosts | `api-1`, `api-2`, `worker-1`, `auth-1` |
| Levels | 20 (1), 30 (4), 40 (2), 50 (4), 60 (1) |
| Messages | Two contain lowercase `timeout`; one contains capitalized `Timeout` |
| Custom attributes | `requestId`, and where applicable `statusCode`, `durationMs`, `orderId`, `userId`, `jobId` |

The fixture sends Pino-style `time`, `msg`, `name`, and `hostname`. Search responses expose these as `event_time`, `message`, `logger`, and `host`. Other fields become `attributes`: `requestId` becomes `attributes.requestId`. The canonical `source` comes from the ingestion credential, not a field in the payload. Event `id` is a server-generated JSON string.

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

Order is descending `event_time`, then descending `id` for ties. There is no configurable sort order. `--since` accepts a positive integer with `ms`, `s`, `m`, `h`, `d`, or `w`: for example `500ms`, `30s`, `15m`, `2h`, `7d`, or `1w`. A relative window is recalculated on each request. Returned events must still exist under the index's age and size retention policies.

## 4. Freeze an investigation window

For consistent examples, comparisons, and pagination, keep the same `[from, to)` bounds. If you seeded the demo, keep its `QUERY_FROM_MS` and `QUERY_TO_MS`. For existing data, capture a new hour once:

```sh
export QUERY_TO_MS=$(jq -nr 'now * 1000 | floor')
export QUERY_FROM_MS=$((QUERY_TO_MS - 3600000))
```

`from` is inclusive; `to` is exclusive. Both are Unix **milliseconds**, not seconds. Events exactly at the end belong to the next adjacent window. The CLI requires both bounds and rejects combining them with `--since`.

Define a helper to reuse the index and bounds throughout the remaining examples:

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

To investigate a named UTC incident window, convert UTC dates instead of relying on platform-specific `date` flags. Edit the dates for your incident:

```sh
export QUERY_FROM_MS=$(jq -nr '"2026-10-07T14:00:00Z" | strptime("%Y-%m-%dT%H:%M:%SZ") | mktime * 1000')
export QUERY_TO_MS=$(jq -nr '"2026-10-07T14:30:00Z" | strptime("%Y-%m-%dT%H:%M:%SZ") | mktime * 1000')
query_window --json
```

Changing these variables changes the helpers. Restore the demo's original bounds before comparing its expected counts. The default maximum query span is 31 days; a larger span needs separate windows or an operator-configured higher maximum.

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

The examples use the common numeric convention `10=trace`, `20=debug`, `30=info`, `40=warn`, `50=error`, `60=fatal`. Logbrook stores nonnegative numeric levels; `--min-level 50` means `level >= 50`, not exact level 50. There is no server-side exact-level or maximum-level filter.

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

Different filters combine with **AND**. `service`, `logger`, and `host` are exact, case-sensitive values: `--service api` does not match `api-worker` or `API`. Only `source` accepts a comma-separated list, meaning production **OR** staging within that filter; do not put spaces around the commas. A comma in a service value is literal, not an OR operator.

Omitting `--source` searches the sources allowed by your read credential. Explicitly requesting a source outside that scope returns `403`; it is not silently ignored. The source examples match the demo only if its ingestion credential was configured with those names.

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

Message matching is a literal substring operation. `%`, `_`, `*`, `|`, and parentheses have no wildcard or regular-expression meaning. `--message 'timeout|declined'` searches for that exact text. A string such as `service:api AND level:50` is not a query language. For case-insensitive matching, OR/NOT conditions, or custom attributes, see the export recipes below.

## 8. Count the full matching window

```sh
count_window                                # Demo: 12
count_window --service api                   # Demo: 9
count_window --message ord-42                # Demo: 2
count_window --service api --min-level 50     # Demo: 3
```

Count is computed server-side over all matching retained events, independent of the search page size. Search/count/facet/histogram requests each take their own snapshot; concurrent ingestion or retention can make separately requested totals differ even with identical bounds.

## 9. Fetch the next page

```sh
page=$(query_window --limit 3 --json)
printf '%s\n' "$page" | jq '.events'
cursor=$(printf '%s\n' "$page" | jq -r '.next_cursor // empty')

if [ -n "$cursor" ]; then
  query_window --limit 3 --cursor "$cursor" --json
fi
```

The first demo page contains requests `req-012`, `req-011`, and `req-010`; the next contains `req-009`, `req-008`, and `req-007`. Treat `next_cursor` as opaque. Stop when it is null; do not invent offsets or reconstruct cursors from event IDs.

Reuse the same index, time bounds, filters, and page size on subsequent requests. The CLI requires explicit bounds with a cursor. The server checks the named-index prefix and cursor position/high-water mark; it does not encode the complete filter set into the cursor. Keeping filters unchanged is the client's responsibility.

A cursor fixes an ingestion high-water mark, excluding events ingested after the first page even when their event times lie inside the window. Retention can still remove events or expire the cursor; this is not a permanent frozen snapshot. Start a new search to include newly ingested events.

## 10. Export every page as NDJSON

This loop writes `query-export.ndjson` in the current directory, replacing that file if it exists. It holds bounds and filters fixed and stops on the last page. A failed request leaves a partial export and returns nonzero; discard that partial file before retrying.

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

Three events per page make the demo visibly paginate. For larger exports, choose a suitable page size up to the server's configured maximum (default 1,000). This exports all matching events still available while pages are read; it cannot recover data expired by retention.

## 11. Inspect attributes and do client-side filtering

These commands operate on the completed export. They do not push predicates into Logbrook and must not be used on an incomplete first page when you need full-window results.

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

The sample `durationMs` values are numbers. If producers use mixed types, normalize or check the attribute type before numeric comparisons. For a small bounded export, summarize slow requests per service on the client:

```sh
jq -s 'map(select((.attributes.durationMs // 0) >= 1000))
  | group_by(.service)
  | map({service: .[0].service, slow_requests: length})' query-export.ndjson
```

Demo result: `[{"service":"api","slow_requests":3}]`. `jq -s` loads the export into memory; server-side facets are the better fit for counts over large windows when the built-in dimensions suffice.

## 12. Use the HTTP API directly

Define a helper that URL-encodes every query parameter and shares the same fixed time window. Paths in these examples use the `QUERY_INDEX` set above.

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

The HTTP parameter is `min_level`; the CLI option is `--min-level`. Every bounded HTTP query requires `from` and `to`; HTTP does not accept `since`. Search returns `{"events":[...],"next_cursor":null}` when no further page exists. Empty searches return an empty array; empty counts return `{"count":0}`.

## 13. Discover services, hosts, loggers, sources, and levels

Facets are currently an HTTP feature; the CLI has no `facets` subcommand.

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

The response contains `services`, `loggers`, `hosts`, `levels`, and `sources`, each with up to 100 non-null values ranked by descending count. Level values are strings in this response, such as `"50"`. Optional null fields are omitted from their dimension, so not every dimension necessarily sums to the full event count. All filters apply before grouping: filtering to `service=api` also narrows the service facet itself.

## 14. Chart event volume over time

Histograms are currently HTTP-only. Request five-minute buckets over the fixed hour:

```sh
logs_get /histogram --data-urlencode 'interval_ms=300000' | jq .

# Error/fatal volume with one-minute buckets; demo total is 5.
logs_get /histogram --data-urlencode 'interval_ms=60000' \
  --data-urlencode 'min_level=50' | jq '.buckets'

# Sum the nonempty buckets; demo: 12.
logs_get /histogram --data-urlencode 'interval_ms=300000' |
  jq '[.buckets[].count] | add // 0'
```

Response shape: `{"buckets":[{"time":1791370000000,"count":3},...]}` (timestamp illustrative). `time` is the bucket's start in Unix milliseconds. Buckets are anchored to the exact requested `from`, appear in ascending order, and include only nonempty buckets. Fill missing buckets with zero in your chart if needed. The final bucket can be shorter than the requested interval.

There are at most 2,000 possible buckets per request. A one-day query with one-minute buckets fits (1,440); a seven-day query with one-minute buckets exceeds the limit. Use a larger interval, such as `900000` (15 minutes; 672 buckets over seven days), or split the window. Omitting `interval_ms` selects at least one minute and increases it as needed to fit the bucket limit.

## 15. Watch new events with live tail

Live tail is currently HTTP/SSE-only; the CLI has no `tail` subcommand. This command stays open until interrupted with Ctrl-C:

```sh
curl --fail-with-body --silent --show-error --no-buffer --get \
  "${LOGBROOK_URL%/}/indexes/$QUERY_INDEX/logs/tail" \
  -H "Authorization: Bearer $LOGBROOK_READ_TOKEN" \
  --data-urlencode 'service=api' --data-urlencode 'min_level=50'
```

Without a resume header, tail starts at the current committed ingestion position; it does not replay the demo events already stored. In another terminal using the same URL, index, and ingestion token, submit an event:

```sh
jq -n '[{time: (now * 1000 | floor), level: 50, service: "api", msg: "live timeout demonstration"}]' |
  curl --fail-with-body --silent --show-error \
    "${LOGBROOK_URL%/}/indexes/$QUERY_INDEX/logs/ingest" \
    -H "Authorization: Bearer $LOGBROOK_INGEST_TOKEN" \
    -H 'Content-Type: application/json' --data-binary @-
```

This adds a thirteenth event to the demo index. It falls outside the original fixed hour's upper bound, so those original-window counts remain unchanged. A later relative query may include it.

The stream sends `log` frames containing a canonical event JSON object and an SSE `id`; `cursor` frames also carry an `id` and indicate the stream is caught up. Keepalive comments may appear. Save the last processed SSE ID from either frame type to resume:

```sh
export LAST_TAIL_ID='<exact id from the SSE stream>'
curl --fail-with-body --silent --show-error --no-buffer --get \
  "${LOGBROOK_URL%/}/indexes/$QUERY_INDEX/logs/tail" \
  -H "Authorization: Bearer $LOGBROOK_READ_TOKEN" \
  -H "Last-Event-ID: $LAST_TAIL_ID" \
  --data-urlencode 'service=api' --data-urlencode 'min_level=50'
```

Use the same index and filters. A named-index SSE ID includes the index prefix; use it intact. It differs from a search `next_cursor` and from the event's JSON `id`. Resume replays matching retained events after that ingestion position, then follows new events. Tail follows ingestion order, so a newly ingested event with an older event timestamp can still appear.

Tail accepts the field filters but rejects `from`, `to`, `since`, `limit`, `cursor`, and `interval_ms`; do not use `logs_get` for it because that helper adds bounds. A `gap` or `error` frame terminates the stream. Reconcile retained history with search before reconnecting after a gap; repeated blind resume cannot recover expired data. `curl` does not automatically save SSE IDs or reconnect for you.

## Supported query surface and common corrections

| Need | Supported approach |
| --- | --- |
| Newest matching retained events | CLI `query` or HTTP `GET /indexes/{index}/logs` |
| Full-window matching count | CLI `count` or HTTP `/logs/count` |
| Top services/hosts/loggers/levels/sources | HTTP `/logs/facets` |
| Event counts per time bucket | HTTP `/logs/histogram` |
| New events plus retained sequence replay | HTTP `/logs/tail` with optional `Last-Event-ID` |
| Arbitrary attribute predicates, OR/NOT, regex, exact severity | Complete bounded export, then a client-side tool such as `jq` |
| SQL, Lucene/KQL, joins, cross-index wildcard search | Not exposed by the current API/CLI |

| Symptom | Check |
| --- | --- |
| `400` invalid/excessive time range | Use milliseconds, `from < to`, and a window within the configured maximum. |
| `400` unknown query field | Use endpoint-specific parameters; only search accepts `limit/cursor`, only histogram accepts `interval_ms`. |
| `401` | Use a configured read token; an admin or ingestion token cannot query. Check `LOGBROOK_TOKEN` overrides in the CLI. |
| `403` | The token must allow both the index and every requested source. |
| `404` | The index must already exist; reads do not create it. |
| `413` or CLI 16 MiB response-limit error | Narrow filters/window or lower page size; configured server response limits also apply. |
| `429` | Capacity is occupied; honor HTTP `Retry-After` and retry with backoff. The CLI does not automatically retry. |
| `503` | Storage is unavailable or shutting down; inspect readiness and server diagnostics. |
| `504` | Narrow the scan or inspect the server's query budget. Raising CLI `--timeout` does not raise the server deadline. |
| Cursor expired / tail `gap` | Retention may expire the continuation point; tail gaps can also signal an output limit. Reconcile retained history. |

See the [CLI guide](CLI.md), [API reference](API.md), [OpenAPI contract](../src/openapi.json), and [operations guide](OPERATIONS.md) for configuration and limits. The inline demo creates synthetic data only; stop writing to the demo when finished. If you choose to remove it, `logbrook indexes delete "$QUERY_INDEX" --yes` requires a separate admin credential and permanently removes that index.
