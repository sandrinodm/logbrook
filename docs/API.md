# HTTP API

Logbrook accepts structured log batches and queries one named index per request. Search and aggregate results include archived events as well as recent ones. The [OpenAPI contract](../src/openapi.json) is served at `GET /openapi.json`, the [query cookbook](QUERY-EXAMPLES.md) has longer investigations and exports, and [operations](OPERATIONS.md) covers server configuration and storage policies.

## Routes and credentials

Send credentials as `Authorization: Bearer <token>`. Each credential has one role, and the roles do not overlap: an admin token cannot read or ingest events. A missing, malformed or unknown token, or one with the wrong role, returns `401`. A valid token that its scope denies returns `403`.

Tokens are 16 to 512 visible ASCII characters with no whitespace. The server refuses to start with any other token.

| Method and path | Credential | Result on success |
| --- | --- | --- |
| `POST /indexes/{index}/logs/ingest` | Ingestion | `200`, `{"accepted":N}` after the batch is committed |
| `GET /indexes/{index}/logs` | Read | `200`, `{"events":[...],"next_cursor":...}` |
| `GET /indexes/{index}/logs/count` | Read | `200`, `{"count":N}` |
| `GET /indexes/{index}/logs/facets` | Read | `200`, five arrays of value/count pairs |
| `GET /indexes/{index}/logs/histogram` | Read | `200`, `{"buckets":[...]}` |
| `GET /indexes/{index}/logs/tail` | Read | `200`, `text/event-stream` |
| `GET /indexes` | Read or admin | `200`, `{"indexes":[...]}` |
| `GET /indexes/{index}` | Read or admin | `200`, index information |
| `PUT /indexes/{index}` | Ingestion or admin | `200`, `{"name":"..."}` |
| `DELETE /indexes/{index}` | Admin | `200`, `{"deleted":"..."}` |
| `GET /metrics` | Unrestricted read | `200`, Prometheus text |
| `GET /health` | None | `200`, `{"status":"ok"}` |
| `GET /ready` | None | `200`, `{"status":"ready"}`, or `503` |
| `GET /openapi.json` | None | `200`, OpenAPI JSON |

The unprefixed `/logs`, `/logs/ingest`, `/logs/count`, `/logs/facets`, `/logs/histogram` and `/logs/tail` routes address the `default` index with the same credential rules. Use the named routes in new clients. Cursors from the two route styles are not interchangeable, even for `default`.

An ingestion token writes to one **source**, which is stamped on every event it sends. A read token has a list of allowed sources, and an empty list allows all of them. Ingestion and read tokens can also be limited to a list of exact index names; without that list they reach every index. A read must pass both the source and the index restriction. A query without a `source` filter searches every source the token allows, and naming a source outside it returns `403`. Admin tokens can inspect and manage every index. `/metrics` requires a read token with no source or index restriction.

## Index lifecycle

An index name has 1 to 63 lowercase ASCII letters, digits, `-` or `_`, and starts with a letter or digit. Names and scopes have no wildcards. Each index has its own storage and retention policy.

`PUT /indexes/{index}` is idempotent. Ingesting into a missing index also creates it, before the body is validated, so a rejected batch can leave an empty index behind. Reads never create indexes and return `404` for an unknown one. By default the server allows four indexes, including the mandatory `default`; creating one more returns `429`.

A new index takes the server defaults and any per-index policy in the server configuration. `PUT` reads no request body and cannot set retention or a size limit; change those in the configuration and restart.

`GET /indexes` lists only the indexes the credential can reach. Admin tokens and read tokens with no source restriction get file sizes, policy and readiness for each index:

```json
{
  "name": "checkout",
  "size_bytes": 10485760,
  "database_bytes": 10485760,
  "wal_bytes": 0,
  "archive_bytes": 0,
  "temp_bytes": 0,
  "other_bytes": 0,
  "max_size_bytes": null,
  "retention_ms": 604800000,
  "ready": true
}
```

Sizes are measured on each request as the sum of file lengths, not allocated disk blocks, and change while the index is being written. `max_size_bytes: null` means the index has no size limit. A read token restricted to particular sources gets only `{"name":"checkout"}`, so it cannot learn how much data other sources store.

`DELETE` permanently removes an index and needs an admin token. `default` and indexes declared in the server configuration cannot be deleted (`400`); remove a configured index from the configuration and restart first. Stop producers before deleting, because a later ingestion recreates the index. Once the server has accepted a deletion it finishes even if the client disconnects, and it retries interrupted cleanup at startup.

## Ingest a batch

These examples use Bash or Zsh, `curl`, `jq` and a running server. Replace the token placeholders with configured tokens. Both tokens must allow the chosen index, and the read token must allow the ingestion token's source. Every ingestion appends new events, so running it twice stores duplicates.

```sh
export LOGBROOK_URL=http://127.0.0.1:3100
export LOGBROOK_INGEST_TOKEN='<configured ingestion token>'
export LOGBROOK_READ_TOKEN='<configured read token>'
export API_INDEX="api-demo-$(date +%s)"
export API_TO_MS=$(jq -nr 'now * 1000 | floor')
export API_FROM_MS=$((API_TO_MS - 60000))

jq -n --argjson until "$API_TO_MS" '
  [
    {time: ($until - 2000), level: 30, service: "checkout",
     name: "http", hostname: "api-1", msg: "order accepted",
     requestId: "demo-001"},
    {time: ($until - 1000), level: 50, service: "checkout",
     name: "http", hostname: "api-1", msg: "payment timeout",
     requestId: "demo-002", durationMs: 1200}
  ]
' |
  curl --fail-with-body --silent --show-error \
    "${LOGBROOK_URL%/}/indexes/$API_INDEX/logs/ingest" \
    -H "Authorization: Bearer $LOGBROOK_INGEST_TOKEN" \
    -H 'Content-Type: application/json' --data-binary @-
```

Expected response: `{"accepted":2}`. The event times are taken from the current clock so they fall inside the default retention window. The example needs a free index slot.

The body is a nonempty JSON **array** of objects, even for a single event. For NDJSON, send `Content-Type: application/x-ndjson` (or `application/ndjson`) with one event object per line and no enclosing array; blank lines are ignored. A request without `Content-Type` is read as JSON, and any other media type returns `400`. Parameters such as `; charset=utf-8` are accepted.

| Input field | Requirement | Canonical response field |
| --- | --- | --- |
| `time` | Required integer Unix milliseconds, `0..253402300799999` | `event_time` |
| `level` | Required integer, `0..2147483647` | `level` |
| `msg` | Required string, including an empty string | `message` |
| `service` | Optional string or null | `service` |
| `name` | Optional string or null | `logger` |
| `hostname` | Optional string or null | `host` |
| `pid` | Optional nonnegative signed 64-bit integer or null | `pid` |
| All other keys | JSON values within configured limits | Nested under `attributes` |

Optional fields that are missing come back as null. Other keys are kept as sent, including nested objects and arrays. A producer's own `id`, `source`, `event_time`, `received_at`, `message`, `host` or `logger` key becomes an attribute and does not override the canonical field. An input `attributes` object stays nested at `attributes.attributes`. The server sets `id` (a JSON string), `received_at` (Unix milliseconds) and `source` (from the ingestion token).

A batch is accepted or rejected as a whole: one invalid, oversized or expired event rejects all of it. Event times must fall between `now - retention_ms` and `now + max_future_skew_ms` (five minutes by default). Raising retention later does not let you ingest events older than data the server has already expired. To load older history, use the offline `logbrook import` command described in [operations](OPERATIONS.md).

A `200` means the batch is committed. There are no idempotency keys, so after a timeout or dropped connection you cannot tell whether the batch was stored, and retrying it can store duplicates.

## Search and filter

Search, count, facets and histogram require integer Unix millisecond `from` and `to` parameters with `0 <= from < to <= 253402300799999`. The interval is half-open: `from` is inclusive and `to` is exclusive. The span can be at most 31 days by default. HTTP has no `since` parameter.

| Parameter | Meaning | Accepted by |
| --- | --- | --- |
| `source` | One source or a comma-separated OR list, no spaces around commas | Search, count, facets, histogram, tail |
| `service`, `logger`, `host` | Exact, case-sensitive string | Search, count, facets, histogram, tail |
| `message` | Literal, case-sensitive substring | Search, count, facets, histogram, tail |
| `min_level` | Nonnegative integer; matches `level >= min_level` | Search, count, facets, histogram, tail |
| `filter` | URL-encoded JSON with top-level attribute equality conditions | Search, count, facets, histogram, tail |
| `from`, `to` | Required event-time bounds | Search, count, facets, histogram |
| `limit` | Search page size; default 100 (or configured maximum if lower), maximum 1,000 by default | Search only |
| `cursor` | Opaque continuation from `next_cursor` | Search only |
| `interval_ms` | Positive histogram bucket duration | Histogram only |

Different filters combine with AND; only the `source` list means OR. A request can name at most 128 sources. Text filters must fit the configured field byte limit, and a cursor is at most 1,024 bytes including its index prefix. On the log GET routes, the path plus query string is limited to 32,768 bytes. In `message`, characters such as `%`, `_`, `*` and regex punctuation match literally.

There are no nested attribute paths, attribute ranges, grouping by attribute, exact or maximum severity filters, regular expressions, query language or cross-index queries. For those, export the events and filter them client-side. Unknown parameters, and parameters the endpoint does not accept, return `400`.

URL-encode every parameter. With the bounds captured above, this request matches both demo events and returns the newest:

```sh
curl --fail-with-body --silent --show-error --get \
  "${LOGBROOK_URL%/}/indexes/$API_INDEX/logs" \
  -H "Authorization: Bearer $LOGBROOK_READ_TOKEN" \
  --data-urlencode "from=$API_FROM_MS" --data-urlencode "to=$API_TO_MS" \
  --data-urlencode 'service=checkout' --data-urlencode 'limit=1'
```

The response looks like this (the IDs, source and timestamps will differ):

```json
{
  "events": [{
    "id": "2", "source": "production",
    "event_time": 1791453599000, "received_at": 1791453600000,
    "level": 50, "service": "checkout", "logger": "http",
    "host": "api-1", "pid": null, "message": "payment timeout",
    "attributes": {"requestId": "demo-002", "durationMs": 1200}
  }],
  "next_cursor": "<opaque continuation>"
}
```

Results are sorted by descending `event_time`, then descending `id` for ties, and the order cannot be changed. An empty result is `{"events":[],"next_cursor":null}`.

To get the next page, send the returned `next_cursor` as `cursor` and stop when it is null. Keep the index, route style, `from`, `to`, filters, sources and `limit` the same: the cursor is tied to the query it came from, and a request that changes any of them returns `400`. Listing the same sources or attribute conditions in a different order is fine. Treat the cursor as opaque and never build one from an event `id`.

A paginated search only returns events that were ingested before its first page, even if later events fall inside the time window. It is not a snapshot, though: retention can still remove events between pages, or expire the cursor itself (`400`). Cursors stay valid across server restarts. After an index is deleted and created again, start a new search instead of reusing an old cursor. A cursor grants no access: every page is checked against the token that sends it.

## Attribute equality

Pass one URL-encoded JSON object in `filter`:

```json
{"attributes":[{"key":"runId","op":"eq","value":"run-123"},{"key":"actorJobId","op":"eq","value":"actor-job-456"}]}
```

All conditions must match, and they apply before pagination and aggregation, so counts, facets and histograms cover only matching events. The same `filter` works on search, count, facets, histogram and tail, and combines with the other filters such as `min_level=50`.

```sh
curl --fail-with-body --silent --show-error --get \
  "${LOGBROOK_URL%/}/indexes/$API_INDEX/logs/count" \
  -H "Authorization: Bearer $LOGBROOK_READ_TOKEN" \
  --data-urlencode "from=$API_FROM_MS" --data-urlencode "to=$API_TO_MS" \
  --data-urlencode 'filter={"attributes":[{"key":"runId","op":"eq","value":"run-123"}]}'
```

A key names a property directly under the event's `attributes`. Dots, slashes, tildes, dollar signs and wildcard characters in a key are matched literally. Keys and string values are case-sensitive and are not Unicode-normalized. The only operator is `eq`, and the value must be a string, number or boolean; null, object and array values are rejected. An event whose property is missing, null, an array or an object never matches. Types must agree: `123` does not match `"123"`, and `true` matches neither `1` nor `"true"`.

Numbers compare by value, so `123`, `123.0` and `1.23e2` are equal, and so are `0` and `-0.0`. Integers in the signed or unsigned 64-bit range compare exactly. Other numbers compare as 64-bit floats, the same precision ingestion stores. An integer filter value outside the 64-bit range, or a number that is not finite, is rejected.

| Filter limit | Value |
| --- | --- |
| Filter document, after URL decoding | 4,096 bytes |
| Conditions | 16 |
| Key | 1 to 256 bytes, or the configured field limit if lower |
| String value | 0 to 1,024 bytes, or the configured field limit if lower |
| Numeric value | 128 characters |

The encoded filter also counts toward the 32,768-byte request target limit, and a proxy in front of Logbrook may allow less.

Omitting `filter` is the same as `{"attributes":[]}`. A condition repeated with the same value counts once (after the 16-condition check), and two different values for one key are rejected. Empty or malformed JSON, `{}`, unknown or repeated members, a repeated `filter` parameter and operators other than `eq` return `400`, for example `{"error":"filter.attributes[1].op: only eq is supported"}`.

Only top-level attributes can be matched. A custom field sent at the top level of an ingested event, such as `runId`, becomes `attributes.runId` and can be filtered. An input `attributes` object stays at `attributes.attributes`, and events received over Fluent Forward keep their application fields under `attributes.application`. Equality filters cannot reach fields nested like that.

## Counts, facets, and histograms

These endpoints cover every matching event in the window, regardless of search page size. Each request reads the data at its own point in time, so totals from separate requests can differ while ingestion or retention runs.

```sh
# Demo: {"count":1} for error and above.
curl --fail-with-body --silent --show-error --get \
  "${LOGBROOK_URL%/}/indexes/$API_INDEX/logs/count" \
  -H "Authorization: Bearer $LOGBROOK_READ_TOKEN" \
  --data-urlencode "from=$API_FROM_MS" --data-urlencode "to=$API_TO_MS" \
  --data-urlencode 'min_level=50'
```

`/logs/count` returns `{"count":0}` when nothing matches. `/logs/facets` returns `services`, `loggers`, `hosts`, `levels` and `sources`. Each is an array of up to 100 non-null values sorted by descending count, such as `{"value":"checkout","count":2}`. Level values are strings, such as `"50"`. Events without an optional field are left out of that field's array. Filters apply before grouping, including a filter on the field being grouped.

`/logs/histogram` returns `{"buckets":[{"time":...,"count":...}]}`. Each `time` is a bucket start in Unix milliseconds, counted from the exact `from` you requested, and buckets come in ascending order. Empty buckets are left out, so fill them with zeros when charting. The last bucket can be shorter than the interval. An empty histogram is `{"buckets":[]}`.

A request can cover at most 2,000 buckets, counting a partial last bucket: `ceil((to - from) / interval_ms) <= 2000`. Without `interval_ms`, the server uses 60,000 ms, or the smallest interval that fits 2,000 buckets if that is larger. For the demo, `/logs/histogram` with the same bounds and `interval_ms=10000` returns buckets that sum to two, and `/logs/facets` with the same bounds shows `checkout` with a count of two.

## Live tail and resume

Tail follows events in the order they were ingested, so an event with an older `event_time` that arrives late appears after newer ones. It takes the filters from the table above but no time bounds, page size, cursor or histogram interval. Without `Last-Event-ID`, it starts at the newest committed event and only sends events ingested after that; it does not replay history.

```sh
curl --fail-with-body --silent --show-error --no-buffer --get \
  "${LOGBROOK_URL%/}/indexes/$API_INDEX/logs/tail" \
  -H "Authorization: Bearer $LOGBROOK_READ_TOKEN" \
  --data-urlencode 'service=checkout'
```

The connection stays open until you interrupt it. Ingest another event from a second terminal to see frames like these:

```text
event: log
id: checkout:v1.<opaque-resume-token>
data: {"id":"3","source":"production",...}

event: cursor
id: checkout:v1.<opaque-resume-token>
data: caught up
```

Real IDs start with your index name, and `log` data is a complete event object. A `cursor` frame means the stream has caught up; its ID is a resume point even when the filters matched nothing. Keepalive comments can appear between frames.

To resume, save the SSE ID of the last `log` or `cursor` frame you processed and reconnect with `Last-Event-ID: <that id>`, using the same index, route style and filters. The server replays matching events still retained after that point, then follows new ones. A resume ID with different filters returns `400` before the stream starts. Treat the ID as opaque, keep its index prefix, and don't confuse it with a search `next_cursor` or the event's own `id`.

Two frames end the stream. `gap` means the resume point has expired under retention, or the next event is larger than the per-subscriber output limit; `error` means the tail query failed. After a `gap`, search the retained history before reconnecting, because resuming again cannot recover expired events. If the very first event to replay is over the output limit, the request fails with `413` before streaming starts. Other failures before the stream starts are ordinary HTTP errors. A long replay does not cause a gap by itself. Clients reconnect and store IDs themselves; the server does neither.

## Limits and errors

Operators can change every limit. The defaults are:

| Limit | Default |
| --- | --- |
| Body / individual event wire size | 1 MiB / 64 KiB |
| Events per batch | 1,000 |
| Field string or metadata key | 4 KiB (UTF-8 bytes) |
| JSON nesting depth | 16 |
| Live retention / future skew | 7 days / 5 minutes |
| Query span / page size / response bytes | 31 days / 1,000 events / 8 MiB |
| Query deadline | 5 seconds |
| Body idle / total receive deadline | 5 / 30 seconds |
| Tail subscribers / output bytes per subscriber | 8 / 1 MiB |
| Index count, including default | 4 |

Request, body memory, connection and tail limits are shared by all indexes. A page can exceed the response byte limit even when `limit` is valid; lower the page size or narrow the filters.

Retention works on event time. Archived events stay searchable until they expire. An optional size limit per index expires the oldest data before its age limit. The size limit is a target rather than a hard disk ceiling, since freed space can take a while to return to the filesystem. Ingestion returns `507` while an index is at its size limit. Raising a limit does not bring back expired data. See [operations](OPERATIONS.md) for the policy settings.

Errors are JSON objects of the form `{"error":"explanation"}`. The explanation is for people; don't parse it as an error code.

| Status | Meaning / client action |
| --- | --- |
| `400` | Invalid payload, unsupported media type, bounds/parameter error, protected deletion, or expired/invalid search cursor; correct the request or start a new search |
| `401` | Missing/invalid bearer credential or wrong credential role |
| `403` | Source/index scope denial, non-admin deletion, or restricted metrics access |
| `404` | Unknown index or endpoint |
| `413` | Body, event, metadata, or response size limit; shrink the request or result |
| `429` | Admission, worker, subscriber, diagnostics, or index capacity pressure; includes `Retry-After: 1` |
| `503` | Storage unavailable, integrity failure, or shutdown |
| `504` | Body receive or query deadline exceeded; ingestion outcomes may be uncertain once admitted |
| `507` | Selected index has reached its physical size target |

Retry `429` with bounded backoff, keeping in mind that a retried ingestion can duplicate events. A known route called with an unsupported method can return `405`.

## Health, readiness, and metrics

`/health` is a public liveness check. `/ready` returns `503` with an error object when any index's storage is unavailable; it does not check free disk space. `/openapi.json` is public.

`/metrics` needs a read token with no source or index restriction. It serves Prometheus text covering HTTP requests and latency, ingested and rejected events, readiness, queues, memory budgets, tails and gaps, and storage. Storage series have an index label; HTTP budget series are global. `logbrook_ingested_events_total` counts events committed since the process started, including events later deleted or expired, and starts from zero on every restart. See [operations](OPERATIONS.md) for monitoring.
