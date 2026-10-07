# HTTP API

Logbrook accepts structured log batches and queries one named index per request. Search and aggregates include retained hot events and Parquet archives automatically. The machine-readable [OpenAPI contract](../src/openapi.json) is served at `GET /openapi.json`; the [query cookbook](QUERY-EXAMPLES.md) covers longer investigations and exports. See [operations](OPERATIONS.md) for server configuration and storage policies.

## Routes and credentials

Send credentials as `Authorization: Bearer <token>`. Credential roles are separate: administration does not grant event read or ingestion access. Missing, malformed, unknown, or inappropriate-role tokens return `401`; a recognized token denied by a scope returns `403`.

Configure tokens with 16 to 512 visible ASCII characters and no whitespace. The server rejects non-ASCII and control characters during configuration validation.

| Method and path | Credential | Result on success |
| --- | --- | --- |
| `POST /indexes/{index}/logs/ingest` | Ingestion | `200`, `{"accepted":N}` after durable commit |
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

The unprefixed `/logs`, `/logs/ingest`, `/logs/count`, `/logs/facets`, `/logs/histogram`, and `/logs/tail` routes address `default` and enforce the same credential scopes. Prefer named routes for new clients. Named-route cursors include their index prefix; they are not interchangeable with unprefixed-route cursors, even for `default`.

An ingestion token maps to one canonical **source**. A read token has an allowed source list: an empty list grants all sources. Optional ingestion/read index scopes restrict each token to exact index names; a missing or empty index scope grants all indexes. A read must satisfy both source and index restrictions. Omitting the `source` filter searches all sources permitted by the read token; explicitly requesting any disallowed source returns `403`. Admin credentials can inspect and manage all indexes. Metrics require a read token unrestricted by both source and index.

## Index lifecycle

An index name contains 1 to 63 lowercase ASCII letters, digits, `-`, or `_`, starting with a letter or digit. Names and scopes have no wildcard syntax. Each index has independent storage and retention policies.

`PUT /indexes/{index}` is idempotent. Authorized ingestion also creates a missing index automatically; the index can be created before body validation, so a rejected batch can leave an empty index. Reads never create indexes and return `404` for an unknown index. The default maximum is four indexes, including the mandatory `default`; creation at capacity returns `429`.

Index creation inherits server defaults and any configured per-index policy. The PUT request does not set a retention or size policy. Change those policies through server configuration and restart.

`GET /indexes` lists only indexes allowed by the credential. Admin tokens and readers with unrestricted source access receive fresh physical file measurements, policy, and readiness:

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

Measurements are illustrative, can change during concurrent activity, and count logical file lengths rather than allocated filesystem blocks. Source-scoped readers receive only `{"name":"checkout"}` to avoid exposing other sources' storage usage. `max_size_bytes: null` means size retention is disabled.

Deletion permanently removes a dynamic index and requires a separate admin credential. `default` and indexes declared in server configuration are protected (`400`). Remove a configured index from configuration and restart before deleting it. Stop producers first: later ingestion can recreate a deleted index. Deletion drains the selected index and cleans up independently of the client's connection; failed cleanup can be retried on startup or a later deletion.

## Ingest a batch

These examples use Bash or Zsh, `curl`, `jq`, and an already running server. Replace the credential placeholders with configured tokens; ensure both tokens allow the chosen index and that the reader allows the producer's source. Each ingestion appends new events, so repeating it creates duplicates.

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

Expected response: `{"accepted":2}`. Times are captured at runtime to fit the default live retention window. This example needs a free index slot and a retention policy accepting these recent timestamps.

The request body must be a nonempty JSON **array** of objects, including for a single event. For NDJSON, use `Content-Type: application/x-ndjson` (or `application/ndjson`) and one event object per line, without an enclosing array. Blank lines are ignored. Missing Content-Type defaults to JSON; other media types return `400`. Media-type parameters such as `; charset=utf-8` are accepted.

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

Optional missing fields become null in canonical event objects. Unknown keys are preserved, including nested objects and arrays. A producer's `id`, `source`, `event_time`, `received_at`, `message`, `host`, or `logger` keys become attributes and cannot override canonical fields. An input `attributes` object stays nested at `attributes.attributes`; it is not merged into the canonical attributes map. The server assigns the event `id` as a JSON string, `received_at` as Unix milliseconds, and `source` from the ingestion credential.

Validation and storage commit are atomic for the event batch. One invalid, oversized, or expired event rejects the whole batch. Live ingestion accepts event times from `now - retention_ms` through `now + max_future_skew_ms` (default five minutes ahead), then rechecks the persisted retention cutoff at commit. Enlarging retention cannot admit timestamps already expired by that cutoff. Historical offline imports are a separate CLI operation.

Success acknowledges appender flush and transaction commit. There are no producer idempotency keys. A timeout or connection loss can leave an uncertain outcome; retrying may duplicate events.

## Search and filter

Every bounded search or aggregate requires integer Unix millisecond `from` and `to` parameters with `0 <= from < to <= 253402300799999`. The interval is half-open: `from` is inclusive and `to` is exclusive. The maximum default span is 31 days. HTTP has no `since` parameter.

| Parameter | Meaning | Accepted by |
| --- | --- | --- |
| `source` | One source or a comma-separated OR list, no spaces around commas | Search, count, facets, histogram, tail |
| `service`, `logger`, `host` | Exact, case-sensitive string | Search, count, facets, histogram, tail |
| `message` | Literal, case-sensitive substring | Search, count, facets, histogram, tail |
| `min_level` | Nonnegative integer; matches `level >= min_level` | Search, count, facets, histogram, tail |
| `from`, `to` | Required event-time bounds | Search, count, facets, histogram |
| `limit` | Search page size; default 100 (or configured maximum if lower), maximum 1,000 by default | Search only |
| `cursor` | Opaque continuation from `next_cursor` | Search only |
| `interval_ms` | Positive histogram bucket duration | Histogram only |

Filters combine with AND; only the source list has OR semantics. At most 128 sources can be requested. Text fields and search cursors must fit the configured field byte limit. `%`, `_`, `*`, and regex punctuation are literal message characters. Arbitrary attribute predicates, exact/maximum severity, regex, query languages, and cross-index queries are not exposed. Export and filter client-side when needed. Unknown or endpoint-inappropriate parameters return `400`.

Use URL encoding for all parameters. The fixed bounds captured above make this request include both demo events:

```sh
curl --fail-with-body --silent --show-error --get \
  "${LOGBROOK_URL%/}/indexes/$API_INDEX/logs" \
  -H "Authorization: Bearer $LOGBROOK_READ_TOKEN" \
  --data-urlencode "from=$API_FROM_MS" --data-urlencode "to=$API_TO_MS" \
  --data-urlencode 'service=checkout' --data-urlencode 'limit=1'
```

A canonical event has this shape (IDs, source, and timestamps below are illustrative):

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

Results sort by descending `event_time`, then descending `id` for ties. There is no configurable sort. Empty results return `{"events":[],"next_cursor":null}`.

To paginate, pass the exact returned `next_cursor` on the next request; stop when it is null. Reuse the same index, route style, bounds, filters, and limit. The cursor records a position and ingestion high-water mark; it does not encode all filters. Keeping the query unchanged is the client's responsibility. Events ingested after the first page are excluded, even if their event times are inside the window. Retention can still remove events or expire the continuation (`400`); pagination is not a permanent snapshot.

## Counts, facets, and histograms

These endpoints compute over the entire matching retained window, independently of search pagination. Each request takes its own snapshot, so concurrent ingestion or retention may change totals between requests.

```sh
# Demo: {"count":1} for error and above.
curl --fail-with-body --silent --show-error --get \
  "${LOGBROOK_URL%/}/indexes/$API_INDEX/logs/count" \
  -H "Authorization: Bearer $LOGBROOK_READ_TOKEN" \
  --data-urlencode "from=$API_FROM_MS" --data-urlencode "to=$API_TO_MS" \
  --data-urlencode 'min_level=50'
```

`/logs/count` returns `{"count":0}` for no matches. `/logs/facets` returns `services`, `loggers`, `hosts`, `levels`, and `sources`. Each array contains up to 100 non-null values ranked by descending count, for example `{"value":"checkout","count":2}`. Level values are strings such as `"50"`. Missing optional fields do not contribute to that dimension. All filters apply before grouping, including a filter on the dimension being grouped.

`/logs/histogram` returns `{"buckets":[{"time":...,"count":...}]}`. Bucket starts are Unix milliseconds, anchored to the exact requested `from`, and returned in ascending order. Only nonempty buckets appear; fill missing buckets with zeros in a chart. The last bucket may be shorter than the interval. An empty histogram returns `{"buckets":[]}`.

At most 2,000 possible buckets are allowed, including a partial final bucket: `ceil((to - from) / interval_ms) <= 2000`. Omitting `interval_ms` selects at least 60,000 ms and increases it to fit this limit. For the demo, request `/logs/histogram` with the same bounds and `interval_ms=10000`; bucket counts sum to two. Request `/logs/facets` with the same bounds to see the checkout service count of two.

## Live tail and resume

Tail follows committed **ingestion order**, so late-arriving events with older event times can appear after newer events. It accepts only the field filters in the table above, with no time bounds, page size, search cursor, or histogram interval. Without `Last-Event-ID`, it starts at the current committed position and follows new ingestion rather than replaying existing history.

```sh
curl --fail-with-body --silent --show-error --no-buffer --get \
  "${LOGBROOK_URL%/}/indexes/$API_INDEX/logs/tail" \
  -H "Authorization: Bearer $LOGBROOK_READ_TOKEN" \
  --data-urlencode 'service=checkout'
```

The connection stays open until interrupted. Submit another event in a separate terminal to see a frame:

```text
event: log
id: checkout:3
data: {"id":"3","source":"production",...}

event: cursor
id: checkout:3
data: caught up
```

This illustrates framing only; actual IDs include your selected index name and `log` data contains a complete canonical event object. `cursor` frames indicate that the stream is caught up and advance its ingestion position even when filters exclude events. Keepalive comments may appear between frames.

Persist the last processed SSE ID from either `log` or `cursor`. Reconnect with `Last-Event-ID: <exact SSE id>` using the same index, route style, and filters to replay matching retained events after that position and then follow new ones. Treat the ID as opaque and preserve its named-index prefix. It differs from a search `next_cursor` and the event object's unprefixed JSON `id`.

A `gap` frame signals retention expiry or a subscriber output byte limit and terminates the stream. Reconcile retained history with search before reconnecting; expired events cannot be recovered by repeating resume. An `error` frame also terminates the stream. Failures before streaming use ordinary HTTP error responses; failures after headers are sent use SSE frames. A large replay is drained in bounded pages; backlog size alone does not imply a gap. Clients must implement their own reconnect and ID persistence.

## Limits and errors

Limits are operator-configurable. Native defaults include:

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

Request, body-memory, connection, and tail budgets are shared across indexes. Response byte limits apply even when a requested page size is valid. Lower the page size or narrow filters if a response exceeds its budget.

Retention is by event time and the committed expiry cutoff. Archives remain queryable until expiry. Optional size retention can expire oldest data before its age limit. Its per-index measured file footprint is a target, not an exact disk ceiling; database allocation and pinned reader files may delay physical reclamation. Ingestion returns `507` when the selected index remains at its target. Raising limits does not resurrect expired data. See [operations](OPERATIONS.md) for policy settings and physical disk behavior.

Application errors have the JSON shape `{"error":"explanation"}`. Do not parse the explanation as a stable error code.

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

Use bounded backoff for capacity failures and preserve the ingestion retry caveat above. Unsupported HTTP methods can return framework-level `405` responses.

## Health, readiness, and metrics

`/health` is a public liveness response. `/ready` checks storage readiness across the registry and returns `503` with an error object if unavailable; it does not predict free disk space. `/openapi.json` is public and contains the served contract.

`/metrics` requires an unrestricted read credential and exposes Prometheus text for HTTP requests and latency, ingest commitments/rejections, readiness, queues, memory admission budgets, tails/gaps, and storage. Storage series carry index labels; HTTP budgets remain global. `logbrook_ingested_events_total` counts durable commitments since process startup, including events later deleted or expired. It resets on restart and does not recount preexisting data. See [operations](OPERATIONS.md) for monitoring and configuration details.
