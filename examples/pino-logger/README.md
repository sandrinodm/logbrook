# Send Pino logs to Logbrook

This example sends structured logs from Node.js to a Logbrook index with [Pino](https://getpino.io) and [`pino-http-transport`](https://github.com/sandrinodm/pino-http-transport). It shows child loggers, error serialization, a paced bulk producer and a shutdown that waits for delivery.

## Requirements

- Node.js 24 or later. CI tests it on Node.js 26.13.0.
- A running Logbrook server and its ingestion token. Start one with the [root quick start](../../README.md#quick-start) or [Docker Compose](../../README.md#run-with-docker-compose). Read and admin tokens can't ingest.
- To check the results, the server's read token and the `logbrook` CLI.

`package-lock.json` pins Pino 10.4.0 and pino-http-transport 2.1.0.

## Quick start

From the repository root:

```sh
cd examples/pino-logger
npm ci

export LOGBROOK_URL=http://127.0.0.1:3100
export LOGBROOK_INGEST_TOKEN='<your ingestion token>'
export LOGBROOK_INDEX=pino-demo

npm start
```

`npm start` runs [basic.mjs](basic.mjs). It logs four records (a startup message, a completed checkout, a slow-provider warning and a payment error), then closes the transport, which sends the last partial batch. The logger setup and shutdown helper are in [logger.mjs](logger.mjs).

The transport sends `POST /indexes/pino-demo/logs/ingest` with a `Bearer` authorization header and `Content-Type: application/json`. If `pino-demo` doesn't exist, the first write creates it, as long as the token may write to it and the server has a free index slot. A server has four index slots by default, and `default` takes one of them.

`LOGBROOK_URL` is the server's base URL. It can include a reverse-proxy path prefix such as `https://logs.example.com/logbrook`, but not credentials, a query string or a fragment. The example appends the ingestion path. Use HTTPS for any server that isn't on your machine.

### Use a .env file

```sh
cp .env.example .env
```

Set `LOGBROOK_INGEST_TOKEN` in `.env`. `npm start` and `npm run bulk` load `.env` when it exists, but variables already exported in your shell win over the file, so unset any you want `.env` to control. The repository's `.gitignore` excludes `.env`.

### Check the records

The `logbrook` CLI doesn't read this example's `.env`. In a shell with the read token, export the same URL and index:

```sh
export LOGBROOK_URL=http://127.0.0.1:3100
export LOGBROOK_INDEX=pino-demo
export LOGBROOK_READ_TOKEN='<your read token>'

logbrook query "$LOGBROOK_INDEX" --since 15m --service checkout --json
logbrook count "$LOGBROOK_INDEX" --since 15m --service checkout
```

If `logbrook` isn't on your `PATH`, point a shell function at your build. From this directory:

```sh
logbrook() { ../../target/release/logbrook "$@"; }
```

After one run against a new index the count is `4`, and each further run adds four.

## Use it in your application

Create one logger and transport per process, and use child loggers to add request context. Every child shares the same worker thread and HTTP queue:

```js
import { createLogger } from './logger.mjs';

const { logger, close } = await createLogger();
try {
  const requestLog = logger.child({ requestId: 'req-123', orderId: 'ord-42' });
  requestLog.info({ durationMs: 42, statusCode: 200 }, 'Checkout completed');
  requestLog.error({ err: new Error('Payment declined') }, 'Payment failed');
} finally {
  await close();
}
```

This script closes right away because its work is done. In a server, call `close()` only after you stop accepting work and in-flight requests finish. `createLogger()` also returns `throwIfFailed()`, which throws if the transport has reported an error; [bulk.mjs](bulk.mjs) calls it to stop producing after a failure.

Logbrook maps Pino's fields like this:

| Pino field | Logbrook field |
| --- | --- |
| `time`, `level`, `msg` | `event_time`, `level`, `message` |
| `service`, `name`, `hostname`, `pid` | `service`, `logger`, `host`, `pid` |
| `requestId`, `orderId`, `durationMs`, `err` and any other field | Inside `attributes` |
| (the ingestion token) | `source`; a payload field can't override it |

The example sets `name` to `pino-example` and `service` from `LOG_SERVICE`. Keep Pino's defaults for the fields Logbrook requires:

- Numeric levels. Don't format levels as labels such as `"info"`.
- Unix-millisecond timestamps. Don't switch to ISO time strings.
- A message on every call. Pino omits `msg` when you log a plain object without a message string, and Logbrook rejects events without one.

Pino's `err` serializer records the error's type, message and stack. Every string value, stack traces included, must fit the server's default field limit of 4,096 bytes, or the whole batch is rejected. Log selected fields rather than whole request bodies or headers, and use Pino's [redaction](https://getpino.io/#/docs/redaction) for anything sensitive.

## Send larger volumes

[bulk.mjs](bulk.mjs) logs a fixed number of small records, each with a `runId` and an increasing `sequence` attribute:

```sh
# Default: 10,000 records at about 1,000 per second.
npm run bulk

# 100,000 records at about 5,000 per second in batches of 500, with a known run ID.
export RUN_ID="bulk-$(date +%s)"
LOG_COUNT=100000 LOG_RATE=5000 BATCH_SIZE=500 npm run bulk
```

It prints a JSON summary with the `runId`, the number of records `produced`, whether it was interrupted and the elapsed time. `produced` counts logger calls, not records Logbrook stored. The second run is paced to take about 20 seconds, plus startup and drain time.

The generator logs at most 100 records at a time and sleeps between bursts to hold the rate, so it never builds the whole run in memory. Ctrl-C or SIGTERM stops production, then drains the transport as usual.

To see what arrived, use the shell from [Check the records](#check-the-records) with the same run ID:

```sh
export RUN_ID='<runId printed by npm run bulk>'

logbrook count "$LOGBROOK_INDEX" --since 1h --message "bulk example run=$RUN_ID"
logbrook query "$LOGBROOK_INDEX" --since 1h --message "bulk example run=$RUN_ID" --limit 10 --json
```

Make sure the window covers the whole run. If nothing was dropped, expired or sent twice, the count is exactly `100000`. A higher count means retries stored duplicates or the run ID was reused. For a lower count, check the transport's stderr, the server logs and the index's retention.

## Settings

All settings are environment variables. The example changes several of the transport's defaults:

| Variable | Example default | Transport default | Purpose |
| --- | --- | --- | --- |
| `LOGBROOK_URL` | `http://127.0.0.1:3100` | | Server base URL |
| `LOGBROOK_INDEX` | `pino-demo` | | Destination index |
| `LOGBROOK_INGEST_TOKEN` | Required | | Ingestion token |
| `LOG_SERVICE` | `checkout` | | Value of the `service` field |
| `BATCH_SIZE` | `250` | `100` | Records per HTTP request, 1 to 1,000 |
| `BATCH_INTERVAL_MS` | `1000` | `5000` | Wait before sending a partial batch; a request in flight can delay it further |
| `MAX_BUFFER_SIZE` | `10000` | `100000` | Waiting records, not counting the batch in flight; at least `BATCH_SIZE` |
| `HTTP_TIMEOUT_MS` | `1500` | `2500` | Timeout for each HTTP attempt |
| `MAX_RETRIES` | `2` | `2` | Retries after the first attempt, 0 to 10 |
| `RETRY_DELAY_MS` | `250` | `1000` | First retry delay; doubles each retry, capped at the timeout |
| `LOG_COUNT` | `10000` | | Records produced by `npm run bulk` |
| `LOG_RATE` | `1000` | | Target records per second for `npm run bulk` |
| `RUN_ID` | Random UUID | | Marker in bulk messages and attributes; letters, digits, `_` and `-` |

Start with 250 records per batch. Measure delivery latency and request sizes, then try 500 or 1,000 if your records are small. For a quiet service, lower `BATCH_INTERVAL_MS` if waiting a second before sending a partial batch is too long. A bigger queue absorbs short bursts but doesn't raise how much the transport can deliver over time.

## Delivery and limits

Each process gets one transport worker thread, which sends one HTTP request at a time and keeps records in order. There's no concurrency option. Serializing records still costs time on the main thread. Total traffic and memory grow with the number of processes, so never create a transport per request or per record.

### Batch size

`BATCH_SIZE` counts records. The transport also splits batches at its `maxBatchBytes` limit, 1 MiB by default, counting the JSON brackets and commas. It doesn't compress requests. The server's default limits:

| Limit | Default |
| --- | --- |
| Events per request (`max_events`) | 1,000 |
| Request body (`max_body_bytes`) | 1 MiB (1,048,576 bytes) |
| One serialized event (`max_event_bytes`) | 64 KiB |
| Each string value or key (`max_field_bytes`) | 4,096 bytes |
| Nesting depth (`max_metadata_depth`) | 16 |

A batch of *n* events is a compact JSON array: 2 bytes of brackets, the event sizes and *n* - 1 commas. Measure serialized UTF-8 bytes, including escaping, Pino's automatic fields and child-logger bindings. For example, 500 events of 1 KiB make a request of about 501 KiB, and 250 events of 8 KiB are split across requests to stay under 1 MiB.

The transport's byte limit matches the server's default body limit, but it doesn't check the server's event, field or nesting limits, and the example logger doesn't limit record size. The transport drops a single record too big for one request and prints a warning. If your server has a smaller body limit, set `maxBatchBytes` in [logger.mjs](logger.mjs) to match. Otherwise trim large fields, or raise the server's limits in its TOML file together with its memory budgets (see the [operations guide](../../docs/OPERATIONS.md)). One invalid or oversized event rejects its whole batch, and retrying won't fix it.

### Queue overflow

`MAX_BUFFER_SIZE` caps the number of waiting records, and the transport's `maxBufferBytes` caps their serialized size at 64 MiB by default. The batch in flight, the encoded request and worker buffers use memory on top of that. When either cap is reached, the transport drops the oldest waiting records and warns on stderr. It doesn't slow your application down, and drops alone don't make shutdown fail. The example keeps these warnings on (`silent: false`). If the server can't keep up with your application, lower the log rate or remove logs you don't need.

### Retries and duplicates

pino-http-transport 2.1.0 retries network errors, timeouts and every non-2xx response, including permanent ones such as `401` and `413`. It ignores `Retry-After` and adds no jitter. When a batch runs out of retries, the transport reports the error, discards the batch and moves on to later ones, and `close()` then fails. Fix the cause (see [Troubleshooting](#troubleshooting)) instead of raising `MAX_RETRIES`.

The queue is only in memory: records still queued are lost if the process is killed or the shutdown drain runs out of time. Logbrook acknowledges a batch only after committing it, but if the response is lost, the retry stores the batch again. Logbrook doesn't deduplicate, and the `runId` and `sequence` attributes are for diagnosis, not idempotency keys. To ride out long outages, send logs through a collector with a persistent spool.

### Shutdown

`close()` waits for the transport to deliver everything, including partial batches and retries, then ends the worker and waits for it to exit. This relies on the delivery-aware flush in thread-stream 4.2.0, which `package.json` pins through `overrides`. A delivery error makes `close()` fail, and the worker is ended anyway. On success the helper doesn't call `process.exit()`.

Pino's worker `end()` blocks the main thread and gives the worker about 10 seconds to finish. Flushing first lets a slow backlog drain asynchronously before that, so request handling and timers keep running during delivery. The flush has no overall deadline; each HTTP attempt still uses `HTTP_TIMEOUT_MS`, so a long backlog can take a long time. A process manager that kills the process before delivery finishes still loses the queued records.

In a service:

1. Stop accepting new work.
2. Wait for in-flight requests to finish and stop producing logs.
3. Call `close()` and watch stderr for delivery errors.

Keep delivery ahead of production so the backlog at shutdown stays small, and give the process manager enough shutdown time.

## Troubleshooting

Transport messages go to stderr with a `[pino-http-transport]` prefix. A failed delivery also makes the example exit with a nonzero status; dropped records only print a warning.

| Error | Likely cause | What to do |
| --- | --- | --- |
| `HTTP 401` | Missing or wrong token, or a read or admin token | Use the server's ingestion token |
| `HTTP 403` | The token's index scope excludes this index | Use an allowed index or update `[ingest_index_scopes]` |
| `HTTP 400` | Invalid event: no `msg`, a non-numeric level or time, or a `time` outside the retention window or more than five minutes ahead | Keep Pino's default formats and check the host clock |
| `HTTP 413` | Too many events, or a request, event or field over the limits above | Lower `BATCH_SIZE` or trim large fields |
| `HTTP 429` | The server is at capacity, or creating the index would exceed its index limit | Reduce the rate or add capacity; check `logbrook indexes list` |
| `HTTP 507` | The index reached its disk-size target | Review the index's retention and size settings |
| `HTTP request timed out` | The server took longer than `HTTP_TIMEOUT_MS` to respond | Check server load before raising the timeout |
| `Buffer limit exceeded` | The queue was full and the oldest records were dropped | Lower the log rate or send larger batches |

## Tests

```sh
npm test
```

The tests run the real Pino worker and HTTP transport in child processes against a local mock receiver, so they need no Logbrook server and don't read `.env`. They cover batching, partial batches, field preservation, retries, permanent failures, configuration errors, signal-driven shutdown and a slow backlog that takes more than ten seconds to drain. To test real ingestion, run the examples against your server and use the commands in [Check the records](#check-the-records). The [query cookbook](../../docs/QUERY-EXAMPLES.md) has more searches and exports.
