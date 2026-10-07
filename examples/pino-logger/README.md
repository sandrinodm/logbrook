# Send Pino logs to Logbrook

A runnable Node.js example that uses [Pino](https://getpino.io) and [`pino-http-transport`](https://github.com/sandrinodm/pino-http-transport) to send batches of structured logs to a named Logbrook index. It covers ordinary structured logging, child loggers, error serialization, a paced high-volume producer, and clean shutdown.

- [Requirements](#requirements)
- [Quick start](#quick-start)
- [Use it in your application](#use-it-in-your-application)
- [Send larger volumes](#send-larger-volumes)
- [Settings](#settings)
- [Delivery behavior and limits](#delivery-behavior-and-limits)
- [Troubleshooting](#troubleshooting)
- [Tests](#tests)

## Requirements

- Node.js 24 or later.
- A running Logbrook server and its **ingestion token**. Start one with the [root quick start](../../README.md#quick-start) or [Docker Compose](../../README.md#run-with-docker-compose). Read and admin tokens cannot ingest.
- To verify the results: the server's **read token** and the `logbrook` CLI.

Dependencies are pinned in `package-lock.json`: Pino 10.4.0 and pino-http-transport 2.1.0.

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

`npm start` runs [basic.mjs](basic.mjs), which logs four records (a startup message, a completed checkout, a slow-provider warning, and a payment error) and then closes the transport, which sends the final partial batch. [logger.mjs](logger.mjs) contains the shared logger setup and shutdown helper.

The transport sends `POST /indexes/pino-demo/logs/ingest` with a `Bearer` authorization header and `Content-Type: application/json`. If `pino-demo` does not exist yet, the first write creates it, provided the token is allowed to write to it and the server has a free index slot. Servers allow four indexes by default, including `default`.

### Use a .env file instead

```sh
cp .env.example .env
```

Fill in `LOGBROOK_INGEST_TOKEN` in `.env`. `npm start` and `npm run bulk` load `.env` if it exists, but **variables already set in your shell take precedence** over values in the file. Unset shell variables you want `.env` to control. The repository's `.gitignore` excludes `.env`.

`LOGBROOK_URL` is the server's base URL. It may include a reverse-proxy path prefix, such as `https://logs.example.com/logbrook`, but not credentials, a query string, or a fragment. The example appends the ingestion path itself. Use HTTPS for any server that is not on your machine.

### Verify the records

Use a shell that has the read token. The `logbrook` CLI does **not** load this example's `.env`, so export the same URL and index you used above:

```sh
export LOGBROOK_URL=http://127.0.0.1:3100
export LOGBROOK_INDEX=pino-demo
export LOGBROOK_READ_TOKEN='<your read token>'

logbrook query "$LOGBROOK_INDEX" --since 15m --service checkout --json
logbrook count "$LOGBROOK_INDEX" --since 15m --service checkout
```

If `logbrook` is not on your `PATH`, define a shell function that points at your build. From this directory:

```sh
logbrook() { ../../target/release/logbrook "$@"; }
```

After one run against a new index, the count is `4`. Each additional run appends four more records.

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

This short script closes immediately because its work is done. In a server, call `close()` only after you stop accepting new work and in-flight requests finish. `createLogger()` also returns `throwIfFailed()`, which throws if the transport has reported an error; [bulk.mjs](bulk.mjs) uses it to stop producing after a failure.

Logbrook maps Pino's fields like this:

| Pino field | Logbrook field |
| --- | --- |
| `time`, `level`, `msg` | `event_time`, `level`, `message` |
| `service`, `name`, `hostname`, `pid` | `service`, `logger`, `host`, `pid` |
| `requestId`, `orderId`, `durationMs`, `err`, and any other field | Inside `attributes` |
| (The ingestion token) | `source`; a payload field cannot override it |

The example sets `name` to `pino-example` and adds `service` from `LOG_SERVICE`. Keep Pino's defaults for the fields Logbrook requires:

- **Numeric levels.** Do not format levels as labels such as `"info"`.
- **Unix-millisecond timestamps.** Do not switch to ISO time strings.
- **A message on every call.** Pino omits `msg` when you log a plain object without a message string, and Logbrook rejects events without one.

Errors go through Pino's `err` serializer, which records the type, message, and stack. Each string value, including a stack trace, must fit within the server's 4,096-byte default field limit, or the whole batch is rejected. Log selected fields rather than whole request bodies or headers, and use Pino's [redaction](https://getpino.io/#/docs/redaction) for anything sensitive.

## Send larger volumes

[bulk.mjs](bulk.mjs) produces a finite run of small records, each tagged with a `runId` and an increasing `sequence` attribute:

```sh
# Default: 10,000 records at about 1,000 records per second.
npm run bulk

# 100,000 records at about 5,000 per second in batches of 500, with a known run ID.
export RUN_ID="bulk-$(date +%s)"
LOG_COUNT=100000 LOG_RATE=5000 BATCH_SIZE=500 npm run bulk
```

The script prints a JSON summary with the `runId`, the number of records `produced`, whether it was interrupted, and the elapsed time. `produced` counts logger calls, not records stored by Logbrook. The larger run aims for about 20 seconds of production plus startup and drain time. That is a pacing target, not a throughput guarantee.

To check what arrived, use the verification shell from the quick start and the same run ID:

```sh
export RUN_ID='<runId printed by npm run bulk>'

logbrook count "$LOGBROOK_INDEX" --since 1h --message "bulk example run=$RUN_ID"
logbrook query "$LOGBROOK_INDEX" --since 1h --message "bulk example run=$RUN_ID" --limit 10 --json
```

Make sure the time window covers the whole run. If nothing was dropped, expired, or delivered twice, the count is exactly `100000`. A higher count points to duplicates from retries or a reused run ID. A lower count means you should check the transport's stderr output, the server logs, and the index's retention.

The generator logs at most 100 records at a time, then sleeps to hold the target rate, so it never builds the whole run in memory. Ctrl-C or SIGTERM stops production and then drains the transport normally.

## Settings

All settings are environment variables. The example deliberately overrides several of the transport's own defaults:

| Variable | Example default | Transport default | Purpose |
| --- | --- | --- | --- |
| `LOGBROOK_URL` | `http://127.0.0.1:3100` | | Server base URL |
| `LOGBROOK_INDEX` | `pino-demo` | | Destination index |
| `LOGBROOK_INGEST_TOKEN` | Required | | Ingestion credential |
| `LOG_SERVICE` | `checkout` | | Value of the `service` field |
| `BATCH_SIZE` | `250` | `100` | Records per HTTP request; the example allows 1 to 1,000 |
| `BATCH_INTERVAL_MS` | `1000` | `5000` | Delay before sending a partial batch; a request in flight can delay it further |
| `MAX_BUFFER_SIZE` | `10000` | `100000` | Waiting records, excluding the batch in flight; at least `BATCH_SIZE` |
| `HTTP_TIMEOUT_MS` | `1500` | `2500` | Timeout for each HTTP attempt |
| `MAX_RETRIES` | `2` | `2` | Retries after the first attempt; 0 to 10 |
| `RETRY_DELAY_MS` | `250` | `1000` | First retry delay; doubles each retry, capped at the timeout |
| `LOG_COUNT` | `10000` | | Records produced by `npm run bulk` |
| `LOG_RATE` | `1000` | | Target records per second for `npm run bulk` |
| `RUN_ID` | Random UUID | | Marker in bulk messages and attributes; letters, digits, `_`, `-` |

Start with 250 records per batch. Measure delivery latency and request sizes, then try 500 or 1,000 if your records are small. For low-volume services, lower `BATCH_INTERVAL_MS` if a one-second delay before partial batches is too long. A larger queue absorbs short bursts but does not increase sustained delivery capacity.

## Delivery behavior and limits

The transport is simple by design. Knowing its limits helps you choose settings and decide whether you need more durable forwarding.

### One worker, one request at a time

The transport runs in a Pino worker thread, but serializing log records still costs time on the main thread. Each transport sends **one HTTP request at a time** and preserves record order; there is no concurrency option. Every application process has its own worker and queue, so total traffic and memory grow with the number of processes. Never create a transport per request or per record.

### Batches respect record and byte limits

`BATCH_SIZE` is a **record count**. The transport also splits batches at its default `maxBatchBytes` limit of 1 MiB, including JSON array brackets and commas. It does not compress requests. A Logbrook server enforces these limits by default:

| Limit | Default |
| --- | --- |
| Events per request (`max_events`) | 1,000 |
| Request body (`max_body_bytes`) | 1 MiB (1,048,576 bytes) |
| One serialized event (`max_event_bytes`) | 64 KiB |
| Each string value or key (`max_field_bytes`) | 4,096 bytes |
| Nesting depth (`max_metadata_depth`) | 16 |

A batch of *n* events is a compact JSON array, so its size is 2 bytes for the brackets, plus the sum of the event sizes, plus *n* - 1 commas. Measure serialized UTF-8 bytes, including escaping, Pino's automatic fields, and child-logger bindings. For example:

- 500 events of 1 KiB each make a request of about 501 KiB.
- 250 events of 8 KiB each are split across requests to stay within 1 MiB.
- 1,000 events of exactly 1 KiB each fit, with little room for variation.

The bulk generator uses small synthetic records. Your application's records may be much larger, and the shared logger does not limit their size. The transport's byte limit matches the server's default request-body limit, but it does not enforce the server's smaller event, field, or nesting limits. An individual record larger than the transport's request limit is dropped with a warning. If the server uses a smaller request-body limit, set `maxBatchBytes` in [logger.mjs](logger.mjs) to match it. Trim large fields or change the server's limits in its TOML file together with its memory budgets; see the [operations guide](../../docs/OPERATIONS.md). One invalid or oversized event rejects its entire batch, and retrying cannot fix it.

### Queue overflow drops the oldest records

`MAX_BUFFER_SIZE` limits the number of **waiting records**. The transport also caps their serialized UTF-8 bytes with its default `maxBufferBytes` limit of 64 MiB; the batch in flight, the serialized request, and worker buffers use memory too. When either queue limit is reached, the transport drops the **oldest waiting records** and writes a warning to stderr. It does not slow down your application, and drops alone do not make shutdown fail. This example keeps those warnings enabled.

Pacing your producer reduces bursts but cannot guarantee delivery when the server is slower than your application. Lower the log rate, remove unnecessary logs, or add server capacity after measuring.

### Retries

pino-http-transport 2.1.0 retries network errors, timeouts, and **every non-2xx response**, including permanent errors such as `401` and `413`. It does not honor `Retry-After` and adds no jitter. When a batch runs out of retries, the transport reports the error, discards that batch, and continues with later batches; `close()` then fails. Fix the cause shown in [Troubleshooting](#troubleshooting) rather than raising `MAX_RETRIES`.

### No durable spool or exactly-once delivery

The queue lives only in memory. Records still queued are lost if the process is killed or the shutdown drain runs out of time. Logbrook acknowledges a batch only after committing it, but if a response is lost, the retry stores that batch again. There is no deduplication. The `runId` and `sequence` attributes help you diagnose a run; Logbrook does not use them as idempotency keys. If you need delivery through extended outages, forward logs through a persistent spool or collector.

### Shutdown flushes delivery before ending the worker

The `close()` helper first waits for the transport's delivery flush, including partial batches and retries, then ends the worker and waits for it to close. The pinned thread-stream 4.2.0 supports this delivery-aware flush. Delivery errors make `close()` fail, and the helper still ends the worker after a failed flush. It never calls `process.exit()` on success.

Pino's worker `end()` blocks the main thread and has a shutdown budget of about 10 seconds. Flushing before `end()` lets a slow backlog drain asynchronously before that budget starts, so request handling and timers can continue during delivery. The flush has no overall deadline; each HTTP attempt still uses `HTTP_TIMEOUT_MS`, and draining many queued batches can take a long time. A process manager that kills the process before delivery finishes can still lose queued records.

In a service:

1. Stop accepting new work.
2. Wait for in-flight requests to finish and stop producing logs.
3. Call `close()` and watch stderr for delivery errors.

Keep normal delivery ahead of production so the backlog at shutdown stays small, and allow sufficient shutdown time in your process manager.

## Troubleshooting

Transport messages appear on stderr, prefixed with `[pino-http-transport]`. A failed delivery also makes the example exit with a nonzero status; dropped records only produce a warning.

| Error | Likely cause | What to do |
| --- | --- | --- |
| `HTTP 401` | Missing or wrong token, or a read or admin token | Use the server's ingestion token |
| `HTTP 403` | The token's index scope excludes this index | Use an allowed index or update `[ingest_index_scopes]` |
| `HTTP 400` | Invalid event: no `msg`, a non-numeric level or time, or a `time` outside the retention window or more than five minutes ahead | Keep Pino's default formats and check the host clock |
| `HTTP 413` | Too many events, or a request, event, or field over the limits above | Lower `BATCH_SIZE` or trim large fields |
| `HTTP 429` | The server is at capacity, or creating the index would exceed its index limit | Reduce the rate or add capacity; check `logbrook indexes list` |
| `HTTP 507` | The index reached its disk-size target | Review the index's retention and size settings |
| `HTTP request timed out` | The server responded slower than `HTTP_TIMEOUT_MS` | Check server load before raising the timeout |
| `Buffer limit ... exceeded` | The queue was full and the oldest records were dropped | Reduce the log rate or batch more efficiently |

## Tests

```sh
npm test
```

The tests run the real Pino worker and HTTP transport in child processes against a local mock receiver, so they need no Logbrook server and do not read `.env`. They cover batching, partial batches, field preservation, retries, permanent failures, configuration errors, signal-driven shutdown, and a slow backlog that takes more than ten seconds to drain. To check real ingestion, run the examples against your server and use the verification commands above. The [query cookbook](../../docs/QUERY-EXAMPLES.md) has more searches and exports.
