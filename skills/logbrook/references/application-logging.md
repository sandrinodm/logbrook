# Send application logs

## Select the index and identity

Use the intended server URL, an ingestion credential and an index such as `apps`. Several applications can share the index with different `service` values. Use separate indexes when retention or access policies differ. The first authorized write creates a missing index if a slot is free; reads never create one.

The token sets `source`, and a payload can't override it. For separate producer credentials or index scopes, configure the token maps in the server's TOML file. Send tokens only from trusted backends or collectors, never from JavaScript served to browsers.

## Event and batching contract

POST to `/indexes/{index}/logs/ingest` with `Authorization: Bearer <ingestion token>` and either:

- `application/json`: a nonempty array, even for one event.
- `application/x-ndjson`: one JSON object per line, without an enclosing array.

Every event needs `time` as integer Unix milliseconds, `level` as a nonnegative integer and `msg` as a string. Pino uses levels 10, 20, 30, 40, 50 and 60 for trace, debug, info, warn, error and fatal. Optional `service`, `name` and `hostname` map to `service`, `logger` and `host`. Other top-level fields such as `requestId`, `durationMs` and `statusCode` come back under `attributes`; don't wrap them in an input `attributes` object unless you want the extra level of nesting.

The default server limits are 1,000 events and 1 MiB per batch, 64 KiB per event, 4 KiB per field or key, and a nesting depth of 16. Limit batches by serialized UTF-8 bytes as well as record count. One invalid event rejects its whole batch. Event timestamps must fall within retention and, by default, no more than five minutes in the future.

## Verify ingestion

Set `LOGBROOK_URL`, `LOGBROOK_INDEX` and the configured ingestion and read credentials in the shell. For the bundled deployment, use `http://127.0.0.1:3100` and index `apps`, and load the deployment's `.env` instead of creating new tokens. This probe writes one event:

```sh
export LOGBROOK_URL=http://127.0.0.1:3100
export LOGBROOK_INDEX=apps
export LOGBROOK_PROBE="logbrook-setup-$(openssl rand -hex 8)"

jq -n --arg marker "$LOGBROOK_PROBE" \
  '[{time: (now * 1000 | floor), level: 30, service: "setup-check", msg: $marker}]' |
  curl --fail-with-body --silent --show-error \
    "${LOGBROOK_URL%/}/indexes/$LOGBROOK_INDEX/logs/ingest" \
    -H "Authorization: Bearer $LOGBROOK_INGEST_TOKEN" \
    -H 'Content-Type: application/json' --data-binary @-

probe_to=$(jq -nr 'now * 1000 | floor')
curl --fail-with-body --silent --show-error --get \
  "${LOGBROOK_URL%/}/indexes/$LOGBROOK_INDEX/logs" \
  -H "Authorization: Bearer $LOGBROOK_READ_TOKEN" \
  --data-urlencode "from=$((probe_to - 600000))" \
  --data-urlencode "to=$probe_to" \
  --data-urlencode "message=$LOGBROOK_PROBE" | jq .
```

Expect `{"accepted":1}`, then the marked event in `events`. Returned events use Logbrook's field names, such as `event_time` and `message`. To check persistence, run the same query after restarting the server, while the event is still inside the window.

## Pino / Node.js

Start from the [Pino example](https://github.com/sandrinodm/logbrook/tree/main/examples/pino-logger). It has a logger factory, a transport with byte-limited batches, a bulk generator and a shutdown that waits for delivery. In a checkout, run:

```sh
cd examples/pino-logger
npm ci
export LOGBROOK_URL=http://127.0.0.1:3100
export LOGBROOK_INDEX=apps
export LOG_SERVICE=checkout
# LOGBROOK_INGEST_TOKEN must already contain the server's configured credential.
npm start
```

Use the server URL that the application's network can reach. The example loads `.env` from its directory if the file exists, and exported shell variables override it. It needs only the ingestion credential; keep read credentials in the verification shell.

For an existing app, copy the example's `logger.mjs` and pinned dependency versions into the app's own package structure. Don't replace the app's logging stack just to run the example. Create one transport per process, share it through child loggers, and keep the factory's error handling and `close()` helper:

```js
import { createLogger } from './logger.mjs';

const { logger, close } = await createLogger();

try {
  const requestLog = logger.child({ requestId: 'req-123' });
  requestLog.info({ durationMs: 42, statusCode: 200 }, 'Checkout completed');
  requestLog.error({ err: new Error('Payment declined') }, 'Payment failed');
} finally {
  await close();
}
```

In a long-running server, call `close()` only after stopping new work and finishing active requests. Keep numeric levels, Unix-millisecond timestamps and a message string on every call. Pick and redact sensitive fields before logging; long stack traces and request bodies can exceed the field limit.

Start with the example's 250-record batches and one-second flush interval. `BATCH_SIZE=500 LOG_COUNT=100000 LOG_RATE=5000 npm run bulk` sends synthetic load; run it only against an index the user can throw away. Its `produced` count is logger calls, so check delivery with a server-side count of the printed run marker.

The example's transport keeps a bounded queue in memory, drops the oldest waiting records when it overflows and has no persistent spool. The pinned version retries network failures and every non-2xx response, and ignores `Retry-After`. Shutdown reports delivery errors, but a lost response followed by a retry still stores duplicates. When writing a new sender, back off on `429`, cap retries and don't retry a batch that is permanently invalid. Use a collector with a persistent spool when the user can't afford to lose logs during an outage.

## Troubleshooting delivery

| Result | Check |
| --- | --- |
| `400` | Numeric time/level, required message, event age, nesting, and field shapes |
| `401` / `403` | Ingestion credential role, source configuration, and allowed index |
| `413` | Serialized batch, event, and field sizes, not only record count |
| `429` | Server at capacity or no free index slot; reduce load and honor `Retry-After` |
| `507` | Per-index storage target and retention progress |
| Fewer records than produced | Sender drops, delivery errors, incomplete shutdown, retention, or an overly narrow query window |
| More records than produced | Retried uncertain commits or a reused verification marker |

For other languages, implement the same HTTP contract with the application's existing logger or collector. Verify one real application event, then a bounded batch and a normal shutdown, before increasing volume.
