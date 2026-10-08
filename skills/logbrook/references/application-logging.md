# Send application logs

## Select the index and identity

Use the intended server URL, an ingestion credential, and an index such as `apps`. Multiple applications can share that index and set distinct `service` values. Separate indexes suit different retention or access policies. First authorized ingestion creates a missing index if a slot is available; reads never create one.

The token supplies the canonical `source`; a payload cannot override it. For separate producer credentials or index scopes, configure the server's TOML token maps. Send tokens only from trusted application backends or collectors, not browser-delivered JavaScript.

## Event and batching contract

POST to `/indexes/{index}/logs/ingest` with `Authorization: Bearer <ingestion token>` and either:

- `application/json`: a nonempty array, even for one event.
- `application/x-ndjson`: one JSON object per line, without an enclosing array.

Every event needs `time` as integer Unix milliseconds, `level` as a nonnegative integer, and `msg` as a string. Pino levels conventionally use 10/20/30/40/50/60 for trace/debug/info/warn/error/fatal. Optional `service`, `name`, and `hostname` map to `service`, `logger`, and `host`. Additional top-level fields such as `requestId`, `durationMs`, and `statusCode` appear under returned `attributes`; avoid wrapping them in an input `attributes` object unless that extra nesting is intended.

Default server limits are 1,000 events and 1 MiB per batch, 64 KiB per event, 4 KiB per field/key, and depth 16. Bound batches by both serialized UTF-8 bytes and record count. One invalid event rejects its entire batch. Live event timestamps must fit retention and cannot be more than five minutes in the future by default.

## Verify ingestion

Set `LOGBROOK_URL`, `LOGBROOK_INDEX`, and the configured ingestion/read credentials in the shell. For the bundled deployment, use `http://127.0.0.1:3100` and index `apps`; load the trusted deployment `.env` rather than creating new tokens. This probe writes one event:

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

Expect `{"accepted":1}`, then the marked event in `events`. Returned fields use canonical names such as `event_time` and `message`. Run the same query after restarting the server to verify persistence while the event remains within the window.

## Pino / Node.js

Use the maintained [Pino example](https://github.com/sandrinodm/logbrook/tree/main/examples/pino-logger), which includes a logger factory, byte-bounded transport, finite load generator, and delivery-aware shutdown. In a checkout, run:

```sh
cd examples/pino-logger
npm ci
export LOGBROOK_URL=http://127.0.0.1:3100
export LOGBROOK_INDEX=apps
export LOG_SERVICE=checkout
# LOGBROOK_INGEST_TOKEN must already contain the server's configured credential.
npm start
```

Use the server URL reachable from that application's network. The example loads `.env` from its own working directory if present; exported shell values take precedence. It uses the ingestion credential only. Read credentials belong in the verification shell.

For an existing app, adapt the example's `logger.mjs` and pinned dependency versions into its normal package structure. Do not replace its logging stack merely to run the example. Create one transport per process, share it through child loggers, and preserve the factory's error handling and `close()` helper:

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

For a long-running server, close only after stopping new work and draining active requests. Keep numeric levels, Unix-millisecond timestamps, and a message string on each call. Select and redact sensitive fields before logging; unbounded stack traces and request bodies can exceed field limits.

Start with the example's 250-record batches and one-second flush interval. `BATCH_SIZE=500 LOG_COUNT=100000 LOG_RATE=5000 npm run bulk` is a finite synthetic load example for a disposable index, not a production verification step or a throughput promise. The generator's `produced` count counts logger calls; verify delivery with server-side counts and the printed run marker.

The example's transport has bounded in-memory queues, drops oldest waiting records on overflow, and has no durable spool. Its pinned transport retries all non-2xx responses as well as network failures; it does not honor `Retry-After`. Delivery-aware shutdown can report errors but does not make delivery exactly once. A lost response can produce duplicates after a retry. For a new sender, honor backpressure, use bounded retry budgets, and avoid retrying a permanently invalid batch. Use a persistent collector when the user's outage tolerance requires it.

## Troubleshooting delivery

| Result | Check |
| --- | --- |
| `400` | Numeric time/level, required message, event age, nesting, and field shapes |
| `401` / `403` | Ingestion credential role, source configuration, and allowed index |
| `413` | Serialized batch, event, and field sizes, not only record count |
| `429` | Server capacity or no free index slot; reduce pressure and honor `Retry-After` when present |
| `507` | Per-index storage target and retention progress |
| Fewer records than produced | Sender drops, delivery errors, incomplete shutdown, retention, or an overly narrow query window |
| More records than produced | Retried uncertain commits or a reused verification marker |

For other languages, implement this same HTTP contract using the application's existing logger or collector. Verify one real application event, then a bounded batch and normal shutdown before increasing volume.
