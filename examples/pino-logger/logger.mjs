import { once } from 'node:events';
import pino from 'pino';

export function integerSetting(env, name, fallback, min = 1, max = 2_147_483_647) {
  const raw = env[name] ?? String(fallback);
  const value = Number(raw);

  // Reject coercible values such as empty strings, fractions, and exponent notation.
  if (!/^\d+$/.test(raw) || !Number.isSafeInteger(value) || value < min || value > max) {
    throw new Error(`${name} must be an integer between ${min} and ${max}`);
  }

  return value;
}

export async function createLogger(env = process.env) {
  const token = env.LOGBROOK_INGEST_TOKEN;
  if (!token || /[\r\n]/.test(token)) {
    throw new Error(
      'Set LOGBROOK_INGEST_TOKEN to the ingestion credential configured on your server',
    );
  }

  let base;
  try {
    base = new URL(env.LOGBROOK_URL ?? 'http://127.0.0.1:3100');
  } catch {
    throw new Error('LOGBROOK_URL must be a valid HTTP(S) server URL');
  }

  if (
    !['http:', 'https:'].includes(base.protocol) ||
    base.username ||
    base.password ||
    base.search ||
    base.hash
  ) {
    throw new Error('LOGBROOK_URL must use HTTP(S) without credentials, a query, or a fragment');
  }

  const index = env.LOGBROOK_INDEX ?? 'pino-demo';
  if (!/^[a-z0-9][a-z0-9_-]{0,62}$/.test(index)) {
    throw new Error(
      'LOGBROOK_INDEX must be 1–63 lowercase letters, digits, underscores or hyphens, starting with a letter or digit',
    );
  }

  const service = env.LOG_SERVICE ?? 'checkout';
  if (!service || Buffer.byteLength(service) > 4096) {
    throw new Error('LOG_SERVICE must be a nonempty string of at most 4096 UTF-8 bytes');
  }

  const batchSize = integerSetting(env, 'BATCH_SIZE', 250, 1, 1000);
  const maxBufferSize = integerSetting(env, 'MAX_BUFFER_SIZE', 10_000);
  if (maxBufferSize < batchSize) {
    throw new Error('MAX_BUFFER_SIZE must be at least BATCH_SIZE');
  }

  // Create one worker per application, shared by all child loggers.
  const transport = pino.transport({
    target: 'pino-http-transport',
    options: {
      url: `${base.href.replace(/\/$/, '')}/indexes/${index}/logs/ingest`,
      headers: { authorization: `Bearer ${token}` },
      batchSize,
      batchInterval: integerSetting(env, 'BATCH_INTERVAL_MS', 1000),
      maxBufferSize,
      timeout: integerSetting(env, 'HTTP_TIMEOUT_MS', 1500),
      maxRetries: integerSetting(env, 'MAX_RETRIES', 2, 0, 10),
      retryDelay: integerSetting(env, 'RETRY_DELAY_MS', 250, 0),
      silent: false,
    },
  });

  // Keep the first worker error, including errors arriving after startup.
  let failure;
  transport.on('error', (error) => {
    failure ??= error;
  });

  await once(transport, 'ready');

  // Keep short-lived scripts alive until close() completes.
  transport.ref();

  const logger = pino({ name: 'pino-example', level: 'info' }, transport).child({ service });
  let closing;

  return {
    logger,

    throwIfFailed() {
      if (failure) {
        throw failure;
      }
    },

    close() {
      // Share shutdown work so repeated calls cannot end the worker twice.
      closing ??= (async () => {
        if (!transport.closed) {
          // Deliver the backlog before end() starts its blocking shutdown budget.
          try {
            await new Promise((resolve, reject) => {
              transport.flush((error) => {
                if (error) {
                  reject(error);
                } else {
                  resolve();
                }
              });
            });
          } finally {
            // Always release the worker, including after a failed delivery flush.
            if (!transport.closed) {
              const closed = once(transport, 'close');
              transport.end();
              await closed;
            }
          }
        }

        if (failure) {
          throw failure;
        }
      })();

      return closing;
    },
  };
}
