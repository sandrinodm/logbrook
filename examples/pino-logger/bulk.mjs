import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { createLogger, integerSetting } from './logger.mjs';

try {
  const count = integerSetting(process.env, 'LOG_COUNT', 10_000);
  const rate = integerSetting(process.env, 'LOG_RATE', 1000, 1, 1_000_000);
  const runId = process.env.RUN_ID ?? randomUUID();
  if (!/^[a-zA-Z0-9_-]{1,80}$/.test(runId)) {
    throw new Error('RUN_ID must contain 1–80 letters, digits, underscores or hyphens');
  }

  const logging = await createLogger();
  const logger = logging.logger.child({ runId });
  const message = `bulk example run=${runId}`;

  // Small bursts yield to the main event loop. This is pacing, not delivery backpressure.
  const chunkSize = Math.max(1, Math.min(100, Math.floor(rate / 10)));
  let stopping = false;
  let produced = 0;

  // Signals stop production; the finally block still drains queued records.
  const stop = () => {
    stopping = true;
  };
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);

  const started = performance.now();
  console.log(`Producing up to ${count} records at approximately ${rate}/s; runId=${runId}`);

  try {
    while (produced < count && !stopping) {
      logging.throwIfFailed();

      const chunkStart = performance.now();
      const end = Math.min(count, produced + chunkSize);
      const size = end - produced;

      for (; produced < end; produced += 1) {
        logger.info({ sequence: produced, durationMs: produced % 1500 }, message);
      }

      if (produced < count) {
        // Never catch up with an unbounded burst after a slow loop iteration.
        await sleep(Math.max(1, (size * 1000) / rate - (performance.now() - chunkStart)));
      }
    }
  } finally {
    try {
      await logging.close();
    } finally {
      process.off('SIGINT', stop);
      process.off('SIGTERM', stop);
    }
  }

  console.log(
    JSON.stringify({
      runId,
      produced,
      interrupted: stopping,
      elapsedSeconds: Number(((performance.now() - started) / 1000).toFixed(3)),
    }),
  );
  console.log(
    'Produced is not a stored count. Verify this run in Logbrook; queue overflow can discard records.',
  );
} catch (error) {
  console.error(`Bulk example failed: ${error.message}`);
  process.exitCode = 1;
}
