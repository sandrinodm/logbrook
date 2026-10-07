import { createLogger } from './logger.mjs';

try {
  const { logger, close } = await createLogger();

  try {
    logger.info('Pino is connected to Logbrook');

    // Child loggers share the worker and HTTP batch queue.
    const requestLog = logger.child({ requestId: 'req-demo-1', orderId: 'ord-42' });
    requestLog.info({ durationMs: 42, statusCode: 200 }, 'Checkout completed');
    requestLog.warn({ durationMs: 1250 }, 'Payment provider was slow');
    requestLog.error({ err: new Error('Demo payment failure') }, 'Payment failed');
  } finally {
    // Flush the last partial batch, even though it is smaller than BATCH_SIZE.
    await close();
  }

  console.log(
    'Produced 4 records and closed the transport. Query the pino-demo index (or LOGBROOK_INDEX).',
  );
} catch (error) {
  // Do not log transport failures through the same failed transport.
  console.error(`Pino example failed: ${error.message}`);
  process.exitCode = 1;
}
