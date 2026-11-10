import { FIXTURE_INDEX, ingestFixtures } from './support/fixtures.ts';
import { INGEST_SOURCE, TOKENS, ingest, startLogbrook } from './support/logbrook-server.ts';

/**
 * Pause between the live events written for trying the tail, in milliseconds.
 */
const LIVE_EVENT_INTERVAL_MILLIS = 2_000;

/**
 * Starts a throwaway Logbrook with sample data and prints how to point the UI at it.
 *
 * Run it with `pnpm dev:logbrook`. It ingests the integration fixtures, then writes one new event
 * every two seconds so the live tail has something to show. Stop it with Ctrl-C (or `SIGTERM`); the
 * data directory is deleted.
 */
async function main(): Promise<void> {
  const server = await startLogbrook();
  await ingestFixtures(server);
  console.log(`Logbrook:  ${server.url}`);
  console.log(`Index:     ${FIXTURE_INDEX} (source ${INGEST_SOURCE})`);
  console.log(`Read:      ${TOKENS.read}`);
  console.log(`Admin:     ${TOKENS.admin}`);
  const environment = `LOGBROOK_URL=${server.url} LOGBROOK_READ_TOKEN=${TOKENS.read}`;
  console.log(`Start the UI with:  ${environment} LOGBROOK_ADMIN_TOKEN=${TOKENS.admin} pnpm dev`);
  console.log(`Without index management, leave the admin token out:  ${environment} pnpm dev`);
  console.log('(A built server takes the same variables: replace "pnpm dev" with "pnpm start".)');

  let sequence = 0;
  const timer = setInterval(() => {
    sequence += 1;
    const event = {
      time: Date.now(),
      level: 30,
      msg: `live event ${sequence}`,
      service: 'checkout',
    };
    ingest(server, FIXTURE_INDEX, [event]).catch((error: unknown) => console.error(error));
  }, LIVE_EVENT_INTERVAL_MILLIS);

  for (const signal of ['SIGINT', 'SIGTERM'] as const) {
    process.once(signal, () => {
      clearInterval(timer);
      server.stop().then(
        () => process.exit(0),
        () => process.exit(1),
      );
    });
  }
}

await main();
