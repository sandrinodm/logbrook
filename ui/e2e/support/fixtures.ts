import type { LogbrookServer } from './logbrook-server.ts';

import { ingest } from './logbrook-server.ts';

/**
 * Index the fixtures are ingested into.
 */
export const FIXTURE_INDEX = 'itest';

/**
 * Number of fixture events. With the page size used in the paging test (500) this gives two full
 * pages and a partial third.
 */
export const FIXTURE_EVENT_COUNT = 1200;

/**
 * Events per ingest request. Logbrook accepts 1,000 per batch by default.
 */
const BATCH_SIZE = 600;

/**
 * Spacing between fixture events in milliseconds.
 */
const SPACING_MILLIS = 1000;

/**
 * Every n-th fixture event is an error (level 50); the rest are info (30).
 */
const ERROR_EVERY = 10;

/**
 * A time range, in Unix milliseconds, that covers every fixture event.
 */
export type FixtureRange = {
  /**
   * Inclusive start.
   */
  readonly from: number;

  /**
   * Exclusive end.
   */
  readonly to: number;
};

/**
 * Ingests `FIXTURE_EVENT_COUNT` events into `FIXTURE_INDEX`, one per second ending a minute ago,
 * alternating between two services. Returns a range that covers them.
 */
export async function ingestFixtures(server: LogbrookServer): Promise<FixtureRange> {
  const now = Date.now();
  const newest = now - 60_000;
  const events = Array.from({ length: FIXTURE_EVENT_COUNT }, (_, index) => ({
    time: newest - index * SPACING_MILLIS,
    level: index % ERROR_EVERY === 0 ? 50 : 30,
    msg: `fixture event ${index}`,
    service: index % 2 === 0 ? 'checkout' : 'billing',
    name: 'http',
    hostname: 'host-1',
    seq: index,
  }));

  for (let start = 0; start < events.length; start += BATCH_SIZE) {
    await ingest(server, FIXTURE_INDEX, events.slice(start, start + BATCH_SIZE));
  }

  return { from: newest - FIXTURE_EVENT_COUNT * SPACING_MILLIS - 60_000, to: now + 60_000 };
}
