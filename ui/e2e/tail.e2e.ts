import { afterAll, beforeAll, describe, expect, test } from 'e2e';

import type { BffServer } from './support/bff-server.ts';
import type { LogbrookServer } from './support/logbrook-server.ts';
import type { SseFrame, SseReader } from './support/sse.ts';

import { startBff } from './support/bff-server.ts';
import { eventually, teardown, withTimeout } from './support/helpers.ts';
import { MAX_TAIL_SUBSCRIBERS, ingest, startLogbrook, TOKENS } from './support/logbrook-server.ts';
import { readSse } from './support/sse.ts';

/**
 * Index the tail tests use.
 */
const TAIL_INDEX = 'tail-it';

/**
 * How long to wait for a frame that should arrive within a poll or two, in milliseconds. A proxy
 * that buffers the stream would not deliver at all, so the margin can be generous.
 */
const FRAME_TIMEOUT_MILLIS = 5_000;

let logbrook: LogbrookServer;
let bff: BffServer;

beforeAll(async () => {
  logbrook = await startLogbrook();
  await ingest(logbrook, TAIL_INDEX, [{ time: Date.now() - 5000, level: 30, msg: 'seed' }]);
  bff = await startBff(logbrook.url, { read: TOKENS.read });
});

afterAll(async () => {
  await teardown(
    () => bff?.stop(),
    () => logbrook?.stop(),
  );
});

/**
 * Opens the tail through the BFF. The returned controller aborts it like a browser closing the tab.
 */
async function openTail(headers: Record<string, string> = {}) {
  const controller = new AbortController();
  const response = await fetch(`${bff.url}/api/lb/indexes/${TAIL_INDEX}/logs/tail`, {
    headers,
    signal: controller.signal,
  });
  return { response, controller };
}

/**
 * Reads frames until one satisfies `predicate`, failing if none arrives in time.
 */
async function nextMatching(
  sse: SseReader,
  predicate: (frame: SseFrame) => boolean,
  label: string,
): Promise<SseFrame> {
  const search = (async () => {
    for (;;) {
      const frame = await sse.next();
      if (frame === null) {
        throw new Error(`Stream ended while waiting for ${label}`);
      }

      if (predicate(frame)) {
        return frame;
      }
    }
  })();
  return withTimeout(search, FRAME_TIMEOUT_MILLIS, label);
}

/**
 * Whether a frame is a `log` frame carrying the message.
 */
function isLogWith(message: string): (frame: SseFrame) => boolean {
  return (frame) => frame.event === 'log' && frame.data.includes(`"message":"${message}"`);
}

/**
 * Reads the number of active tail subscribers from Logbrook's metrics.
 */
async function activeSubscribers(): Promise<number> {
  const response = await fetch(`${logbrook.url}/metrics`, {
    headers: { authorization: `Bearer ${TOKENS.read}` },
  });
  const line = (await response.text())
    .split('\n')
    .find((text) => text.startsWith('logbrook_tail_active_subscribers '));
  return Number(line?.split(' ')[1]);
}

describe('live tail through the proxy', () => {
  test('delivers frames as they happen, without buffering', async () => {
    const { response, controller } = await openTail();
    expect(response.status).toBe(200);
    expect(response.headers.get('content-type')).toContain('text/event-stream');
    expect(response.headers.get('content-encoding')).toBeNull();
    const sse = readSse(response);

    await nextMatching(sse, (frame) => frame.event === 'cursor', 'caught-up cursor frame');
    await ingest(logbrook, TAIL_INDEX, [{ time: Date.now(), level: 30, msg: 'live-1' }]);
    const frame = await nextMatching(sse, isLogWith('live-1'), 'log frame for live-1');

    expect(frame.id).toMatch(new RegExp(`^${TAIL_INDEX}:`));
    controller.abort();
    await eventually(async () => (await activeSubscribers()) === 0, 5_000, 'subscriber released');
  });

  test('cancels the upstream subscription when the browser aborts', async () => {
    const { response, controller } = await openTail();
    const sse = readSse(response);
    await nextMatching(sse, (frame) => frame.event === 'cursor', 'caught-up cursor frame');

    expect(await activeSubscribers()).toBe(1);
    controller.abort();

    await eventually(async () => (await activeSubscribers()) === 0, 5_000, 'subscriber released');
  });

  test('does not leak subscribers across repeated open and abort', async () => {
    for (let round = 0; round < MAX_TAIL_SUBSCRIBERS + 3; round += 1) {
      const { response, controller } = await openTail();
      expect(response.status).toBe(200);
      await nextMatching(readSse(response), (frame) => frame.event === 'cursor', 'cursor frame');
      controller.abort();
      await eventually(async () => (await activeSubscribers()) === 0, 5_000, 'subscriber released');
    }
  });

  test('passes the subscriber limit through as 429 with Retry-After', async () => {
    const open = [];
    for (let index = 0; index < MAX_TAIL_SUBSCRIBERS; index += 1) {
      open.push(await openTail());
    }

    const refused = await fetch(`${bff.url}/api/lb/indexes/${TAIL_INDEX}/logs/tail`);

    expect(refused.status).toBe(429);
    expect(refused.headers.get('retry-after')).toBe('1');
    expect(await refused.json()).toEqual({ error: expect.any(String) });
    for (const connection of open) {
      connection.controller.abort();
    }

    await eventually(async () => (await activeSubscribers()) === 0, 5_000, 'subscribers released');
  });

  test('resumes after the last seen event with Last-Event-ID', async () => {
    const first = await openTail();
    const firstSse = readSse(first.response);
    await nextMatching(firstSse, (frame) => frame.event === 'cursor', 'caught-up cursor frame');
    await ingest(logbrook, TAIL_INDEX, [{ time: Date.now(), level: 30, msg: 'resume-A' }]);
    const seenA = await nextMatching(firstSse, isLogWith('resume-A'), 'log frame for resume-A');
    first.controller.abort();
    await eventually(async () => (await activeSubscribers()) === 0, 5_000, 'subscriber released');

    await ingest(logbrook, TAIL_INDEX, [{ time: Date.now(), level: 30, msg: 'resume-B' }]);
    const second = await openTail({ 'last-event-id': seenA.id ?? '' });
    const secondSse = readSse(second.response);
    const frames: SseFrame[] = [];
    const seenB = await nextMatching(
      secondSse,
      (frame) => {
        frames.push(frame);
        return isLogWith('resume-B')(frame);
      },
      'log frame for resume-B after resume',
    );
    second.controller.abort();

    expect(seenB.event).toBe('log');
    expect(frames.filter(isLogWith('resume-A'))).toHaveLength(0);
    expect(frames.filter(isLogWith('seed'))).toHaveLength(0);
  });

  test('answers 400 for a resume id that does not belong to the index', async () => {
    const response = await fetch(`${bff.url}/api/lb/indexes/${TAIL_INDEX}/logs/tail`, {
      headers: { 'last-event-id': 'other:v1.bogus' },
    });

    expect(response.status).toBe(400);
  });
});

describe('shutdown with an open tail', () => {
  /**
   * Longest a stop may take with a tail stream open, in milliseconds. Before the proxy ended its
   * streams on shutdown this took the whole 5 s grace period of Nitro's graceful shutdown.
   */
  const MAX_STOP_MILLIS = 2_500;

  test('exits promptly on SIGTERM and ends the stream, as docker stop needs', async () => {
    const own = await startBff(logbrook.url, { read: TOKENS.read });
    try {
      const controller = new AbortController();
      const response = await fetch(`${own.url}/api/lb/indexes/${TAIL_INDEX}/logs/tail`, {
        signal: controller.signal,
      });
      const sse = readSse(response);
      await nextMatching(sse, (frame) => frame.event === 'cursor', 'caught-up cursor frame');

      const started = Date.now();
      await own.stop();

      expect(Date.now() - started).toBeLessThan(MAX_STOP_MILLIS);
      // The stream ended (cleanly or with an error) instead of being left open.
      await withTimeout(
        sse.next().catch(() => null),
        FRAME_TIMEOUT_MILLIS,
        'stream to end',
      );
      await eventually(async () => (await activeSubscribers()) === 0, 5_000, 'subscriber released');
    } finally {
      await own.stop();
    }
  });
});
