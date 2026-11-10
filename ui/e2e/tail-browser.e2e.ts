import type { Browser } from '@e2e-dev/web';
import type { Screen } from 'e2e';

import { describe, test } from '@e2e-dev/web';
import { afterAll, beforeAll, expect } from 'e2e';

import type { BffServer } from './support/bff-server.ts';
import type { LogbrookServer } from './support/logbrook-server.ts';

import { startBff } from './support/bff-server.ts';
import { teardown } from './support/helpers.ts';
import { ingest, startLogbrook, TOKENS } from './support/logbrook-server.ts';
import { pause, searchParams, pathname } from './support/page.ts';

/**
 * Index these tests tail.
 */
const BROWSER_INDEX = 'tail-browser';

/**
 * Events per ingest request. The server accepts 1,000 per batch by default.
 */
const BATCH_SIZE = 1_000;

/**
 * Events the burst test sends: more than the 10,000 the tail keeps, so the cap is exercised.
 */
const BURST_EVENTS = 12_000;

/**
 * How long a page may go between two animation frames during the burst, in milliseconds. A page
 * that renders every event blocks for seconds; a healthy one stays within a few frames. The limit
 * is far above that to keep slow CI machines from failing the test.
 */
const MAX_FRAME_GAP_MILLIS = 500;

let logbrook: LogbrookServer;
let bff: BffServer;
let bffScoped: BffServer;
let bffWrongToken: BffServer;

beforeAll(async () => {
  logbrook = await startLogbrook();
  await ingest(logbrook, BROWSER_INDEX, [{ time: Date.now() - 5_000, level: 30, msg: 'seed' }]);
  bff = await startBff(logbrook.url, { read: TOKENS.read });
  bffScoped = await startBff(logbrook.url, { read: TOKENS.scopedRead });
  bffWrongToken = await startBff(logbrook.url, { read: 'unknown-token-0123456789' });
});

afterAll(async () => {
  await teardown(
    () => bff?.stop(),
    () => bffScoped?.stop(),
    () => bffWrongToken?.stop(),
    () => logbrook?.stop(),
  );
});

/**
 * Viewport these tests use, which differs from the config's default.
 */
const VIEWPORT = { width: 1280, height: 800 };

/**
 * Opens the tail page and waits until the stream is live.
 */
async function openTail(browser: Browser, screen: Screen): Promise<void> {
  await browser.setViewport(VIEWPORT);
  await browser.goto(`${bff.url}/tail?index=${BROWSER_INDEX}`);
  await screen.getByText('Live').waitFor();
}

/**
 * Ingests `count` events with messages `burst 0`, `burst 1`, ... in batches.
 */
async function ingestBurst(count: number): Promise<void> {
  for (let sent = 0; sent < count; sent += BATCH_SIZE) {
    const batch = Array.from({ length: Math.min(BATCH_SIZE, count - sent) }, (_, offset) => ({
      time: Date.now(),
      level: 30,
      msg: `burst ${sent + offset}`,
    }));
    await ingest(logbrook, BROWSER_INDEX, batch);
  }
}

describe('live tail in a browser', () => {
  test('shows a newly ingested event', async ({ browser, screen }) => {
    await openTail(browser, screen);

    await ingest(logbrook, BROWSER_INDEX, [{ time: Date.now(), level: 30, msg: 'hello tail' }]);

    await screen.getByText('hello tail', { exact: false }).waitFor({ timeout: 10_000 });
    expect(await screen.getByText('hello tail', { exact: false }).count()).toBe(1);
  });

  test('stays responsive and keeps at most 10,000 events under a burst', async ({
    browser,
    screen,
  }) => {
    await openTail(browser, screen);
    await browser.evaluate(() => {
      const gaps = { max: 0 };
      let last = performance.now();
      const tick = (now: number) => {
        gaps.max = Math.max(gaps.max, now - last);
        last = now;
        requestAnimationFrame(tick);
      };
      requestAnimationFrame(tick);
      Object.assign(window, { frameGaps: gaps });
      return null;
    });

    await ingestBurst(BURST_EVENTS);

    await screen
      .getByText(`burst ${BURST_EVENTS - 1}`, { exact: false })
      .waitFor({ timeout: 30_000 });
    await expect.poll(() => screen.getByText('10,000 buffered', { exact: false }).count()).toBe(1);
    await expect.poll(() => screen.getByText(/older dropped/).count()).toBe(1);
    const renderedRows = await browser.locator('[role=log] > div > div').count();
    expect(renderedRows).toBeLessThan(100);
    const maxGap = await browser.evaluate(
      () => (window as unknown as { frameGaps: { max: number } }).frameGaps.max,
    );
    expect(maxGap).toBeLessThan(MAX_FRAME_GAP_MILLIS);
  });

  test('pauses when scrolled away and counts what arrives', async ({ browser, screen }) => {
    await openTail(browser, screen);
    await ingestBurst(200);
    await screen.getByText('burst 199', { exact: false }).waitFor();

    await browser.mouse.move(600, 400);
    await browser.mouse.wheel(0, -1_500);
    await ingest(logbrook, BROWSER_INDEX, [{ time: Date.now(), level: 30, msg: 'while paused' }]);

    await screen.getByRole('button', { name: /1 new event/ }).waitFor({ timeout: 10_000 });
    await screen.getByRole('button', { name: /new event/ }).click();
    await screen.getByText('while paused', { exact: false }).waitFor();
    expect(await screen.getByRole('button', { name: /new event/ }).count()).toBe(0);
  });

  test('drops the frozen view when the filters change, instead of showing the old stream', async ({
    browser,
    screen,
  }) => {
    await openTail(browser, screen);
    await ingestBurst(200);
    await screen.getByText('burst 199', { exact: false }).waitFor();
    await browser.mouse.move(600, 400);
    await browser.mouse.wheel(0, -1_500);
    await ingest(logbrook, BROWSER_INDEX, [{ time: Date.now(), level: 30, msg: 'while paused' }]);
    await screen.getByRole('button', { name: /1 new event/ }).waitFor({ timeout: 10_000 });

    await screen
      .getByLabel('Message contains (case-sensitive)', { exact: false })
      .fill('no-such-text');

    await screen.getByText('Waiting for new events', { exact: false }).waitFor({ timeout: 10_000 });
    expect(await screen.getByText(/burst \d+/).count()).toBe(0);
    expect(await screen.getByRole('button', { name: /new event/ }).count()).toBe(0);
    await ingest(logbrook, BROWSER_INDEX, [
      { time: Date.now(), level: 30, msg: 'no-such-text after all' },
    ]);
    await screen.getByText('no-such-text after all', { exact: false }).waitFor({ timeout: 10_000 });
  });

  test('keeps a typed trailing comma in the Sources field', async ({ browser, screen }) => {
    await openTail(browser, screen);
    const sources = screen.getByLabel('Sources (comma-separated)', { exact: false });

    await sources.pressSequentially('api,');
    await expect.poll(async () => (await searchParams(browser)).get('source')).toBe('["api"]');
    await pause(500);
    expect(await sources.inputValue()).toBe('api,');
    await sources.pressSequentially('web');

    expect(await sources.inputValue()).toBe('api,web');
    await expect
      .poll(async () => (await searchParams(browser)).get('source'))
      .toBe('["api","web"]');
  });

  test('explains a scope refusal as such, not as a rejected server token', async ({
    browser,
    screen,
  }) => {
    await browser.setViewport(VIEWPORT);

    await browser.goto(`${bffScoped.url}/tail?index=${BROWSER_INDEX}&source=not-allowed`);

    await screen
      .getByText('This token may not read that.', { exact: false })
      .waitFor({ timeout: 15_000 });
    expect(await screen.getByText(/token was rejected/).count()).toBe(0);
    expect(await screen.getByRole('link').filter({ hasText: /sign/i }).count()).toBe(0);
    expect(await screen.getByText(/scope/).count()).toBeGreaterThan(0);
  });

  test('tells the reader the server token was rejected when Logbrook answers 401', async ({
    browser,
    screen,
  }) => {
    await browser.setViewport(VIEWPORT);

    await browser.goto(`${bffWrongToken.url}/tail?index=${BROWSER_INDEX}`);

    await screen
      .getByText("The server's Logbrook token was rejected.", { exact: false })
      .waitFor({ timeout: 15_000 });
    expect(await screen.getByText(/LOGBROOK_READ_TOKEN/).count()).toBeGreaterThan(0);
    expect(await pathname(browser)).toBe('/tail');
    expect(await screen.getByRole('link').filter({ hasText: /sign/i }).count()).toBe(0);
  });
});
