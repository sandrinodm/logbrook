import type { Browser } from '@e2e-dev/web';
import type { Screen } from 'e2e';

import { describe, test } from '@e2e-dev/web';
import { afterAll, beforeAll, expect } from 'e2e';

import type { BffServer } from './support/bff-server.ts';
import type { LogbrookServer } from './support/logbrook-server.ts';

import { startBff } from './support/bff-server.ts';
import { FIXTURE_EVENT_COUNT, FIXTURE_INDEX, ingestFixtures } from './support/fixtures.ts';
import { teardown } from './support/helpers.ts';
import { TOKENS, startLogbrook } from './support/logbrook-server.ts';
import {
  HISTOGRAM_SELECTOR,
  moveMouseInSteps,
  pause,
  readText,
  scrollGrid,
  searchParams,
} from './support/page.ts';

let logbrook: LogbrookServer;
let bff: BffServer;

beforeAll(async () => {
  logbrook = await startLogbrook();
  await ingestFixtures(logbrook);
  bff = await startBff(logbrook.url, { read: TOKENS.read });
});

afterAll(async () => {
  await teardown(
    () => bff?.stop(),
    () => logbrook?.stop(),
  );
});

/**
 * Opens the explorer for an index and returns once the page is requested. With `networkidle` it
 * also waits for the page to go quiet, so the keyboard shortcuts are registered when it returns.
 */
async function openExplorer(
  browser: Browser,
  query: string,
  waitUntil: 'load' | 'networkidle' = 'load',
): Promise<void> {
  await browser.goto(`${bff.url}/?${query}`, { waitUntil });
}

/**
 * Waits for the footer to show `text` rows loaded.
 */
async function expectRowsLoaded(screen: Screen, text: string): Promise<void> {
  await expect
    .poll(async () => readText(screen.getByText(/rows loaded/)), { timeout: 15_000 })
    .toBe(text);
}

describe('explorer in a browser', () => {
  test('shows the total from the histogram, and pages in more rows by scrolling', async ({
    browser,
    screen,
  }) => {
    await openExplorer(browser, `index=${FIXTURE_INDEX}`);

    await expectRowsLoaded(screen, '500 rows loaded');
    await expect
      .poll(async () => readText(screen.getByText(/ events$/).first()))
      .toBe(`${FIXTURE_EVENT_COUNT.toLocaleString('en-US')} events`);

    await scrollGrid(browser, 'bottom');
    await expectRowsLoaded(screen, '1,000 rows loaded');
    await scrollGrid(browser, 'bottom');
    await expectRowsLoaded(screen, `${FIXTURE_EVENT_COUNT.toLocaleString('en-US')} rows loaded`);
  });

  test('renders only a window of rows, and different rows after scrolling', async ({
    browser,
    screen,
  }) => {
    await openExplorer(browser, `index=${FIXTURE_INDEX}`);
    await expectRowsLoaded(screen, '500 rows loaded');

    const rendered = await browser.locator('[role=row][data-index]').count();
    expect(rendered).toBeLessThan(100);

    await scrollGrid(browser, 10_000);
    const first = browser.locator('[role=row][data-index]').first();
    await expect
      .poll(async () => Number(await first.getAttribute('data-index')))
      .toBeGreaterThan(200);
  });

  test('opens an event in the inspector and keeps it in the URL', async ({ browser, screen }) => {
    await openExplorer(browser, `index=${FIXTURE_INDEX}`);
    await expectRowsLoaded(screen, '500 rows loaded');

    await browser.locator('[role=row][data-index="5"]').click();

    await expect.poll(async () => (await searchParams(browser)).get('event')).not.toBeNull();
    await expect
      .poll(async () => readText(screen.getByLabel('Event details', { exact: false })))
      .toContain('Envelope'.toUpperCase());
  });

  test('filters by a facet value and by message, and shows the empty state', async ({
    browser,
    screen,
  }) => {
    await openExplorer(browser, `index=${FIXTURE_INDEX}`);
    await expectRowsLoaded(screen, '500 rows loaded');

    await screen.getByRole('button', { name: /^billing/ }).click();
    await expect.poll(async () => (await searchParams(browser)).get('service')).toBe('billing');
    await expectRowsLoaded(screen, '500 rows loaded');

    await screen
      .getByLabel('Message contains (case-sensitive)', { exact: false })
      .fill('no such message');
    await screen.getByText('No matching events', { exact: false }).waitFor({ timeout: 15_000 });
    await screen.getByText('service = billing', { exact: false }).waitFor({ timeout: 15_000 });
  });

  test('zooms to a dragged range of the histogram', async ({ browser, screen }) => {
    await openExplorer(browser, `index=${FIXTURE_INDEX}`);
    await expectRowsLoaded(screen, '500 rows loaded');

    const box = await screen.getByRole('img', { name: /Events over time/ }).boundingBox();
    if (box === null) {
      throw new Error('The histogram is not visible');
    }

    const start = { x: box.x + box.width * 0.7, y: box.y + 20 };
    await browser.mouse.move(start.x, start.y);
    await browser.mouse.down();
    await moveMouseInSteps(browser, start, { x: box.x + box.width * 0.9, y: box.y + 20 }, 5);
    await browser.mouse.up();

    await expect.poll(async () => (await searchParams(browser)).has('from')).toBe(true);
    expect((await searchParams(browser)).has('to')).toBe(true);
    expect((await searchParams(browser)).has('at')).toBe(false);
  });

  describe('histogram from the keyboard', () => {
    /**
     * Presses Tab until the histogram has the focus; fails if it is never reached.
     */
    async function tabToHistogram(browser: Browser): Promise<void> {
      for (let step = 0; step < 60; step += 1) {
        await browser.keyboard.press('Tab');
        const role = await browser.evaluate(
          () => document.activeElement?.getAttribute('role') ?? null,
        );
        if (role === 'slider') {
          return;
        }
      }

      throw new Error('The histogram never got the focus with Tab');
    }

    /**
     * The histogram's keyboard stop.
     */
    const slider = (screen: Screen) =>
      screen.getByRole('slider', { name: 'Events over time', exact: false });

    test('moves a cursor, selects with Shift, announces it, and zooms with Enter, without a mouse', async ({
      browser,
      screen,
    }) => {
      await openExplorer(browser, `index=${FIXTURE_INDEX}`, 'networkidle');
      await expectRowsLoaded(screen, '500 rows loaded');
      await tabToHistogram(browser);

      // The first arrow puts the cursor on the newest bucket; the next ones move it left.
      await browser.keyboard.press('ArrowLeft');
      await browser.keyboard.press('ArrowLeft');
      await browser.keyboard.press('ArrowLeft');
      const bars = await browser.locator(`${HISTOGRAM_SELECTOR} g`).count();
      await expect.poll(() => slider(screen).getAttribute('aria-valuenow')).toBe(String(bars - 3));
      expect(await slider(screen).getAttribute('aria-valuetext')).toMatch(
        /events.*\. Shift with the arrows/,
      );
      await browser.locator('[data-testid=histogram-key-cursor]').waitFor();
      expect(await browser.locator('[data-testid=histogram-key-selection]').count()).toBe(0);

      // Shift+Left three times selects four buckets, the one the cursor started on included.
      for (let step = 0; step < 3; step += 1) {
        await browser.keyboard.press('Shift+ArrowLeft');
      }

      const selection = browser.locator('[data-testid=histogram-key-selection]');
      await selection.waitFor();
      const svg = await screen.getByRole('img', { name: /Events over time/ }).boundingBox();
      const rect = await selection.boundingBox();
      if (svg === null || rect === null) {
        throw new Error('The histogram is not visible');
      }

      // The box includes the 1 px outline, so allow a pixel more than four buckets.
      expect(Math.abs(rect.width - (svg.width / bars) * 4)).toBeLessThan(1.5);
      expect(await slider(screen).getAttribute('aria-valuetext')).toMatch(
        /^Selected .* to .*, \d+ events/,
      );
      // The header, a live region, reads the selection out too.
      await expect
        .poll(() => readText(browser.locator('[aria-live=polite]').first()))
        .toMatch(/selected .* to .* \d+ events.*Enter to zoom, Esc to clear/);

      // Escape clears the selection and leaves the page alone; the selection can be made again.
      await browser.keyboard.press('Escape');
      await expect.poll(() => selection.count()).toBe(0);
      expect((await searchParams(browser)).has('from')).toBe(false);
      await browser.keyboard.press('Shift+ArrowLeft');
      await browser.keyboard.press('Shift+ArrowLeft');
      await selection.waitFor();

      await browser.keyboard.press('Enter');

      await expect.poll(async () => (await searchParams(browser)).has('from')).toBe(true);
      const params = await searchParams(browser);
      expect(params.has('at')).toBe(false);
      const from = Number(params.get('from'));
      const to = Number(params.get('to'));
      expect(to - from).toBeGreaterThan(0);
      // Two buckets after the first Shift+Left pair, so a multiple of the interval, narrower than the range.
      expect(to - from).toBeLessThan(60 * 60 * 1000);
    });

    test('lists the keys in the shortcut help', async ({ browser, screen }) => {
      await openExplorer(browser, `index=${FIXTURE_INDEX}`, 'networkidle');
      await expectRowsLoaded(screen, '500 rows loaded');

      await browser.keyboard.press('?');
      const dialog = screen.getByRole('dialog', { name: 'Keyboard shortcuts', exact: false });
      await dialog.waitFor();

      await dialog.getByRole('region', { name: /histogram/i }).waitFor();
      expect(
        await dialog.getByText('Extend the selection over more buckets', { exact: false }).count(),
      ).toBe(1);
      expect(await dialog.getByText('Zoom to the selection', { exact: false }).count()).toBe(1);
    });
  });

  test('explains an unknown index', async ({ browser, screen }) => {
    await openExplorer(browser, 'index=no-such-index');

    await expect
      .poll(() => screen.getByText('Index not found', { exact: false }).count(), {
        timeout: 15_000,
      })
      .toBeGreaterThan(0);
    // The table explains it in full; the chart and the facet sidebar do not repeat it.
    await screen.getByText('no data', { exact: false }).waitFor();
    expect(await screen.getByText('Index not found', { exact: false }).count()).toBe(1);
    expect(await screen.getByRole('alert').count()).toBe(1);
  });

  test('explains a rejected query, quoting the server', async ({ browser, screen }) => {
    await openExplorer(browser, `index=${FIXTURE_INDEX}&from=0&to=${Date.now()}`);

    await expect
      .poll(() => screen.getByText('The server rejected this query', { exact: false }).count(), {
        timeout: 15_000,
      })
      .toBeGreaterThan(0);
    expect(await screen.getByText('The server rejected this query', { exact: false }).count()).toBe(
      1,
    );
    expect(await screen.getByRole('alert').count()).toBe(1);
  });
});

/**
 * Counts the log searches the page sends, split by whether they carry a cursor, and answers the
 * ones with a cursor with `status` while `failing.on` is true. Facet and histogram requests have
 * other paths and are not counted. The pattern is a RegExp because `?` in a glob of the runner
 * matches any character and would also catch `/logs/facets?...`.
 */
async function interceptSearches(
  browser: Browser,
  status: number,
): Promise<{ first: number; paged: number; failing: { on: boolean } }> {
  const counts = { first: 0, paged: 0, failing: { on: true } };
  await browser.route(/\/api\/lb\/indexes\/[^/]+\/logs\?/, async (route) => {
    if (!new URL(route.request.url).searchParams.has('cursor')) {
      counts.first += 1;
      await route.continue();
      return;
    }

    counts.paged += 1;
    if (counts.failing.on) {
      await route.fulfill({
        status,
        contentType: 'application/json',
        body: JSON.stringify({ error: status === 400 ? 'cursor expired' : 'boom' }),
      });
      return;
    }

    await route.continue();
  });
  return counts;
}

describe('explorer when an older page fails', () => {
  test('keeps the loaded rows, shows the failure under them and retries only that page', async ({
    browser,
    screen,
  }) => {
    const counts = await interceptSearches(browser, 500);
    await browser.goto(`${bff.url}/?index=${FIXTURE_INDEX}`);
    await expectRowsLoaded(screen, '500 rows loaded');

    await scrollGrid(browser, 'bottom');

    await screen
      .getByText('Older events could not be loaded', { exact: false })
      .waitFor({ timeout: 15_000 });
    expect(await screen.getByRole('grid').count()).toBe(1);
    await expectRowsLoaded(screen, '500 rows loaded');
    // The table does not ask again by itself.
    await pause(1_500);
    expect(counts.paged).toBe(1);

    counts.failing.on = false;
    await screen.getByRole('button', { name: 'Try this page again', exact: false }).click();

    await expectRowsLoaded(screen, '1,000 rows loaded');
    expect(counts.paged).toBe(2);
    expect(counts.first).toBe(1);
    expect(
      await screen.getByText('Older events could not be loaded', { exact: false }).count(),
    ).toBe(0);
  });

  test('offers to start the search again when the cursor was refused', async ({
    browser,
    screen,
  }) => {
    const counts = await interceptSearches(browser, 400);
    await browser.goto(`${bff.url}/?index=${FIXTURE_INDEX}`);
    await expectRowsLoaded(screen, '500 rows loaded');

    await scrollGrid(browser, 'bottom');

    await screen
      .getByText('Older events could not be loaded', { exact: false })
      .waitFor({ timeout: 15_000 });
    expect(
      await screen.getByRole('button', { name: 'Try this page again', exact: false }).count(),
    ).toBe(0);
    counts.failing.on = false;
    await screen.getByRole('button', { name: 'Start the search again', exact: false }).click();

    await expect
      .poll(() => screen.getByText('Older events could not be loaded', { exact: false }).count())
      .toBe(0);
    await expectRowsLoaded(screen, '500 rows loaded');
    expect(counts.first).toBe(2);
  });
});
