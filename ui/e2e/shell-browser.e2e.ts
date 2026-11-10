import type { Browser } from '@e2e-dev/web';
import type { Screen } from 'e2e';

import { describe, test } from '@e2e-dev/web';
import { afterAll, beforeAll, expect } from 'e2e';

import type { BffServer } from './support/bff-server.ts';
import type { LogbrookServer } from './support/logbrook-server.ts';

import { startBff } from './support/bff-server.ts';
import { FIXTURE_INDEX, ingestFixtures } from './support/fixtures.ts';
import { teardown } from './support/helpers.ts';
import { TOKENS, ingest, startLogbrook } from './support/logbrook-server.ts';
import { pathname, pause, readText, searchParams, settle } from './support/page.ts';

/**
 * An index whose name is made only of digits, which the router parses as a number.
 */
const DIGIT_INDEX = '12345';

let logbrook: LogbrookServer;
let bff: BffServer;

beforeAll(async () => {
  logbrook = await startLogbrook();
  await ingestFixtures(logbrook);
  await ingest(
    logbrook,
    DIGIT_INDEX,
    Array.from({ length: 20 }, (_, index) => ({
      time: Date.now() - 60_000 - index * 1000,
      level: 30,
      msg: `digit event ${index}`,
      service: 'checkout',
    })),
  );
  bff = await startBff(logbrook.url, { read: TOKENS.read });
});

afterAll(async () => {
  await teardown(
    () => bff?.stop(),
    () => logbrook?.stop(),
  );
});

/**
 * What `open` takes besides the path.
 */
type OpenOptions = {
  /**
   * `prefers-color-scheme` to emulate before the page loads.
   */
  readonly colorScheme?: 'dark' | 'light';

  /**
   * `prefers-reduced-motion` to emulate before the page loads.
   */
  readonly reducedMotion?: 'reduce';

  /**
   * How far the navigation must get before `open` returns. `networkidle` also waits for the page to
   * go quiet, so it has hydrated and its keyboard shortcuts are registered.
   */
  readonly waitUntil?: 'load' | 'networkidle';
};

/**
 * Opens `path` with the given media preferences.
 */
async function open(browser: Browser, path: string, options: OpenOptions = {}): Promise<void> {
  const { waitUntil = 'load', ...media } = options;
  if (Object.keys(media).length > 0) {
    await browser.emulateMedia(media);
  }

  await browser.goto(`${bff.url}${path}`, { waitUntil });
}

/**
 * Waits until the explorer reports `text` rows loaded.
 */
async function expectRows(screen: Screen, text: string): Promise<void> {
  await expect
    .poll(async () => readText(screen.getByText(/rows loaded/)), { timeout: 15_000 })
    .toBe(text);
}

/**
 * Whether the page is in dark mode: the `dark` class on the root element.
 */
function isDark(browser: Browser): Promise<boolean> {
  return browser.evaluate(() => document.documentElement.classList.contains('dark'));
}

describe('index names made only of digits', () => {
  test('opens the explorer and the tail on such an index', async ({ browser, screen }) => {
    await open(browser, `/?index=${DIGIT_INDEX}&range=1h`);

    await expectRows(screen, '20 rows loaded');
    expect(
      await screen.getByRole('button', { name: `Index: ${DIGIT_INDEX}`, exact: false }).count(),
    ).toBe(1);

    await screen.getByRole('link', { name: 'Live Tail', exact: false }).click();
    await expect.poll(() => pathname(browser)).toBe('/tail');
    await screen.getByText('Live').waitFor({ timeout: 10_000 });
    expect(await screen.getByLabel('Index').inputValue()).toBe(DIGIT_INDEX);
  });
});

describe('filters between Logs and Live Tail', () => {
  test('carries the index and filters both ways and leaves explorer-only params out', async ({
    browser,
    screen,
  }) => {
    await open(
      browser,
      `/?index=${FIXTURE_INDEX}&range=6h&service=checkout&minLevel=50&message=fixture&cols=seq`,
      { waitUntil: 'networkidle' },
    );
    await expectRows(screen, '120 rows loaded');

    await screen.getByRole('link', { name: 'Live Tail', exact: false }).click();
    await expect.poll(() => pathname(browser)).toBe('/tail');
    expect(Object.fromEntries(await searchParams(browser))).toEqual({
      index: FIXTURE_INDEX,
      service: 'checkout',
      minLevel: '50',
      message: 'fixture',
    });

    await screen.getByRole('link', { name: 'Logs', exact: false }).click();
    await expect.poll(() => pathname(browser)).toBe('/');
    await expect.poll(async () => (await searchParams(browser)).has('at')).toBe(true);
    const params = await searchParams(browser);
    expect(params.get('service')).toBe('checkout');
    expect(params.get('minLevel')).toBe('50');
    expect(params.get('message')).toBe('fixture');
    expect(params.has('event')).toBe(false);
  });

  test('switches with the t key, and ignores it while typing in a field', async ({
    browser,
    screen,
  }) => {
    await open(browser, `/?index=${FIXTURE_INDEX}&service=checkout`, { waitUntil: 'networkidle' });
    await expectRows(screen, '500 rows loaded');

    await screen.getByLabel('Message contains (case-sensitive)', { exact: false }).fill('tt');
    expect(await pathname(browser)).toBe('/');

    await browser.keyboard.press('Escape');
    await browser.keyboard.press('t');
    await expect.poll(() => pathname(browser)).toBe('/tail');
    expect((await searchParams(browser)).get('service')).toBe('checkout');

    await browser.keyboard.press('t');
    await expect.poll(() => pathname(browser)).toBe('/');
  });
});

describe('explorer settings across a visit to the tail', () => {
  /**
   * Explorer-only params the tests start with. None is the default, so a restored value cannot be
   * mistaken for a default that was filled in.
   */
  const EXPLORER_ONLY = 'range=24h&density=comfortable&tz=local&wrap=1&cols=seq';

  /**
   * Waits for the explorer after a return from the tail, and reads its URL params.
   */
  async function backOnLogs(browser: Browser): Promise<URLSearchParams> {
    await expect.poll(() => pathname(browser)).toBe('/');
    await expect.poll(async () => (await searchParams(browser)).has('at')).toBe(true);
    return searchParams(browser);
  }

  /**
   * The explorer-only params of a URL, and whether its "now" is newer than `before`.
   */
  function settingsOf(params: URLSearchParams, before: number) {
    return {
      range: params.get('range'),
      density: params.get('density'),
      tz: params.get('tz'),
      wrap: params.get('wrap'),
      pinsSeq: params.get('cols')?.includes('seq') ?? false,
      freshNow: Number(params.get('at')) > before,
    };
  }

  /**
   * What `settingsOf` reads after the settings came back.
   */
  const RESTORED = {
    range: '24h',
    density: 'comfortable',
    tz: 'local',
    wrap: '1',
    pinsSeq: true,
    freshNow: true,
  };

  /**
   * Opens the explorer with the explorer-only settings and leaves for the tail through the nav.
   * Returns the moment of the first load.
   */
  async function visitTail(browser: Browser, screen: Screen): Promise<number> {
    await open(browser, `/?index=${FIXTURE_INDEX}&${EXPLORER_ONLY}&service=checkout`, {
      waitUntil: 'networkidle',
    });
    await expectRows(screen, '500 rows loaded');
    const first = Number((await searchParams(browser)).get('at'));

    await screen.getByRole('link', { name: 'Live Tail', exact: false }).click();
    await expect.poll(() => pathname(browser)).toBe('/tail');
    expect(Object.fromEntries(await searchParams(browser))).toEqual({
      index: FIXTURE_INDEX,
      service: 'checkout',
    });
    await pause(20);
    return first;
  }

  test('brings the range and display settings back through the nav, keeping the tail filters', async ({
    browser,
    screen,
  }) => {
    const first = await visitTail(browser, screen);
    await screen.getByLabel('Service').fill('billing');
    await expect.poll(async () => (await searchParams(browser)).get('service')).toBe('billing');

    await screen.getByRole('link', { name: 'Logs', exact: false }).click();
    const params = await backOnLogs(browser);

    expect(settingsOf(params, first)).toEqual(RESTORED);
    expect(params.get('service')).toBe('billing');
    expect(params.has('event')).toBe(false);
  });

  test('brings them back with the t key', async ({ browser, screen }) => {
    const first = await visitTail(browser, screen);
    await browser.locator('body').click({ position: { x: 5, y: 400 } });

    await browser.keyboard.press('t');

    expect(settingsOf(await backOnLogs(browser), first)).toEqual(RESTORED);
  });

  test('brings them back through the command palette', async ({ browser, screen }) => {
    const first = await visitTail(browser, screen);

    await browser.keyboard.press('Control+k');
    await screen.getByRole('dialog', { name: 'Command palette', exact: false }).waitFor();
    await browser.keyboard.type('logs');
    await browser.keyboard.press('Enter');

    expect(settingsOf(await backOnLogs(browser), first)).toEqual(RESTORED);
  });

  test('does not alter a direct link to the explorer', async ({ browser, screen }) => {
    await visitTail(browser, screen);

    // A direct link is a new page load in this or another tab: nothing is remembered.
    await browser.goto(`${bff.url}/?index=${FIXTURE_INDEX}&range=15m`);
    await expect.poll(async () => (await searchParams(browser)).has('at')).toBe(true);

    const params = await searchParams(browser);
    expect(params.get('range')).toBe('15m');
    expect(params.has('density')).toBe(false);
    expect(params.has('tz')).toBe(false);
    expect(params.has('wrap')).toBe(false);
    expect(params.has('cols')).toBe(false);
  });
});

describe('keyboard and dialogs', () => {
  test('opens the shortcut help with ? and gives the focus back when it closes', async ({
    browser,
    screen,
  }) => {
    await open(browser, `/?index=${FIXTURE_INDEX}`, { waitUntil: 'networkidle' });
    await expectRows(screen, '500 rows loaded');

    await browser.keyboard.press('?');
    const dialog = screen.getByRole('dialog', { name: 'Keyboard shortcuts', exact: false });
    await dialog.waitFor();
    expect(await dialog.getByText('Open the command palette', { exact: false }).count()).toBe(1);

    await browser.keyboard.press('Escape');
    await dialog.waitFor({ state: 'detached' });
  });

  test('returns the focus to the button that opened a dialog', async ({ browser, screen }) => {
    await open(browser, `/?index=${FIXTURE_INDEX}`, { waitUntil: 'networkidle' });
    await expectRows(screen, '500 rows loaded');
    const activeLabel = () =>
      browser.evaluate(() => document.activeElement?.getAttribute('aria-label') ?? null);

    await screen.getByRole('button', { name: 'Keyboard shortcuts', exact: false }).focus();
    await browser.keyboard.press('Enter');
    await screen.getByRole('dialog', { name: 'Keyboard shortcuts', exact: false }).waitFor();
    expect(
      await browser.evaluate(() => document.activeElement?.closest('[role=dialog]') !== null),
    ).toBe(true);
    await browser.keyboard.press('Escape');
    await screen.getByRole('dialog').waitFor({ state: 'detached' });
    // The runner sees the dialog gone before the page has finished giving the focus back, which
    // Playwright's slower wait hid.
    await settle(browser);
    expect(await activeLabel()).toBe('Keyboard shortcuts');

    await screen.getByRole('button', { name: 'Open the command palette', exact: false }).focus();
    await browser.keyboard.press('Enter');
    await screen.getByRole('dialog', { name: 'Command palette', exact: false }).waitFor();
    await browser.keyboard.press('Escape');
    await screen.getByRole('dialog').waitFor({ state: 'detached' });
    await settle(browser);
    expect(await activeLabel()).toBe('Open the command palette');
  });

  test('opens the command palette with Ctrl+K and runs a command', async ({ browser, screen }) => {
    await open(browser, `/?index=${FIXTURE_INDEX}`, { waitUntil: 'networkidle' });
    await expectRows(screen, '500 rows loaded');

    await browser.keyboard.press('Control+k');
    await screen.getByRole('dialog').waitFor();
    // Typing the key that switches views must go into the palette's field, not switch the view.
    await browser.keyboard.type('live t');
    expect(await pathname(browser)).toBe('/');
    await browser.keyboard.press('Enter');

    await expect.poll(() => pathname(browser)).toBe('/tail');
    expect((await searchParams(browser)).get('index')).toBe(FIXTURE_INDEX);
  });

  test('selects with j and k, opens with Enter and returns the focus to the grid on Escape', async ({
    browser,
    screen,
  }) => {
    await open(browser, `/?index=${FIXTURE_INDEX}`, { waitUntil: 'networkidle' });
    await expectRows(screen, '500 rows loaded');

    // Each key is awaited until the table has drawn the new selection, as a person would.
    const selectedRow = () =>
      browser.locator('[role=row][aria-selected=true]').getAttribute('data-index');
    await browser.keyboard.press('j');
    await expect.poll(selectedRow).toBe('0');
    const first = (await searchParams(browser)).get('event');
    await browser.keyboard.press('j');
    await expect.poll(selectedRow).toBe('1');
    expect((await searchParams(browser)).get('event')).not.toBe(first);
    await browser.keyboard.press('k');
    await expect.poll(selectedRow).toBe('0');
    expect((await searchParams(browser)).get('event')).toBe(first);

    await browser.keyboard.press('Escape');
    await expect.poll(async () => (await searchParams(browser)).has('event')).toBe(false);
    expect(await browser.evaluate(() => document.activeElement?.getAttribute('role') ?? null)).toBe(
      'grid',
    );
  });

  test('focuses the message box with / and leaves it with Escape', async ({ browser, screen }) => {
    await open(browser, `/?index=${FIXTURE_INDEX}`, { waitUntil: 'networkidle' });
    await expectRows(screen, '500 rows loaded');

    await browser.keyboard.press('/');
    expect(
      await browser.evaluate(() => document.activeElement?.getAttribute('aria-label') ?? null),
    ).toContain('Message');
    await browser.keyboard.press('Escape');
    expect(await browser.evaluate(() => document.activeElement?.tagName ?? null)).toBe('BODY');
  });

  test('shows a focus ring on every control reached with Tab', async ({ browser, screen }) => {
    await open(browser, `/?index=${FIXTURE_INDEX}`, { waitUntil: 'networkidle' });
    await expectRows(screen, '500 rows loaded');

    const unfocused: string[] = [];
    for (let step = 0; step < 25; step += 1) {
      await browser.keyboard.press('Tab');
      const visible = await browser.evaluate(() => {
        const element = document.activeElement;
        if (element === null || element === document.body) {
          return { label: 'body', ring: true };
        }

        const style = getComputedStyle(element);
        const outline = style.outlineStyle !== 'none' && Number.parseFloat(style.outlineWidth) > 0;
        const shadow = style.boxShadow !== 'none';
        const label = `${element.tagName} ${element.getAttribute('aria-label') ?? element.textContent?.slice(0, 20) ?? ''}`;
        return { label, ring: outline || shadow };
      });
      if (!visible.ring) {
        unfocused.push(visible.label);
      }
    }

    expect(unfocused).toEqual([]);
  });
});

describe('theme', () => {
  test('follows the system preference before the first paint, without a stored choice', async ({
    browser,
  }) => {
    await open(browser, '/ingest', { colorScheme: 'dark' });
    expect(await isDark(browser)).toBe(true);

    // The runner gives a test one browser context, where this test used two fresh ones. Asserting
    // that nothing was stored keeps the second load free of any choice carried over from the first.
    await browser.emulateMedia({ colorScheme: 'light' });
    expect(await browser.evaluate(() => localStorage.length)).toBe(0);
    await browser.goto(`${bff.url}/ingest`);
    expect(await isDark(browser)).toBe(false);
  });

  test('sets the class from the document head, before the body exists', async ({ browser }) => {
    await browser.emulateMedia({ colorScheme: 'dark' });
    await browser.addInitScript(() => {
      const seen = { bodyExistedWhenThemeApplied: null as boolean | null };
      Object.assign(window, { themeProbe: seen });
      new MutationObserver(() => {
        if (seen.bodyExistedWhenThemeApplied === null) {
          seen.bodyExistedWhenThemeApplied = document.body !== null;
        }
      }).observe(document, { attributes: true, subtree: true, attributeFilter: ['class'] });
    });
    await browser.goto(`${bff.url}/ingest`);

    const probe = await browser.evaluate(
      () =>
        (window as unknown as { themeProbe: { bodyExistedWhenThemeApplied: boolean } }).themeProbe,
    );
    expect(probe.bodyExistedWhenThemeApplied).toBe(false);
  });

  test('keeps an explicit choice over the system preference, and survives a reload', async ({
    browser,
    screen,
  }) => {
    await open(browser, `/?index=${FIXTURE_INDEX}`, {
      colorScheme: 'dark',
      waitUntil: 'networkidle',
    });
    await expectRows(screen, '500 rows loaded');

    // system -> light
    await screen.getByRole('button', { name: /^Theme: system/ }).click();
    await screen.getByRole('button', { name: /^Theme: light/ }).waitFor();
    expect(await isDark(browser)).toBe(false);

    await browser.reload();
    expect(await isDark(browser)).toBe(false);
  });
});

describe('reduced motion', () => {
  test('turns animations off when the system asks for less motion', async ({ browser }) => {
    await open(browser, `/?index=${FIXTURE_INDEX}`, { reducedMotion: 'reduce' });

    const duration = await browser.evaluate(() => {
      const probe = document.createElement('div');
      probe.className = 'animate-pulse';
      document.body.append(probe);
      const value = getComputedStyle(probe).animationDuration;
      probe.remove();
      return value;
    });
    expect(Number.parseFloat(duration)).toBeLessThan(0.001);
  });
});
