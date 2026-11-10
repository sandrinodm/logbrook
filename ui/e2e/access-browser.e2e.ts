import type { Browser } from '@e2e-dev/web';

import { describe, test } from '@e2e-dev/web';
import { afterAll, beforeAll, expect } from 'e2e';

import type { BffServer, BffTokens } from './support/bff-server.ts';
import type { LogbrookServer } from './support/logbrook-server.ts';

import { startBff } from './support/bff-server.ts';
import { FIXTURE_INDEX, ingestFixtures } from './support/fixtures.ts';
import { teardown } from './support/helpers.ts';
import { TOKENS, startLogbrook } from './support/logbrook-server.ts';
import { pathname, pause, readText } from './support/page.ts';

/**
 * How long to wait for something that depends on the metrics poll or a first load, in milliseconds.
 */
const WAIT_MILLIS = 15_000;

/**
 * How long the page must stay quiet before a check that nothing was requested, in milliseconds.
 */
const NETWORK_IDLE_MILLIS = 500;

/**
 * A BFF configuration under test, by the name used in the test titles.
 */
type Configuration = 'full' | 'readOnly' | 'scoped' | 'wrongToken';

/**
 * The tokens of each configuration.
 */
const CONFIGURATIONS: Record<Configuration, BffTokens> = {
  full: { read: TOKENS.read, admin: TOKENS.admin },
  readOnly: { read: TOKENS.read },
  scoped: { read: TOKENS.scopedRead },
  wrongToken: { read: 'unknown-token-0123456789' },
};

let logbrook: LogbrookServer;
const servers = new Map<Configuration, BffServer>();

beforeAll(async () => {
  logbrook = await startLogbrook();
  await ingestFixtures(logbrook);
  for (const [name, tokens] of Object.entries(CONFIGURATIONS) as Array<
    [Configuration, BffTokens]
  >) {
    servers.set(name, await startBff(logbrook.url, tokens));
  }
});

afterAll(async () => {
  await teardown(...[...servers.values()].map((server) => () => server.stop()), () =>
    logbrook?.stop(),
  );
});

/**
 * Opens `path` on the BFF started for `configuration`, with no sign-in of any kind.
 */
async function open(
  browser: Browser,
  configuration: Configuration,
  path: string,
  waitUntil: 'load' | 'networkidle' = 'load',
): Promise<void> {
  const server = servers.get(configuration);
  if (server === undefined) {
    throw new Error(`No BFF for ${configuration}`);
  }

  await browser.goto(`${server.url}${path}`, { waitUntil });
}

describe('opening the app directly', () => {
  test('shows the explorer with data and no sign-in step', async ({ browser, screen }) => {
    await open(browser, 'readOnly', '/');

    await expect
      .poll(async () => readText(screen.getByText(/rows loaded/)), { timeout: WAIT_MILLIS })
      .toMatch(/rows loaded/);
    expect(await pathname(browser)).toBe('/');
    expect(await screen.getByText(/sign (in|out)/i).count()).toBe(0);
    expect(await screen.getByRole('button', { name: 'Sign out', exact: false }).count()).toBe(0);
  });

  test('makes no server function call when moving between views that need none', async ({
    browser,
    screen,
  }) => {
    await open(browser, 'readOnly', `/?index=${FIXTURE_INDEX}&range=6h`, 'networkidle');
    await screen.getByText(/rows loaded/).waitFor({ timeout: WAIT_MILLIS });
    const serverFunctionCalls: string[] = [];
    // The runner has no `page.on('request')`: a route that passes every server function request on
    // records it.
    await browser.route(/\/_serverFn\//, async (route) => {
      serverFunctionCalls.push(route.request.url);
      await route.fallback();
    });

    await screen.getByRole('link', { name: 'Live Tail', exact: false }).click();
    await expect.poll(() => pathname(browser)).toBe('/tail');
    await screen.getByRole('link', { name: 'Ingest', exact: false }).click();
    await expect.poll(() => pathname(browser)).toBe('/ingest');
    await screen.getByRole('link', { name: 'Logs', exact: false }).click();
    await expect.poll(() => pathname(browser)).toBe('/');
    // `waitForLoadState('networkidle')` has no equivalent; a quiet period of the same length (the
    // time Playwright's network idle needs) lets any late request arrive before the check.
    await pause(NETWORK_IDLE_MILLIS);

    expect(serverFunctionCalls).toEqual([]);
  });
});

describe('indexes view', () => {
  test('offers create and delete when an admin token is configured', async ({
    browser,
    screen,
  }) => {
    await open(browser, 'full', '/indexes');

    await screen
      .getByRole('button', { name: 'New index', exact: false })
      .waitFor({ timeout: WAIT_MILLIS });
    expect(await screen.getByText(FIXTURE_INDEX).count()).toBeGreaterThan(0);
    expect(await screen.getByRole('button', { name: /^Delete/ }).count()).toBeGreaterThan(0);
  });

  test('hides create and delete, and says why, without an admin token', async ({
    browser,
    screen,
  }) => {
    await open(browser, 'readOnly', '/indexes');

    await screen.getByText(/this server has none/).waitFor({ timeout: WAIT_MILLIS });
    expect(await screen.getByText(FIXTURE_INDEX).count()).toBeGreaterThan(0);
    expect(await screen.getByRole('button', { name: 'New index', exact: false }).count()).toBe(0);
    expect(await screen.getByRole('button', { name: /^Delete/ }).count()).toBe(0);
  });

  test('lists names only for a source-restricted read token', async ({ browser, screen }) => {
    await open(browser, 'scoped', '/indexes');

    await screen.getByText(/returns index names only/).waitFor({ timeout: WAIT_MILLIS });
    expect(await screen.getByText('Size / limit', { exact: false }).count()).toBe(1);
  });
});

describe('access panel and metrics', () => {
  test('reports the tokens the server holds and shows metrics', async ({ browser, screen }) => {
    await open(browser, 'full', '/ingest');

    const panel = screen.getByRole('region', { name: 'Server access', exact: false });
    await panel.waitFor({ timeout: WAIT_MILLIS });
    await expect.poll(() => readText(panel)).toContain('Can create and delete indexes.');
    await screen
      .getByText('Events ingested', { exact: false })
      .first()
      .waitFor({ timeout: WAIT_MILLIS });
  });

  test('reports that no admin token is configured', async ({ browser, screen }) => {
    await open(browser, 'readOnly', '/ingest');

    const panel = screen.getByRole('region', { name: 'Server access', exact: false });
    await expect
      .poll(() => readText(panel), { timeout: WAIT_MILLIS })
      .toContain('LOGBROOK_ADMIN_TOKEN');
    expect(await readText(panel)).not.toContain('Can create and delete indexes.');
  });

  test('explains why a source-restricted read token has no metrics', async ({
    browser,
    screen,
  }) => {
    await open(browser, 'scoped', '/ingest');

    await screen
      .getByText('Metrics are not available to this token', { exact: false })
      .waitFor({ timeout: WAIT_MILLIS });
    expect(
      await screen.getByText(/needs a read token with no source or index restriction/).count(),
    ).toBe(1);
  });
});

describe('a rejected server token', () => {
  test('shows the operator message in the explorer, without a redirect or a sign-in link', async ({
    browser,
    screen,
  }) => {
    await open(browser, 'wrongToken', `/?index=${FIXTURE_INDEX}&range=1h`);

    await screen
      .getByText("The server's Logbrook token was rejected", { exact: false })
      .first()
      .waitFor({ timeout: WAIT_MILLIS });
    expect(await screen.getByText(/LOGBROOK_READ_TOKEN/).count()).toBeGreaterThan(0);
    expect(await pathname(browser)).toBe('/');
    expect(await screen.getByRole('link').filter({ hasText: /sign/i }).count()).toBe(0);
  });

  test('shows the operator message when the URL names no index', async ({ browser, screen }) => {
    await open(browser, 'wrongToken', '/');

    await screen
      .getByText("The server's Logbrook token was rejected", { exact: false })
      .first()
      .waitFor({ timeout: WAIT_MILLIS });
    expect(await pathname(browser)).toBe('/');
    expect(await screen.getByText(/can read no index yet/).count()).toBe(0);
  });

  test('shows the operator message in the metrics view', async ({ browser, screen }) => {
    await open(browser, 'wrongToken', '/ingest');

    await screen
      .getByText(/The operator must fix LOGBROOK_READ_TOKEN/)
      .waitFor({ timeout: WAIT_MILLIS });
    expect(await pathname(browser)).toBe('/ingest');
    expect(await screen.getByRole('link').filter({ hasText: /sign/i }).count()).toBe(0);
  });
});
