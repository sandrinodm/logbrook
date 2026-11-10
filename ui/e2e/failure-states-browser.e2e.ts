import type { Browser } from '@e2e-dev/web';

import { describe, test } from '@e2e-dev/web';
import { afterAll, beforeAll, expect } from 'e2e';
import { createServer } from 'node:http';

import type { BffServer } from './support/bff-server.ts';
import type { LogbrookServer } from './support/logbrook-server.ts';

import { startBff } from './support/bff-server.ts';
import { freePort } from './support/free-port.ts';
import { teardown } from './support/helpers.ts';
import { TOKENS, ingest, startLogbrook } from './support/logbrook-server.ts';
import { chartWidths, readText } from './support/page.ts';

/**
 * How long to wait for something that depends on the metrics poll (every 5 s), in milliseconds.
 */
const POLL_WAIT_MILLIS = 15_000;

let logbrook: LogbrookServer;
let bff: BffServer;
let bffWithAdmin: BffServer;

beforeAll(async () => {
  logbrook = await startLogbrook();
  bff = await startBff(logbrook.url, { read: TOKENS.read });
  bffWithAdmin = await startBff(logbrook.url, { read: TOKENS.read, admin: TOKENS.admin });
});

afterAll(async () => {
  await teardown(
    () => bff?.stop(),
    () => bffWithAdmin?.stop(),
    () => logbrook?.stop(),
  );
});

/**
 * Opens a page of `server`.
 */
async function openPage(browser: Browser, server: BffServer, path: string): Promise<void> {
  await browser.goto(`${server.url}${path}`);
}

/**
 * A running front for Logbrook.
 */
type Front = {
  /**
   * Base URL without a trailing slash.
   */
  readonly url: string;

  /**
   * Stops the server.
   */
  readonly stop: () => Promise<void>;
};

/**
 * Starts an HTTP server that forwards every request to `upstreamUrl` except `GET /indexes`, which
 * it answers with an empty list.
 */
async function startEmptyListFront(upstreamUrl: string): Promise<Front> {
  const server = createServer((request, response) => {
    const path = request.url ?? '/';
    if (path === '/indexes') {
      response.setHeader('content-type', 'application/json');
      response.end(JSON.stringify({ indexes: [] }));
      return;
    }

    const controller = new AbortController();
    response.on('close', () => controller.abort());
    fetch(`${upstreamUrl}${path}`, {
      headers: { authorization: request.headers.authorization ?? '' },
      signal: controller.signal,
    }).then(
      async (upstream) => {
        response.writeHead(upstream.status, {
          'content-type': upstream.headers.get('content-type') ?? 'application/json',
        });
        response.end(Buffer.from(await upstream.arrayBuffer()));
      },
      () => response.destroy(),
    );
  });
  const port = await freePort();
  await new Promise<void>((done) => server.listen(port, '127.0.0.1', done));
  return {
    url: `http://127.0.0.1:${port}`,
    stop: () =>
      new Promise<void>((done) => {
        server.closeAllConnections();
        server.close(() => done());
      }),
  };
}

describe('metrics view', () => {
  test('says the figures are out of date when a later scrape fails', async ({
    browser,
    screen,
  }) => {
    let failing = false;
    // The pattern is a RegExp: in a glob of the runner `?` would match any character.
    await browser.route(/\/api\/lb\/metrics$/, async (route) => {
      if (failing) {
        await route.fulfill({
          status: 500,
          contentType: 'application/json',
          body: JSON.stringify({ error: 'scrape exploded' }),
        });
        return;
      }

      await route.continue();
    });
    await browser.goto(`${bff.url}/ingest`);
    await screen
      .getByText('Events ingested', { exact: false })
      .first()
      .waitFor({ timeout: POLL_WAIT_MILLIS });
    expect(await screen.getByText('These figures are out of date', { exact: false }).count()).toBe(
      0,
    );

    failing = true;

    await screen
      .getByText('These figures are out of date', { exact: false })
      .waitFor({ timeout: POLL_WAIT_MILLIS });
    const warning = await readText(screen.getByRole('alert'));
    expect(warning).toContain('scrape exploded');
    expect(warning).toContain('last good scrape was at');
    expect(await screen.getByText('Events ingested', { exact: false }).count()).toBeGreaterThan(0);

    failing = false;
    await screen.getByRole('button', { name: 'Try again', exact: false }).click();
    await expect
      .poll(() => screen.getByText('These figures are out of date', { exact: false }).count())
      .toBe(0);
  });
});

describe('index dialogs', () => {
  test('show a message when the server function itself cannot be reached', async ({
    browser,
    screen,
  }) => {
    await openPage(browser, bffWithAdmin, '/indexes');
    await browser.route(/\/_serverFn\//, async (route) => {
      if (route.request.method === 'POST') {
        await route.abort();
        return;
      }

      await route.continue();
    });

    await screen.getByRole('button', { name: 'New index', exact: false }).click();
    await screen.getByLabel('Name', { exact: false }).fill('network-down');
    await screen.getByRole('button', 'Create').click();

    await screen
      .getByRole('alert')
      .getByText(/Could not reach the server/)
      .waitFor();
    expect(await screen.getByRole('button', 'Create').isDisabled()).toBe(false);
  });
});

describe('index delete dialog', () => {
  test('shows a message when the server function itself cannot be reached', async ({
    browser,
    screen,
  }) => {
    await ingest(logbrook, 'doomed', [{ time: Date.now(), level: 30, msg: 'to be deleted' }]);
    await openPage(browser, bffWithAdmin, '/indexes');
    await browser.route(/\/_serverFn\//, async (route) => {
      if (route.request.method === 'POST') {
        await route.abort();
        return;
      }

      await route.continue();
    });

    await screen.getByRole('button', 'Delete').click();
    await screen.getByLabel(/^Type doomed to confirm/).fill('doomed');
    await screen.getByRole('button', { name: 'Delete index', exact: false }).click();

    await screen
      .getByRole('alert')
      .getByText(/Could not reach the server/)
      .waitFor();
    expect(
      await screen.getByRole('button', { name: 'Delete index', exact: false }).isDisabled(),
    ).toBe(false);
  });
});

describe('explorer opened before any index exists', () => {
  test('measures the chart once it appears and follows later resizes', async ({
    browser,
    screen,
  }) => {
    // A front for Logbrook that reports an empty index list, which is what makes the explorer
    // render without a plan; every other request is passed on.
    const front = await startEmptyListFront(logbrook.url);
    const frontBff = await startBff(front.url, { read: TOKENS.read });
    try {
      await openPage(browser, frontBff, '/');
      await screen
        .getByText('This token can read no index yet', { exact: false })
        .waitFor({ timeout: 15_000 });

      // A client-side navigation to the same route: the explorer component stays mounted and its
      // chart area appears only now.
      await browser.evaluate(() => {
        window.history.pushState({}, '', '/?index=default');
        window.dispatchEvent(new PopStateEvent('popstate'));
        return null;
      });
      const chart = screen.getByRole('img', { name: /Events over time/ });
      await chart.waitFor({ timeout: 15_000 });

      await expect
        .poll(async () => (await chartWidths(browser)).drawn)
        .toBe((await chartWidths(browser)).available);

      await browser.setViewport({ width: 800, height: 850 });

      await expect
        .poll(async () => (await chartWidths(browser)).drawn)
        .toBe((await chartWidths(browser)).available);
      expect((await chartWidths(browser)).available).toBeLessThan(800);
    } finally {
      await frontBff.stop();
      await front.stop();
    }
  });
});
