import type { Browser, WebRoute } from '@e2e-dev/web';
import type { Screen } from 'e2e';

import { describe, test } from '@e2e-dev/web';
import { afterAll, beforeAll, expect } from 'e2e';
import { request as httpRequest } from 'node:http';

import type { BffServer } from './support/bff-server.ts';
import type { LogbrookServer } from './support/logbrook-server.ts';

import { deleteIndex, listIndexes } from '../src/server/credentials.server.ts';
import { startBff } from './support/bff-server.ts';
import { teardown, withTimeout } from './support/helpers.ts';
import { TOKENS, startLogbrook } from './support/logbrook-server.ts';

/**
 * Index the browser creates to learn what a server function request looks like.
 */
const RECORDED_NAME = 'csrf-recorded';

/**
 * Index a same-origin replay creates, to show that the replay itself is a valid request.
 */
const CONTROL_NAME = 'csrf-control';

/**
 * Index a cross-site request tries to create. It must never exist.
 */
const FORBIDDEN_NAME = 'csrf-forbidden';

/**
 * A request the browser sent, as the route handler sees it.
 */
type Request = WebRoute['request'];

/**
 * What a replayed request got back.
 */
type Replay = {
  /**
   * HTTP status.
   */
  readonly status: number;
};

let logbrook: LogbrookServer;
let bff: BffServer;

beforeAll(async () => {
  logbrook = await startLogbrook();
  bff = await startBff(logbrook.url, { read: TOKENS.read, admin: TOKENS.admin });
});

afterAll(async () => {
  await teardown(
    () => bff?.stop(),
    () => logbrook?.stop(),
  );
});

/**
 * Creates an index through the page and returns the server function request the browser sent.
 */
async function recordCreateRequest(browser: Browser, screen: Screen): Promise<Request> {
  await browser.goto(`${bff.url}/indexes`);
  await screen.getByRole('button', { name: 'New index', exact: false }).click();
  await screen.getByLabel('Name', { exact: false }).fill(RECORDED_NAME);
  // The runner has no `waitForRequest`: a route that passes every request on records the first
  // POST. It is registered right before the click, as the wait was.
  const sent = Promise.withResolvers<Request>();
  let recorded = false;
  await browser.route(/.*/, async (route) => {
    if (!recorded && route.request.method === 'POST') {
      recorded = true;
      sent.resolve(route.request);
    }

    await route.fallback();
  });
  await screen.getByRole('button', 'Create').click();
  return withTimeout(sent.promise, 30_000, 'the POST request of the create button');
}

/**
 * Sends the recorded request again over plain HTTP, so the headers a browser would set itself
 * (`Sec-Fetch-Site`, `Origin`) can be chosen freely. `rename` swaps the index name in the body.
 */
function replay(
  recorded: Request,
  name: string,
  headerChanges: Record<string, string | null>,
): Promise<Replay> {
  const headers: Record<string, string> = { ...recorded.headers };
  for (const [key, value] of Object.entries(headerChanges)) {
    if (value === null) {
      delete headers[key];
    } else {
      headers[key] = value;
    }
  }

  const body = (recorded.postData ?? '').replace(RECORDED_NAME, name);
  headers['content-length'] = String(Buffer.byteLength(body));
  const target = new URL(recorded.url);
  return new Promise((resolve, reject) => {
    const outgoing = httpRequest(
      {
        host: target.hostname,
        port: target.port,
        path: target.pathname + target.search,
        method: 'POST',
        headers,
      },
      (response) => {
        response.resume();
        response.on('end', () => resolve({ status: response.statusCode ?? 0 }));
      },
    );
    outgoing.once('error', reject);
    outgoing.end(body);
  });
}

/**
 * Names of the indexes on Logbrook, read with the admin token.
 */
async function indexNames(): Promise<ReadonlyArray<string>> {
  const indexes = await listIndexes(TOKENS.admin, { upstreamUrl: logbrook.url, fetch });
  return indexes.map((index) => index.name);
}

describe('server functions and cross-site requests', () => {
  test('refuses a cross-origin POST with 403 and creates nothing, while a same-origin one works', async ({
    browser,
    screen,
  }) => {
    const recorded = await recordCreateRequest(browser, screen);
    await expect.poll(indexNames).toContain(RECORDED_NAME);
    const origin = new URL(bff.url).origin;

    const crossSite = await replay(recorded, FORBIDDEN_NAME, {
      'sec-fetch-site': 'cross-site',
      origin: 'https://evil.example',
    });
    const sameSite = await replay(recorded, FORBIDDEN_NAME, {
      'sec-fetch-site': 'same-site',
      origin: 'https://evil.example',
    });
    const originOnly = await replay(recorded, FORBIDDEN_NAME, {
      'sec-fetch-site': null,
      origin: 'https://evil.example',
    });
    const sameOrigin = await replay(recorded, CONTROL_NAME, {
      'sec-fetch-site': 'same-origin',
      origin,
    });

    expect(crossSite.status).toBe(403);
    expect(sameSite.status).toBe(403);
    expect(originOnly.status).toBe(403);
    expect(sameOrigin.status).toBe(200);
    const names = await indexNames();
    expect(names).not.toContain(FORBIDDEN_NAME);
    expect(names).toContain(CONTROL_NAME);
    for (const name of [RECORDED_NAME, CONTROL_NAME]) {
      await deleteIndex(name, TOKENS.admin, { upstreamUrl: logbrook.url, fetch });
    }
  });
});
