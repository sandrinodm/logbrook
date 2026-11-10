import { afterAll, beforeAll, describe, expect, test } from 'e2e';
import { request as httpRequest } from 'node:http';
import { gunzipSync } from 'node:zlib';

import type { BffServer } from './support/bff-server.ts';
import type { FixtureRange } from './support/fixtures.ts';
import type { LogbrookServer } from './support/logbrook-server.ts';

import {
  classifyToken,
  createIndex,
  deleteIndex,
  checkConfiguredTokens,
  listIndexes,
} from '../src/server/credentials.server.ts';
import { startBff } from './support/bff-server.ts';
import { FIXTURE_EVENT_COUNT, FIXTURE_INDEX, ingestFixtures } from './support/fixtures.ts';
import { rejection, teardown } from './support/helpers.ts';
import { TOKENS, startLogbrook } from './support/logbrook-server.ts';

let logbrook: LogbrookServer;
let range: FixtureRange;

/**
 * BFF with an unrestricted read token and no admin token.
 */
let bff: BffServer;

/**
 * BFF with an unrestricted read token and an admin token.
 */
let bffWithAdmin: BffServer;

/**
 * BFF whose read token is restricted to one source, with an admin token.
 */
let bffScoped: BffServer;

/**
 * BFF whose read token Logbrook does not know.
 */
let bffWrongToken: BffServer;

beforeAll(async () => {
  logbrook = await startLogbrook();
  range = await ingestFixtures(logbrook);
  bff = await startBff(logbrook.url, { read: TOKENS.read });
  bffWithAdmin = await startBff(logbrook.url, { read: TOKENS.read, admin: TOKENS.admin });
  bffScoped = await startBff(logbrook.url, { read: TOKENS.scopedRead, admin: TOKENS.admin });
  bffWrongToken = await startBff(logbrook.url, { read: 'unknown-token-0123456789' });
});

afterAll(async () => {
  await teardown(
    () => bff?.stop(),
    () => bffWithAdmin?.stop(),
    () => bffScoped?.stop(),
    () => bffWrongToken?.stop(),
    () => logbrook?.stop(),
  );
});

/**
 * GETs a path on the BFF `server` (the plain one by default).
 */
function proxyGet(path: string, server: BffServer = bff, headers: Record<string, string> = {}) {
  return fetch(`${server.url}/api/lb/${path}`, { headers });
}

/**
 * Builds the range query string shared by the search tests.
 */
function rangeQuery(extra = ''): string {
  return `from=${range.from}&to=${range.to}${extra}`;
}

describe('no authentication of its own', () => {
  test('serves the app and the proxy to a request that carries no credentials', async () => {
    const page = await fetch(`${bff.url}/`);
    const indexes = await proxyGet('indexes');

    expect(page.status).toBe(200);
    expect(await page.text()).toContain('logbrook');
    expect(indexes.status).toBe(200);
  });

  test('answers the health check without calling Logbrook', async () => {
    const response = await fetch(`${bff.url}/healthz`);

    expect(response.status).toBe(200);
  });

  test('has no login route', async () => {
    const response = await fetch(`${bff.url}/login`, { redirect: 'manual' });

    expect(response.status).toBe(404);
  });

  test('uses the configured token whatever Authorization or cookie the client sends', async () => {
    const response = await proxyGet('indexes', bffScoped, {
      authorization: `Bearer ${TOKENS.admin}`,
      cookie: 'session=anything',
    });

    const body = (await response.json()) as { indexes: Array<Record<string, unknown>> };
    expect(response.status).toBe(200);
    expect(body.indexes[0]).toEqual({ name: expect.any(String) });
  });

  test('passes a 401 through when the configured read token is wrong', async () => {
    const response = await proxyGet(`indexes/${FIXTURE_INDEX}/logs?${rangeQuery()}`, bffWrongToken);

    expect(response.status).toBe(401);
    expect(await response.json()).toEqual({ error: expect.any(String) });
  });

  test('classifies real tokens by role', async () => {
    const deps = { upstreamUrl: logbrook.url, fetch };

    expect(await classifyToken(TOKENS.read, deps)).toBe('read');
    expect(await classifyToken(TOKENS.scopedRead, deps)).toBe('read');
    expect(await classifyToken(TOKENS.admin, deps)).toBe('admin');
    expect(await classifyToken(TOKENS.ingest, deps)).toBe('invalid');
    expect(await classifyToken('unknown-token-0123456789', deps)).toBe('invalid');
  });

  test('checks the configured tokens against the real server', async () => {
    const deps = { upstreamUrl: logbrook.url, fetch };

    expect(
      await checkConfiguredTokens({ readToken: TOKENS.read, adminToken: TOKENS.admin }, deps),
    ).toEqual([]);
    expect(
      await checkConfiguredTokens({ readToken: TOKENS.admin, adminToken: TOKENS.read }, deps),
    ).toHaveLength(2);
    expect(
      await checkConfiguredTokens(
        { readToken: 'unknown-token-0123456789', adminToken: null },
        deps,
      ),
    ).toEqual([expect.stringContaining('LOGBROOK_READ_TOKEN')]);
  });
});

describe('proxy', () => {
  test('lists indexes with the read token even when an admin token is configured too', async () => {
    const scoped = await proxyGet('indexes', bffScoped);
    const full = await proxyGet('indexes', bffWithAdmin);

    const scopedList = (await scoped.json()) as { indexes: Array<Record<string, unknown>> };
    const fullList = (await full.json()) as { indexes: Array<Record<string, unknown>> };
    expect(scopedList.indexes[0]).toEqual({ name: expect.any(String) });
    expect(fullList.indexes.find((index) => index.name === FIXTURE_INDEX)).toMatchObject({
      size_bytes: expect.any(Number),
    });
  });

  test('serves /metrics to an unrestricted read token and refuses a source-restricted one', async () => {
    const allowed = await proxyGet('metrics');
    const refused = await proxyGet('metrics', bffScoped);

    expect(allowed.status).toBe(200);
    expect(await allowed.text()).toContain('logbrook_ingested_events_total');
    expect(refused.status).toBe(403);
    expect(await refused.json()).toEqual({ error: expect.any(String) });
  });

  test('pages through every event with cursors and no duplicates', async () => {
    const ids: string[] = [];
    let cursor: string | null = null;
    let pages = 0;

    do {
      const suffix: string = cursor === null ? '' : `&cursor=${encodeURIComponent(cursor)}`;
      const response = await proxyGet(
        `indexes/${FIXTURE_INDEX}/logs?${rangeQuery(`&limit=500${suffix}`)}`,
      );
      expect(response.status).toBe(200);
      const page = (await response.json()) as {
        events: Array<{ id: string }>;
        next_cursor: string | null;
      };
      ids.push(...page.events.map((event) => event.id));
      cursor = page.next_cursor;
      pages += 1;
    } while (cursor !== null);

    expect(pages).toBe(3);
    expect(ids).toHaveLength(FIXTURE_EVENT_COUNT);
    expect(new Set(ids).size).toBe(FIXTURE_EVENT_COUNT);
  });

  test('serves facets and a histogram whose buckets sum to the event count', async () => {
    const facets = await proxyGet(`indexes/${FIXTURE_INDEX}/logs/facets?${rangeQuery()}`);
    const histogram = await proxyGet(
      `indexes/${FIXTURE_INDEX}/logs/histogram?${rangeQuery('&interval_ms=60000')}`,
    );

    const facetBody = (await facets.json()) as {
      services: Array<{ value: string; count: number }>;
    };
    const histogramBody = (await histogram.json()) as { buckets: Array<{ count: number }> };
    expect(facetBody.services.map((s) => s.value).sort()).toEqual(['billing', 'checkout']);
    const total = histogramBody.buckets.reduce((sum, bucket) => sum + bucket.count, 0);
    expect(total).toBe(FIXTURE_EVENT_COUNT);
  });

  test('passes error statuses and bodies through: 400, 403, 404', async () => {
    const badCursor = await proxyGet(
      `indexes/${FIXTURE_INDEX}/logs?${rangeQuery('&cursor=bogus')}`,
      bffScoped,
    );
    const forbidden = await proxyGet(
      `indexes/${FIXTURE_INDEX}/logs?${rangeQuery('&source=other')}`,
      bffScoped,
    );
    const missing = await proxyGet(`indexes/nope/logs?${rangeQuery()}`, bffScoped);

    expect(badCursor.status).toBe(400);
    expect(await badCursor.json()).toEqual({ error: expect.any(String) });
    expect(forbidden.status).toBe(403);
    expect(missing.status).toBe(404);
  });

  test('refuses routes outside the allowlist and non-GET methods', async () => {
    const count = await proxyGet(`indexes/${FIXTURE_INDEX}/logs/count?${rangeQuery()}`);
    const ingest = await fetch(`${bff.url}/api/lb/indexes/${FIXTURE_INDEX}/logs/ingest`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '[]',
    });
    const del = await fetch(`${bff.url}/api/lb/indexes/${FIXTURE_INDEX}`, {
      method: 'DELETE',
    });

    expect(count.status).toBe(404);
    expect(ingest.status).toBe(405);
    expect(del.status).toBe(405);
    expect(await countEvents()).toBe(FIXTURE_EVENT_COUNT);
  });

  test('compresses large JSON for clients that accept gzip, and the body is intact', async () => {
    const query = `indexes/${FIXTURE_INDEX}/logs?${rangeQuery('&limit=500')}`;

    const plain = await proxyGet(query);
    const plainText = await plain.text();
    const gzipped = await rawGet(`/api/lb/${query}`, { 'accept-encoding': 'gzip' });

    expect(gzipped.headers['content-encoding']).toBe('gzip');
    expect(gunzipSync(gzipped.body).toString()).toBe(plainText);
    expect(gzipped.body.length).toBeLessThan(plainText.length / 2);
  });
});

describe('index management with the admin token', () => {
  test('creates and deletes an index, and refuses to delete default', async () => {
    const deps = { upstreamUrl: logbrook.url, fetch };

    await createIndex('created-by-it', TOKENS.admin, deps);
    const listed = await listIndexes(TOKENS.admin, deps);
    await deleteIndex('created-by-it', TOKENS.admin, deps);

    expect(listed.map((index) => index.name)).toContain('created-by-it');
    expect(await rejection(deleteIndex('default', TOKENS.admin, deps))).toMatchObject({
      status: 400,
    });
    expect(await rejection(createIndex('x-y', TOKENS.read, deps))).toMatchObject({ status: 401 });
  });

  test('answers 429 at the index limit and lists sizes only to the admin and unrestricted tokens', async () => {
    const deps = { upstreamUrl: logbrook.url, fetch };
    const created: string[] = [];
    let refusal: unknown = null;
    for (const name of ['limit-a', 'limit-b', 'limit-c', 'limit-d', 'limit-e']) {
      try {
        await createIndex(name, TOKENS.admin, deps);
        created.push(name);
      } catch (error) {
        refusal = error;
        break;
      }
    }

    const asAdmin = await listIndexes(TOKENS.admin, deps);
    const asScoped = await listIndexes(TOKENS.scopedRead, deps);
    for (const name of created) {
      await deleteIndex(name, TOKENS.admin, deps);
    }

    expect(refusal).toMatchObject({ status: 429 });
    expect(asAdmin[0]).toMatchObject({ size_bytes: expect.any(Number) });
    expect(asScoped[0]).toEqual({ name: expect.any(String) });
  });
});

/**
 * Counts the fixture events directly on Logbrook.
 */
async function countEvents(): Promise<number> {
  const response = await fetch(
    `${logbrook.url}/indexes/${FIXTURE_INDEX}/logs/count?${rangeQuery()}`,
    { headers: { authorization: `Bearer ${TOKENS.read}` } },
  );
  return ((await response.json()) as { count: number }).count;
}

/**
 * GETs through `node:http` so the response is not decompressed, which `fetch` would do.
 */
function rawGet(
  path: string,
  headers: Record<string, string>,
): Promise<{ headers: Record<string, string | string[] | undefined>; body: Buffer }> {
  return new Promise((resolve, reject) => {
    const url = new URL(bff.url);
    const request = httpRequest(
      { host: url.hostname, port: url.port, path, headers },
      (response) => {
        const chunks: Buffer[] = [];
        response.on('data', (chunk: Buffer) => chunks.push(chunk));
        response.on('end', () =>
          resolve({ headers: response.headers, body: Buffer.concat(chunks) }),
        );
      },
    );
    request.once('error', reject);
    request.end();
  });
}
