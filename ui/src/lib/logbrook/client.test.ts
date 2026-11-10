import { describe, expect, it } from 'vite-plus/test';

import { requestUrl } from '../../test-support/request-url.ts';
import { createLogbrookClient } from './client.ts';
import { LogbrookError } from './errors.ts';

/**
 * A request seen by the fake fetch.
 */
type Seen = { url: string; headers: Headers; signal: AbortSignal | undefined };

/**
 * Builds a client over a fake fetch that records requests and answers with `respond`.
 */
function setup(respond: () => Response) {
  const seen: Seen[] = [];
  const fakeFetch: typeof fetch = async (input, init) => {
    seen.push({
      url: requestUrl(input),
      headers: new Headers(init?.headers),
      signal: init?.signal ?? undefined,
    });
    return respond();
  };
  const client = createLogbrookClient({
    baseUrl: '/api/lb/',
    fetch: fakeFetch,
    authorize: (headers) => headers.set('authorization', 'Bearer t'),
  });
  return { client, seen };
}

describe('createLogbrookClient', () => {
  it('lists indexes and unwraps the envelope', async () => {
    const { client, seen } = setup(() => Response.json({ indexes: [{ name: 'default' }] }));

    const indexes = await client.listIndexes();

    expect(indexes).toEqual([{ name: 'default' }]);
    expect(seen[0]?.url).toBe('/api/lb/indexes');
    expect(seen[0]?.headers.get('authorization')).toBe('Bearer t');
  });

  it('builds the search URL with range, filters, limit and cursor, and passes the signal', async () => {
    const { client, seen } = setup(() => Response.json({ events: [], next_cursor: null }));
    const controller = new AbortController();

    await client.searchLogs(
      'my index',
      { from: 10, to: 20, service: 'api', minLevel: 50, limit: 500 },
      'cur:1',
      controller.signal,
    );

    const url = new URL(seen[0]?.url ?? '', 'http://x');
    expect(url.pathname).toBe('/api/lb/indexes/my%20index/logs');
    expect(Object.fromEntries(url.searchParams)).toEqual({
      from: '10',
      to: '20',
      service: 'api',
      min_level: '50',
      limit: '500',
      cursor: 'cur:1',
    });
    expect(seen[0]?.signal).toBe(controller.signal);
  });

  it('omits the cursor on the first page', async () => {
    const { client, seen } = setup(() => Response.json({ events: [], next_cursor: null }));

    await client.searchLogs('a', { from: 1, to: 2 }, null);

    expect(seen[0]?.url).not.toContain('cursor');
  });

  it('adds interval_ms to histogram requests', async () => {
    const { client, seen } = setup(() => Response.json({ buckets: [] }));

    await client.getHistogram('a', { from: 1, to: 2, intervalMs: 1000 });

    expect(seen[0]?.url).toContain('interval_ms=1000');
  });

  it('throws a LogbrookError with status and Retry-After for failures', async () => {
    const { client } = setup(() =>
      Response.json({ error: 'busy' }, { status: 429, headers: { 'retry-after': '1' } }),
    );

    const failure = client.getFacets('a', { from: 1, to: 2 }, {});

    await expect(failure).rejects.toMatchObject({
      name: 'LogbrookError',
      status: 429,
      retryAfterSeconds: 1,
    });
    await expect(failure).rejects.toBeInstanceOf(LogbrookError);
  });

  it('builds the tail URL from filters only', () => {
    const { client } = setup(() => Response.json({}));

    expect(client.tailUrl('a', { service: 'x' })).toBe('/api/lb/indexes/a/logs/tail?service=x');
  });
});
