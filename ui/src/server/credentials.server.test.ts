import { describe, expect, it } from 'vite-plus/test';

import { LogbrookError } from '../lib/logbrook/errors.ts';
import { requestUrl } from '../test-support/request-url.ts';
import {
  checkConfiguredTokens,
  classifyToken,
  createIndex,
  deleteIndex,
} from './credentials.server.ts';

const READ = 'read-token-0123456789';
const ADMIN = 'admin-token-0123456789';

/**
 * Fake Logbrook: answers by token and path the way the real server's role rules do. Anything else
 * is a `401`.
 */
function fakeLogbrook(calls: string[] = []): typeof fetch {
  return async (input, init) => {
    const url = new URL(requestUrl(input));
    const token = new Headers(init?.headers).get('authorization')?.replace('Bearer ', '');
    calls.push(`${init?.method ?? 'GET'} ${url.pathname} ${token}`);
    const isCount = url.pathname.endsWith('/logs/count');
    if (url.pathname === '/indexes' && (token === READ || token === ADMIN)) {
      return Response.json({ indexes: [] });
    }

    if (isCount && token === READ) {
      return Response.json({ error: 'from is required' }, { status: 400 });
    }

    if (init?.method === 'PUT' && token === ADMIN) {
      return Response.json({ name: 'x' });
    }

    if (init?.method === 'DELETE' && token === ADMIN) {
      return Response.json({ error: 'default cannot be deleted' }, { status: 400 });
    }

    return Response.json({ error: 'invalid token' }, { status: 401 });
  };
}

const deps = { upstreamUrl: 'http://lb', fetch: fakeLogbrook() };

describe('classifyToken', () => {
  it('tells read, admin and invalid tokens apart', async () => {
    expect(await classifyToken(READ, deps)).toBe('read');
    expect(await classifyToken(ADMIN, deps)).toBe('admin');
    expect(await classifyToken('nope-nope-nope-nope', deps)).toBe('invalid');
  });

  it('throws for a response that says nothing about the token, after the retries', async () => {
    const waits: number[] = [];
    let calls = 0;
    const busy = {
      upstreamUrl: 'http://lb',
      fetch: async () => {
        calls += 1;
        return Response.json({ error: 'busy' }, { status: 429, headers: { 'retry-after': '1' } });
      },
      sleep: async (millis: number) => {
        waits.push(millis);
      },
    };

    await expect(classifyToken(READ, busy)).rejects.toBeInstanceOf(LogbrookError);
    expect(waits.slice(0, 3)).toEqual([1000, 1000, 1000]);
    // Two requests per attempt (list and probe), four attempts each.
    expect(calls).toBe(8);
  });

  it('repeats a 429 on either request, so a busy server never decides the role', async () => {
    const inner = fakeLogbrook();
    const busyFirst = new Set<string>();
    const waits: number[] = [];
    const flaky = {
      upstreamUrl: 'http://lb',
      fetch: (async (input, init) => {
        const path = new URL(requestUrl(input)).pathname;
        if (!busyFirst.has(path)) {
          busyFirst.add(path);
          return Response.json({ error: 'busy' }, { status: 429, headers: { 'retry-after': '2' } });
        }

        return inner(input, init);
      }) as typeof fetch,
      sleep: async (millis: number) => {
        waits.push(millis);
      },
    };

    expect(await classifyToken(ADMIN, flaky)).toBe('admin');
    expect(waits).toEqual([2000, 2000]);
  });

  it('releases the body of the successful index list', async () => {
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        controller.enqueue(new TextEncoder().encode('{"indexes":[]}'));
      },
      cancel() {
        cancelled = true;
      },
    });
    const inner = fakeLogbrook();
    const spying = {
      upstreamUrl: 'http://lb',
      fetch: (async (input, init) =>
        new URL(requestUrl(input)).pathname === '/indexes'
          ? new Response(body, { headers: { 'content-type': 'application/json' } })
          : inner(input, init)) as typeof fetch,
    };

    expect(await classifyToken(READ, spying)).toBe('read');
    expect(cancelled).toBe(true);
  });
});

describe('checkConfiguredTokens', () => {
  it('has nothing to say about a read token alone, or with an admin token', async () => {
    expect(await checkConfiguredTokens({ readToken: READ, adminToken: null }, deps)).toEqual([]);
    expect(await checkConfiguredTokens({ readToken: READ, adminToken: ADMIN }, deps)).toEqual([]);
  });

  it('warns about a rejected read token, naming the variable and not the value', async () => {
    const token = 'nope-nope-nope-nope';

    const warnings = await checkConfiguredTokens({ readToken: token, adminToken: null }, deps);

    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain('LOGBROOK_READ_TOKEN');
    expect(warnings[0]).not.toContain(token);
  });

  it('warns when the read and admin tokens are swapped', async () => {
    const warnings = await checkConfiguredTokens({ readToken: ADMIN, adminToken: READ }, deps);

    expect(warnings).toHaveLength(2);
    expect(warnings[0]).toContain('LOGBROOK_READ_TOKEN is an admin token');
    expect(warnings[1]).toContain('LOGBROOK_ADMIN_TOKEN is a read token');
  });

  it('warns about a rejected admin token', async () => {
    const warnings = await checkConfiguredTokens(
      { readToken: READ, adminToken: 'nope-nope-nope-nope' },
      deps,
    );

    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toContain('LOGBROOK_ADMIN_TOKEN');
  });

  it('reports an unreachable server by its origin only', async () => {
    const down = {
      upstreamUrl: 'http://user:pass@lb:3100',
      fetch: () => Promise.reject(new TypeError('fetch failed')),
    };

    const warnings = await checkConfiguredTokens({ readToken: READ, adminToken: null }, down);

    expect(warnings).toEqual([expect.stringContaining('Cannot reach Logbrook at http://lb:3100')]);
    expect(warnings[0]).not.toContain('pass');
  });

  it('reports the status of an unexpected answer', async () => {
    const busy = {
      upstreamUrl: 'http://lb',
      fetch: async () => Response.json({ error: 'busy' }, { status: 503 }),
      sleep: async () => {},
    };

    const warnings = await checkConfiguredTokens({ readToken: READ, adminToken: null }, busy);

    expect(warnings).toEqual(['Logbrook answered 503 when the tokens were checked: busy']);
  });
});

describe('createIndex and deleteIndex', () => {
  it('create sends PUT with the admin token and the encoded name', async () => {
    const calls: string[] = [];

    await createIndex('billing', ADMIN, { upstreamUrl: 'http://lb', fetch: fakeLogbrook(calls) });

    expect(calls).toEqual([`PUT /indexes/billing ${ADMIN}`]);
  });

  it('surfaces the server refusal as a LogbrookError', async () => {
    await expect(deleteIndex('default', ADMIN, deps)).rejects.toMatchObject({
      status: 400,
      message: 'default cannot be deleted',
    });
  });

  it('rejects when the token is not an admin token', async () => {
    await expect(createIndex('x', READ, deps)).rejects.toMatchObject({ status: 401 });
  });
});
