import { gunzipSync } from 'node:zlib';
import { describe, expect, it } from 'vite-plus/test';

import { requestUrl } from '../test-support/request-url.ts';
import { handleProxyRequest } from './proxy.server.ts';

const READ = 'read-token-0123456789';

/**
 * What the fake upstream saw of a request.
 */
type Seen = {
  url: string;
  headers: Headers;
  method: string | undefined;
  signal: AbortSignal | undefined;
};

/**
 * Builds proxy dependencies over a fake fetch that records the request and answers `respond`.
 */
function setup(respond: () => Response | Promise<Response>) {
  const seen: Seen[] = [];
  const fakeFetch: typeof fetch = async (input, init) => {
    seen.push({
      url: requestUrl(input),
      headers: new Headers(init?.headers),
      method: init?.method,
      signal: init?.signal ?? undefined,
    });
    return respond();
  };
  return { seen, deps: { upstreamUrl: 'http://lb:3100', readToken: READ, fetch: fakeFetch } };
}

/**
 * Sends a GET through the proxy.
 */
function get(
  path: string,
  deps: ReturnType<typeof setup>['deps'],
  options: { headers?: Record<string, string>; signal?: AbortSignal } = {},
) {
  const request = new Request(`http://ui.test/api/lb/${path}`, {
    headers: options.headers ?? {},
    ...(options.signal === undefined ? {} : { signal: options.signal }),
  });
  return handleProxyRequest(request, deps);
}

describe('handleProxyRequest', () => {
  it('forwards the path, the query string and the bearer token, and not the cookie', async () => {
    const { seen, deps } = setup(() => Response.json({ events: [], next_cursor: null }));

    const response = await get('indexes/a/logs?from=1&to=2&message=a%20b', deps, {
      headers: { cookie: 'other_cookie=secret', accept: 'application/json' },
    });

    expect(response.status).toBe(200);
    expect(seen[0]?.url).toBe('http://lb:3100/indexes/a/logs?from=1&to=2&message=a%20b');
    expect(seen[0]?.headers.get('authorization')).toBe(`Bearer ${READ}`);
    expect(seen[0]?.headers.get('cookie')).toBeNull();
    expect(seen[0]?.headers.get('accept')).toBe('application/json');
  });

  it('forwards Last-Event-ID', async () => {
    const { seen, deps } = setup(
      () => new Response('', { headers: { 'content-type': 'text/event-stream' } }),
    );

    await get('indexes/a/logs/tail', deps, { headers: { 'last-event-id': 'a:v1.xyz' } });

    expect(seen[0]?.headers.get('last-event-id')).toBe('a:v1.xyz');
  });

  it('turns the last_event_id parameter into the Last-Event-ID header and drops it', async () => {
    const { seen, deps } = setup(
      () => new Response('', { headers: { 'content-type': 'text/event-stream' } }),
    );

    await get('indexes/a/logs/tail?service=x%20y&last_event_id=a%3Av1.xyz&host=h', deps);

    expect(seen[0]?.url).toBe('http://lb:3100/indexes/a/logs/tail?service=x%20y&host=h');
    expect(seen[0]?.headers.get('last-event-id')).toBe('a:v1.xyz');
  });

  it('rejects a last_event_id that is not a plain opaque token, before calling upstream', async () => {
    const { seen, deps } = setup(() => Response.json({}));

    for (const bad of ['a%0d%0aX-Evil%3A%201', 'has%20space', '', 'a'.repeat(1_101), '%00']) {
      const response = await get(`indexes/a/logs/tail?last_event_id=${bad}`, deps);
      expect(response.status, bad).toBe(400);
    }

    expect(seen).toHaveLength(0);
  });

  it('answers 400 instead of failing on a malformed percent-escape next to a resume ID', async () => {
    const { seen, deps } = setup(() => Response.json({}));

    const response = await get('indexes/a/logs/tail?last_event_id=abc&%E0%A4%A=1', deps);

    expect(response.status).toBe(400);
    expect(seen).toHaveLength(0);
  });

  it('honours last_event_id only on the tail route', async () => {
    const { seen, deps } = setup(() => Response.json({ events: [], next_cursor: null }));

    await get('indexes/a/logs?from=1&to=2&last_event_id=a%3Av1.xyz', deps);

    expect(seen[0]?.headers.has('last-event-id')).toBe(false);
    expect(seen[0]?.url).toContain('last_event_id=');
  });

  it('sends nothing but the fixed set of headers upstream', async () => {
    const { seen, deps } = setup(
      () => new Response('', { headers: { 'content-type': 'text/event-stream' } }),
    );

    await get('indexes/a/logs/tail?last_event_id=a%3Av1.xyz', deps, {
      headers: {
        cookie: 'other_cookie=secret',
        authorization: 'Bearer attacker',
        'x-forwarded-for': '1.2.3.4',
        accept: 'text/event-stream',
      },
    });

    expect([...(seen[0]?.headers.keys() ?? [])].sort()).toEqual([
      'accept',
      'accept-encoding',
      'authorization',
      'last-event-id',
    ]);
    expect(seen[0]?.headers.get('authorization')).toBe(`Bearer ${READ}`);
  });

  it('passes status, error body and Retry-After through', async () => {
    const { deps } = setup(() =>
      Response.json({ error: 'busy' }, { status: 429, headers: { 'retry-after': '1' } }),
    );

    const response = await get('indexes/a/logs?from=1&to=2', deps);

    expect(response.status).toBe(429);
    expect(response.headers.get('retry-after')).toBe('1');
    expect(await response.json()).toEqual({ error: 'busy' });
  });

  it('refuses methods other than GET and paths outside the allowlist without calling upstream', async () => {
    const { seen, deps } = setup(() => Response.json({}));

    const post = await handleProxyRequest(
      new Request('http://ui.test/api/lb/indexes', { method: 'POST' }),
      deps,
    );
    const count = await get('indexes/a/logs/count?from=1&to=2', deps);
    const ingest = await get('indexes/a/logs/ingest', deps);

    expect(post.status).toBe(405);
    expect(post.headers.get('allow')).toBe('GET');
    expect(count.status).toBe(404);
    expect(ingest.status).toBe(404);
    expect(seen).toHaveLength(0);
  });

  it('answers 502 when Logbrook cannot be reached', async () => {
    const { deps } = setup(() => Promise.reject(new TypeError('fetch failed')));

    const response = await get('indexes', deps);

    expect(response.status).toBe(502);
    expect(await response.json()).toEqual({ error: 'Logbrook is unreachable' });
  });

  it('hands the browser abort signal to the upstream request', async () => {
    const { seen, deps } = setup(() => Response.json({}));
    const controller = new AbortController();

    await get('indexes', deps, { signal: controller.signal });
    controller.abort();

    expect(seen[0]?.signal?.aborted).toBe(true);
  });

  it('ends a tail stream when the process shuts down, and leaves other requests alone', async () => {
    const shutdown = new AbortController();
    const seen: Array<{ path: string; signal: AbortSignal | undefined }> = [];
    const fakeFetch: typeof fetch = async (input, init) => {
      seen.push({ path: requestUrl(input), signal: init?.signal ?? undefined });
      return new Response(
        new ReadableStream({
          start(controller) {
            init?.signal?.addEventListener('abort', () => controller.error(init.signal?.reason));
          },
        }),
        { headers: { 'content-type': 'text/event-stream' } },
      );
    };
    const deps = {
      upstreamUrl: 'http://lb:3100',
      readToken: READ,
      fetch: fakeFetch,
      shutdownSignal: shutdown.signal,
    };

    const tail = await get('indexes/a/logs/tail', deps);
    await get('indexes/a/logs', deps);
    shutdown.abort(new Error('SIGTERM'));

    const [tailRequest, queryRequest] = seen;
    expect(tailRequest?.signal?.aborted).toBe(true);
    expect(queryRequest?.signal?.aborted).toBe(false);
    await expect(tail.text()).rejects.toThrow('SIGTERM');
  });

  it('never marks the event stream as compressible and disables caching and proxy buffering', async () => {
    const body = 'x'.repeat(5000);
    const { deps } = setup(
      () =>
        new Response(body, {
          headers: { 'content-type': 'text/event-stream', 'content-length': '5000' },
        }),
    );

    const response = await get('indexes/a/logs/tail', deps, {
      headers: { 'accept-encoding': 'gzip' },
    });

    expect(response.headers.get('content-encoding')).toBeNull();
    expect(response.headers.get('cache-control')).toBe('no-cache, no-transform');
    expect(response.headers.get('x-accel-buffering')).toBe('no');
    expect(await response.text()).toBe(body);
  });

  it('gzips large JSON for a browser that accepts it', async () => {
    const payload = JSON.stringify({
      events: Array.from({ length: 200 }, (_, i) => ({ id: i, message: 'hello world' })),
    });
    const { deps } = setup(
      () =>
        new Response(payload, {
          headers: { 'content-type': 'application/json', 'content-length': String(payload.length) },
        }),
    );

    const response = await get('indexes/a/logs?from=1&to=2', deps, {
      headers: { 'accept-encoding': 'gzip, br' },
    });

    expect(response.headers.get('content-encoding')).toBe('gzip');
    expect(response.headers.get('content-length')).toBeNull();
    expect(response.headers.get('vary')).toContain('accept-encoding');
    const compressed = Buffer.from(await response.arrayBuffer());
    expect(gunzipSync(compressed).toString()).toBe(payload);
    expect(compressed.length).toBeLessThan(payload.length / 2);
  });

  it('leaves small JSON and clients without gzip alone', async () => {
    const small = JSON.stringify({ buckets: [] });
    const large = 'x'.repeat(5000);
    const { deps: smallDeps } = setup(
      () =>
        new Response(small, {
          headers: { 'content-type': 'application/json', 'content-length': String(small.length) },
        }),
    );
    const { deps: largeDeps } = setup(
      () =>
        new Response(large, {
          headers: { 'content-type': 'application/json', 'content-length': '5000' },
        }),
    );

    const smallResponse = await get('indexes', smallDeps, {
      headers: { 'accept-encoding': 'gzip' },
    });
    const largeResponse = await get('indexes', largeDeps, {
      headers: { 'accept-encoding': 'identity' },
    });

    expect(smallResponse.headers.get('content-encoding')).toBeNull();
    expect(largeResponse.headers.get('content-encoding')).toBeNull();
    expect(largeResponse.headers.get('content-length')).toBe('5000');
  });

  it('does not compress error bodies', async () => {
    const { deps } = setup(() => Response.json({ error: 'x'.repeat(3000) }, { status: 400 }));

    const response = await get('indexes', deps, { headers: { 'accept-encoding': 'gzip' } });

    expect(response.headers.get('content-encoding')).toBeNull();
  });
});
