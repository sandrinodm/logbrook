import { describe, expect, it } from 'vite-plus/test';

import {
  DEFAULT_BIND_HOST,
  applyDefaultHost,
  bindHostOf,
  createHostGuard,
  isLoopbackAddress,
  parseAllowedHosts,
} from './host-guard.server.ts';

/**
 * A request for `/` with the given `Host` header (or none).
 */
function requestFor(host: string | null): Request {
  return new Request('http://internal.test/', host === null ? {} : { headers: { host } });
}

describe('applyDefaultHost', () => {
  it('binds loopback when neither HOST nor NITRO_HOST is set', () => {
    const env: Record<string, string | undefined> = {};

    applyDefaultHost(env);

    expect(env.HOST).toBe(DEFAULT_BIND_HOST);
    expect(DEFAULT_BIND_HOST).toBe('127.0.0.1');
  });

  it.each(['', '  '])('treats the empty value %j as not set', (empty) => {
    const env: Record<string, string | undefined> = { HOST: empty, NITRO_HOST: empty };

    applyDefaultHost(env);

    expect(env.HOST).toBe('127.0.0.1');
  });

  it('leaves an explicit HOST or NITRO_HOST alone', () => {
    const host: Record<string, string | undefined> = { HOST: '0.0.0.0' };
    const nitro: Record<string, string | undefined> = { NITRO_HOST: '10.0.0.5' };

    applyDefaultHost(host);
    applyDefaultHost(nitro);

    expect(host).toEqual({ HOST: '0.0.0.0' });
    expect(nitro).toEqual({ NITRO_HOST: '10.0.0.5' });
  });
});

describe('bindHostOf', () => {
  it('prefers NITRO_HOST, as the Nitro server does, then HOST, then loopback', () => {
    expect(bindHostOf({ NITRO_HOST: '10.0.0.5', HOST: '0.0.0.0' })).toBe('10.0.0.5');
    expect(bindHostOf({ HOST: '0.0.0.0' })).toBe('0.0.0.0');
    expect(bindHostOf({})).toBe('127.0.0.1');
  });
});

describe('isLoopbackAddress', () => {
  it.each(['127.0.0.1', '127.1.2.3', 'localhost', '::1', '[::1]', 'LOCALHOST'])(
    'accepts %s',
    (host) => {
      expect(isLoopbackAddress(host)).toBe(true);
    },
  );

  it.each(['0.0.0.0', '::', '10.0.0.5', '192.168.1.2', 'example.com', '1127.0.0.1'])(
    'rejects %s',
    (host) => {
      expect(isLoopbackAddress(host)).toBe(false);
    },
  );
});

describe('parseAllowedHosts', () => {
  it('is null when unset or empty', () => {
    expect(parseAllowedHosts(undefined)).toBeNull();
    expect(parseAllowedHosts('')).toBeNull();
    expect(parseAllowedHosts(' , ')).toBeNull();
  });

  it('splits on commas, trims, lower-cases and ignores ports', () => {
    expect(parseAllowedHosts(' Logs.Example.com , ui.internal:8080,[::1]:3000 ')).toEqual([
      'logs.example.com',
      'ui.internal',
      '[::1]',
    ]);
  });

  it.each(['https://example.com', 'example.com/path', '*.example.com', 'a b', 'ex ample.com'])(
    'rejects %s, naming the variable and the entry',
    (entry) => {
      expect(() => parseAllowedHosts(`ok.example.com,${entry}`)).toThrow(
        new RegExp(`ALLOWED_HOSTS entry "${entry.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}"`),
      );
    },
  );
});

describe('createHostGuard', () => {
  it('allows only the loopback names when bound to loopback without ALLOWED_HOSTS', () => {
    const guard = createHostGuard({});

    expect(guard.warning).toBeNull();
    for (const host of ['localhost:3000', '127.0.0.1:3000', '[::1]:3000', 'LocalHost']) {
      expect(guard.check(requestFor(host))).toBeNull();
    }

    for (const host of ['evil.example.com', 'evil.example.com:3000', '127.0.0.1.evil.test', '']) {
      expect(guard.check(requestFor(host))?.status).toBe(421);
    }
  });

  it('refuses with a short plain-text body that does not echo the header', async () => {
    const response = createHostGuard({}).check(requestFor('attacker.example'));

    expect(response?.status).toBe(421);
    expect(response?.headers.get('content-type')).toContain('text/plain');
    const body = (await response?.text()) ?? '';
    expect(body).toContain('ALLOWED_HOSTS');
    expect(body).not.toContain('attacker');
    expect(body.length).toBeLessThan(200);
  });

  it('allows the configured hosts, ports ignored, plus the loopback names', () => {
    const guard = createHostGuard({ ALLOWED_HOSTS: 'logs.example.com', HOST: '0.0.0.0' });

    expect(guard.warning).toBeNull();
    expect(guard.check(requestFor('logs.example.com'))).toBeNull();
    expect(guard.check(requestFor('LOGS.example.com:8443'))).toBeNull();
    expect(guard.check(requestFor('127.0.0.1:3000'))).toBeNull();
    expect(guard.check(requestFor('other.example.com'))?.status).toBe(421);
  });

  it('enforces ALLOWED_HOSTS on a loopback bind too', () => {
    const guard = createHostGuard({ ALLOWED_HOSTS: 'logs.example.com' });

    expect(guard.check(requestFor('logs.example.com'))).toBeNull();
    expect(guard.check(requestFor('evil.example.com'))?.status).toBe(421);
  });

  it.each([{ HOST: '0.0.0.0' }, { NITRO_HOST: '10.0.0.5' }, { HOST: '::' }])(
    'accepts any host and warns once when bound to %j without ALLOWED_HOSTS',
    (env) => {
      const guard = createHostGuard(env);

      expect(guard.warning).toContain('ALLOWED_HOSTS');
      expect(guard.check(requestFor('anything.example'))).toBeNull();
    },
  );

  it('falls back to the URL host for a request without a Host header', () => {
    const guard = createHostGuard({});

    expect(guard.check(new Request('http://localhost:3000/x'))).toBeNull();
    expect(guard.check(new Request('http://evil.example/x'))?.status).toBe(421);
  });
});
