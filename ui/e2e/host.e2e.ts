import { afterAll, beforeAll, describe, expect, test } from 'e2e';
import { networkInterfaces } from 'node:os';

import type { LogbrookServer } from './support/logbrook-server.ts';

import { runBffUntilExit, startBff } from './support/bff-server.ts';
import { getWithHost, teardown } from './support/helpers.ts';
import { TOKENS, startLogbrook } from './support/logbrook-server.ts';

let logbrook: LogbrookServer;

beforeAll(async () => {
  logbrook = await startLogbrook();
});

afterAll(async () => {
  await teardown(() => logbrook?.stop());
});

/**
 * The first non-loopback IPv4 address of this machine, or `undefined` on a machine with none.
 */
function lanAddress(): string | undefined {
  return Object.values(networkInterfaces())
    .flat()
    .find((entry) => entry?.family === 'IPv4' && !entry.internal)?.address;
}

/**
 * Whether a TCP connection to `host:port` is accepted.
 */
async function accepts(host: string, port: number): Promise<boolean> {
  try {
    await fetch(`http://${host}:${port}/healthz`, { signal: AbortSignal.timeout(2_000) });
    return true;
  } catch {
    return false;
  }
}

describe('default bind address', () => {
  test('listens on 127.0.0.1 only when HOST is not set', async () => {
    const bff = await startBff(logbrook.url, { read: TOKENS.read });
    try {
      expect(bff.output()).toContain(`Listening on: http://127.0.0.1:${bff.port}`);
      expect(bff.output()).not.toContain('all interfaces');
      expect(await accepts('127.0.0.1', bff.port)).toBe(true);
      // A machine without a LAN address has nothing else to try; the output line above still holds.
      const lan = lanAddress();
      const reachableOnLan = lan === undefined ? false : await accepts(lan, bff.port);
      expect(reachableOnLan, `reachable on ${lan}`).toBe(false);
    } finally {
      await bff.stop();
    }
  });

  test('treats an empty HOST as not set', async () => {
    const bff = await startBff(logbrook.url, { read: TOKENS.read }, { HOST: '' });
    try {
      expect(bff.output()).toContain(`Listening on: http://127.0.0.1:${bff.port}`);
    } finally {
      await bff.stop();
    }
  });

  test('honours an explicit HOST', async () => {
    const bff = await startBff(logbrook.url, { read: TOKENS.read }, { HOST: '0.0.0.0' });
    try {
      expect(bff.output()).toContain('all interfaces');
    } finally {
      await bff.stop();
    }
  });
});

describe('Host header check', () => {
  test('refuses unknown hosts on every kind of request before any upstream call', async () => {
    const bff = await startBff(logbrook.url, { read: TOKENS.read, admin: TOKENS.admin });
    try {
      const paths = [
        '/',
        '/healthz',
        '/api/lb/indexes',
        '/api/lb/indexes/x/logs/tail',
        '/_serverFn/anything',
        '/assets/does-not-exist.js',
      ];
      for (const path of paths) {
        const refused = await getWithHost(bff.url, path, 'rebind.attacker.example');
        expect(refused.status, path).toBe(421);
        expect(refused.body, path).toContain('ALLOWED_HOSTS');
        expect(refused.body, path).not.toContain('attacker');
      }

      for (const host of ['localhost', `127.0.0.1:${bff.port}`, `[::1]:${bff.port}`]) {
        expect((await getWithHost(bff.url, '/healthz', host)).status, host).toBe(200);
      }

      const proxied = await getWithHost(bff.url, '/api/lb/indexes', `localhost:${bff.port}`);
      expect(proxied.status).toBe(200);
    } finally {
      await bff.stop();
    }
  });

  test('accepts any host and warns once when bound to all interfaces without ALLOWED_HOSTS', async () => {
    const bff = await startBff(logbrook.url, { read: TOKENS.read }, { HOST: '0.0.0.0' });
    try {
      const response = await getWithHost(bff.url, '/healthz', 'public.example.com');

      expect(response.status).toBe(200);
      const warnings = bff.output().split('ALLOWED_HOSTS is not set').length - 1;
      expect(warnings).toBe(1);
    } finally {
      await bff.stop();
    }
  });

  test('enforces ALLOWED_HOSTS, ignores the port and does not warn', async () => {
    const bff = await startBff(
      logbrook.url,
      { read: TOKENS.read },
      { HOST: '0.0.0.0', ALLOWED_HOSTS: 'logs.example.com,other.example.com' },
    );
    try {
      expect((await getWithHost(bff.url, '/healthz', 'logs.example.com:8443')).status).toBe(200);
      expect((await getWithHost(bff.url, '/healthz', 'OTHER.example.com')).status).toBe(200);
      expect((await getWithHost(bff.url, '/healthz', 'third.example.com')).status).toBe(421);
      expect((await getWithHost(bff.url, '/healthz', '127.0.0.1')).status).toBe(200);
      expect(bff.output()).not.toContain('ALLOWED_HOSTS is not set');
    } finally {
      await bff.stop();
    }
  });

  test('stops at boot on an invalid ALLOWED_HOSTS entry, naming the variable', async () => {
    const result = await runBffUntilExit({
      LOGBROOK_URL: logbrook.url,
      LOGBROOK_READ_TOKEN: TOKENS.read,
      ALLOWED_HOSTS: 'https://logs.example.com',
      PORT: '0',
    });

    expect(result.code).not.toBe(0);
    expect(result.output).toContain('ALLOWED_HOSTS entry "https://logs.example.com"');
  });
});
