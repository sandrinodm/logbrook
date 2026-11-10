import { afterAll, beforeAll, describe, expect, test } from 'e2e';

import type { LogbrookServer } from './support/logbrook-server.ts';

import { runBffUntilExit, startBff } from './support/bff-server.ts';
import { freePort } from './support/free-port.ts';
import { teardown } from './support/helpers.ts';
import { TOKENS, startLogbrook } from './support/logbrook-server.ts';

/**
 * A token Logbrook does not know, with a valid shape.
 */
const UNKNOWN_TOKEN = 'unknown-token-0123456789';

let logbrook: LogbrookServer;

beforeAll(async () => {
  logbrook = await startLogbrook();
});

afterAll(async () => {
  await teardown(() => logbrook?.stop());
});

/**
 * Polls the BFF's output until `text` appears, or fails after the deadline.
 */
async function waitForOutput(output: () => string, text: string): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    if (output().includes(text)) {
      return;
    }

    await new Promise((done) => setTimeout(done, 50));
  }

  throw new Error(`"${text}" never appeared in:\n${output()}`);
}

describe('startup validation', () => {
  test('stops at boot when the read token is missing, naming the variable', async () => {
    const result = await runBffUntilExit({ LOGBROOK_URL: logbrook.url, PORT: '0' });

    expect(result.code).not.toBe(0);
    expect(result.output).toContain('LOGBROOK_READ_TOKEN is required');
  });

  test('treats an empty or whitespace-only read token as missing', async () => {
    const result = await runBffUntilExit({
      LOGBROOK_URL: logbrook.url,
      LOGBROOK_READ_TOKEN: '   ',
      PORT: '0',
    });

    expect(result.code).not.toBe(0);
    expect(result.output).toContain('LOGBROOK_READ_TOKEN is required');
  });

  test('rejects malformed and identical tokens without printing their values', async () => {
    const malformed = 'has a space in it 0123456789';
    const bad = await runBffUntilExit({
      LOGBROOK_URL: logbrook.url,
      LOGBROOK_READ_TOKEN: TOKENS.read,
      LOGBROOK_ADMIN_TOKEN: malformed,
      PORT: '0',
    });
    const same = await runBffUntilExit({
      LOGBROOK_URL: logbrook.url,
      LOGBROOK_READ_TOKEN: TOKENS.read,
      LOGBROOK_ADMIN_TOKEN: TOKENS.read,
      PORT: '0',
    });

    expect(bad.code).not.toBe(0);
    expect(bad.output).toContain('LOGBROOK_ADMIN_TOKEN must be');
    expect(bad.output).not.toContain(malformed);
    expect(same.code).not.toBe(0);
    expect(same.output).toContain('must be different');
    expect(same.output).not.toContain(TOKENS.read);
  });
});

describe('startup check against Logbrook', () => {
  test('logs nothing alarming when the tokens are right', async () => {
    const bff = await startBff(logbrook.url, { read: TOKENS.read, admin: TOKENS.admin });
    try {
      await new Promise((done) => setTimeout(done, 1_000));

      expect(bff.output()).not.toContain('[logbrook-ui]');
    } finally {
      await bff.stop();
    }
  });

  test('warns about a rejected token, names the variable and never prints a token', async () => {
    const bff = await startBff(logbrook.url, { read: UNKNOWN_TOKEN, admin: TOKENS.admin });
    try {
      await waitForOutput(bff.output, 'Logbrook rejected LOGBROOK_READ_TOKEN');

      expect(bff.output()).not.toContain(UNKNOWN_TOKEN);
      expect(bff.output()).not.toContain(TOKENS.admin);
    } finally {
      await bff.stop();
    }
  });

  test('starts and warns when Logbrook is unreachable', async () => {
    const closedPort = await freePort();
    const bff = await startBff(`http://127.0.0.1:${closedPort}`, { read: TOKENS.read });
    try {
      await waitForOutput(bff.output, `Cannot reach Logbrook at http://127.0.0.1:${closedPort}`);

      expect(bff.output()).not.toContain(TOKENS.read);
    } finally {
      await bff.stop();
    }
  });
});
