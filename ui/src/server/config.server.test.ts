import { describe, expect, it } from 'vite-plus/test';

import { parseServerConfig } from './config.server.ts';

const READ = 'read-token-0123456789';
const ADMIN = 'admin-token-0123456789';

describe('parseServerConfig', () => {
  it('reads the settings and strips trailing slashes from the URL', () => {
    const config = parseServerConfig({
      LOGBROOK_URL: 'http://lb:3100//',
      LOGBROOK_READ_TOKEN: READ,
      LOGBROOK_ADMIN_TOKEN: ADMIN,
    });

    expect(config).toEqual({ logbrookUrl: 'http://lb:3100', readToken: READ, adminToken: ADMIN });
  });

  it('defaults the URL to a local server and the admin token to none', () => {
    expect(parseServerConfig({ LOGBROOK_READ_TOKEN: READ })).toEqual({
      logbrookUrl: 'http://127.0.0.1:3100',
      readToken: READ,
      adminToken: null,
    });
  });

  it('fails without a read token, naming the variable', () => {
    expect(() => parseServerConfig({})).toThrow(/LOGBROOK_READ_TOKEN is required/);
  });

  it.each(['', '   ', '\t\n'])('treats the empty value %j as not set', (empty) => {
    expect(() => parseServerConfig({ LOGBROOK_READ_TOKEN: empty })).toThrow(/LOGBROOK_READ_TOKEN/);
    expect(
      parseServerConfig({
        LOGBROOK_URL: empty,
        LOGBROOK_READ_TOKEN: READ,
        LOGBROOK_ADMIN_TOKEN: empty,
      }),
    ).toEqual({ logbrookUrl: 'http://127.0.0.1:3100', readToken: READ, adminToken: null });
  });

  it.each([
    ['too short', 'short'],
    ['too long', 'x'.repeat(513)],
    ['with a space', 'read token 0123456789'],
    ['with a trailing newline', `${READ}\n`],
    ['not ASCII', 'read-token-0123456789é'],
  ])('rejects a token that is %s, without quoting it', (_label, token) => {
    const attempt = () => parseServerConfig({ LOGBROOK_READ_TOKEN: token });

    expect(attempt).toThrow(/LOGBROOK_READ_TOKEN must be 16 to 512 visible ASCII/);
    expect(attempt).not.toThrow(token);
    expect(() =>
      parseServerConfig({ LOGBROOK_READ_TOKEN: READ, LOGBROOK_ADMIN_TOKEN: token }),
    ).toThrow(/LOGBROOK_ADMIN_TOKEN must be/);
  });

  it('accepts tokens at the length limits', () => {
    expect(parseServerConfig({ LOGBROOK_READ_TOKEN: 'a'.repeat(16) }).readToken).toHaveLength(16);
    expect(parseServerConfig({ LOGBROOK_READ_TOKEN: 'a'.repeat(512) }).readToken).toHaveLength(512);
  });

  it('rejects identical read and admin tokens', () => {
    expect(() =>
      parseServerConfig({ LOGBROOK_READ_TOKEN: READ, LOGBROOK_ADMIN_TOKEN: READ }),
    ).toThrow(/must be different/);
  });

  it.each(['not a url', 'ftp://host', 'logbrook:3100'])('rejects the URL %j', (url) => {
    expect(() => parseServerConfig({ LOGBROOK_URL: url, LOGBROOK_READ_TOKEN: READ })).toThrow(
      /LOGBROOK_URL/,
    );
  });
});
