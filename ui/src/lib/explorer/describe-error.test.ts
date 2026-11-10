import { describe, expect, it } from 'vite-plus/test';

import { LogbrookError } from '@/lib/logbrook/errors';

import { describeError, nextPageOptions } from './describe-error.ts';

describe('describeError', () => {
  it('quotes the server for a 400, with no retry', () => {
    const result = describeError(new LogbrookError(400, 'filter.attributes[1].op: bad', null));
    expect(result.action).toBe('none');
    expect(result.serverMessage).toBe('filter.attributes[1].op: bad');
  });

  it('tells the reader the server token was rejected on a 401, with nothing to retry', () => {
    const result = describeError(new LogbrookError(401, 'invalid token', null));

    expect(result.detail).toContain('LOGBROOK_READ_TOKEN');
    expect(result.action).toBe('none');
    expect(nextPageOptions(new LogbrookError(401, 'invalid token', null)).retry).toBe(false);
  });

  it('explains 403 and 404 differently', () => {
    expect(describeError(new LogbrookError(403, 'x', null)).title).not.toBe(
      describeError(new LogbrookError(404, 'x', null)).title,
    );
  });

  it('suggests narrowing for 413 and 504', () => {
    expect(describeError(new LogbrookError(413, 'x', null)).action).toBe('narrow');
    expect(describeError(new LogbrookError(504, 'x', null)).action).toBe('narrow');
  });

  it('carries Retry-After for a 429', () => {
    const result = describeError(new LogbrookError(429, 'busy', 1));
    expect(result.action).toBe('wait');
    expect(result.retryAfterSeconds).toBe(1);
  });

  it('describes a network failure', () => {
    expect(describeError(new TypeError('Failed to fetch')).action).toBe('retry');
  });
});

describe('nextPageOptions', () => {
  it('offers a restart, not a retry, when the cursor was refused', () => {
    expect(nextPageOptions(new LogbrookError(400, 'cursor expired', null))).toEqual({
      retry: false,
      restart: true,
      narrow: false,
    });
  });

  it('offers a retry for a failed page, and narrowing when the page was too big or slow', () => {
    expect(nextPageOptions(new LogbrookError(500, 'boom', null))).toMatchObject({
      retry: true,
      restart: false,
      narrow: false,
    });
    expect(nextPageOptions(new LogbrookError(504, 'slow', null))).toMatchObject({
      retry: true,
      narrow: true,
    });
    expect(nextPageOptions(new TypeError('Failed to fetch'))).toMatchObject({ retry: true });
  });

  it('offers nothing for a 401, which the explorer handles by itself', () => {
    expect(nextPageOptions(new LogbrookError(401, 'x', null))).toEqual({
      retry: false,
      restart: false,
      narrow: false,
    });
  });
});
