import { describe, expect, it } from 'vite-plus/test';

import { LogbrookError, errorFromResponse, isRetryable, parseRetryAfter } from './errors.ts';

describe('parseRetryAfter', () => {
  it('parses whole seconds', () => {
    expect(parseRetryAfter('1')).toBe(1);
    expect(parseRetryAfter(' 30 ')).toBe(30);
  });

  it('returns null for missing, negative, fractional and date values', () => {
    expect(parseRetryAfter(null)).toBeNull();
    expect(parseRetryAfter('-1')).toBeNull();
    expect(parseRetryAfter('1.5')).toBeNull();
    expect(parseRetryAfter('Wed, 21 Oct 2026 07:28:00 GMT')).toBeNull();
  });
});

describe('errorFromResponse', () => {
  it('carries status, the server message and Retry-After', async () => {
    const response = Response.json(
      { error: 'too busy' },
      { status: 429, headers: { 'retry-after': '1' } },
    );

    const error = await errorFromResponse(response);

    expect(error).toBeInstanceOf(LogbrookError);
    expect(error.status).toBe(429);
    expect(error.message).toBe('too busy');
    expect(error.retryAfterSeconds).toBe(1);
  });

  it('falls back to the status line for a body that is not an error object', async () => {
    const response = new Response('<html>bad gateway</html>', {
      status: 502,
      statusText: 'Bad Gateway',
    });

    const error = await errorFromResponse(response);

    expect(error.message).toBe('502 Bad Gateway');
    expect(error.retryAfterSeconds).toBeNull();
  });

  it('falls back when the JSON has no error string', async () => {
    const error = await errorFromResponse(Response.json({ error: 5 }, { status: 400 }));

    expect(error.message).toBe('400');
  });
});

describe('isRetryable', () => {
  it('retries 429 and 503 only', () => {
    expect(isRetryable(new LogbrookError(429, 'x', null))).toBe(true);
    expect(isRetryable(new LogbrookError(503, 'x', null))).toBe(true);
    expect(isRetryable(new LogbrookError(400, 'x', null))).toBe(false);
    expect(isRetryable(new LogbrookError(504, 'x', null))).toBe(false);
    expect(isRetryable(new Error('network'))).toBe(false);
  });
});
