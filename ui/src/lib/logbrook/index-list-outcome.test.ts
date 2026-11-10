import { describe, expect, it } from 'vite-plus/test';

import { LogbrookError, isRetryable } from './errors.ts';
import { failureOf, settleOutcome, unwrapIndexList } from './index-list-outcome.ts';
import { retryDelayMs, shouldRetry } from './queries.ts';

describe('failureOf', () => {
  it('keeps the status, message and Retry-After of a Logbrook error', () => {
    expect(failureOf(new LogbrookError(429, 'busy', 3))).toEqual({
      ok: false,
      status: 429,
      message: 'busy',
      retryAfterSeconds: 3,
    });
  });

  it('reports anything else as an unreachable server', () => {
    expect(failureOf(new TypeError('fetch failed'))).toMatchObject({ ok: false, status: 502 });
  });
});

describe('unwrapIndexList', () => {
  it('returns the indexes of a success', () => {
    const indexes = [{ name: 'default' }];

    expect(unwrapIndexList({ ok: true, indexes })).toBe(indexes);
  });

  it('rethrows a failure as a LogbrookError that the retry policy recognises', () => {
    let thrown: unknown;
    try {
      unwrapIndexList({ ok: false, status: 429, message: 'busy', retryAfterSeconds: 4 });
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeInstanceOf(LogbrookError);
    expect(isRetryable(thrown)).toBe(true);
    expect(shouldRetry(0, thrown)).toBe(true);
    expect(retryDelayMs(0, thrown)).toBe(4000);
  });
});

describe('settleOutcome', () => {
  it('passes an outcome through', async () => {
    expect(await settleOutcome(Promise.resolve({ ok: true as const }))).toEqual({ ok: true });
  });

  it('turns a rejected call into a failure the dialog can show', async () => {
    const outcome = await settleOutcome(Promise.reject(new TypeError('Failed to fetch')));

    expect(outcome).toMatchObject({ ok: false, status: 502 });
    expect(outcome.ok ? '' : outcome.message).toContain('Could not reach the server');
  });
});
