import { describe, expect, it } from 'vite-plus/test';

import { backoffDelayMs, decideAfterFailure } from './tail-reconnect.ts';

describe('backoffDelayMs', () => {
  it('doubles from one second up to thirty', () => {
    const delays = [0, 1, 2, 3, 4, 5, 6, 20].map((attempt) => backoffDelayMs(attempt, () => 0));

    expect(delays).toEqual([1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000, 30_000]);
  });

  it('adds at most a quarter as jitter', () => {
    expect(backoffDelayMs(0, () => 0.999)).toBeLessThanOrEqual(1_250);
    expect(backoffDelayMs(0, () => 0.999)).toBeGreaterThan(1_200);
  });
});

describe('decideAfterFailure', () => {
  const none = () => 0;

  it('stops and reports the rejected token on 401', () => {
    expect(decideAfterFailure(401, null, 0, none)).toEqual({
      action: 'stop',
      unauthorized: true,
      forbidden: false,
    });
  });

  it('stops on 403 as a scope refusal, not a rejected token', () => {
    expect(decideAfterFailure(403, null, 0, none)).toEqual({
      action: 'stop',
      unauthorized: false,
      forbidden: true,
    });
  });

  it('stops on errors the request caused', () => {
    for (const status of [400, 404, 413]) {
      expect(decideAfterFailure(status, null, 0, none)).toEqual({
        action: 'stop',
        unauthorized: false,
        forbidden: false,
      });
    }
  });

  it('retries the subscriber limit, honouring Retry-After', () => {
    expect(decideAfterFailure(429, 20, 0, none)).toEqual({
      action: 'retry',
      delayMs: 20_000,
      limit: true,
    });
    expect(decideAfterFailure(429, null, 3, none)).toEqual({
      action: 'retry',
      delayMs: 8_000,
      limit: true,
    });
  });

  it('caps a huge Retry-After', () => {
    expect(decideAfterFailure(429, 99_999, 0, none)).toMatchObject({ delayMs: 120_000 });
  });

  it('retries server errors and a missing response as a network drop', () => {
    expect(decideAfterFailure(502, null, 1, none)).toMatchObject({ action: 'retry', limit: false });
    expect(decideAfterFailure(null, null, 0, none)).toMatchObject({
      action: 'retry',
      limit: false,
    });
  });
});
