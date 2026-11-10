import { describe, expect, it } from 'vite-plus/test';

import type { SearchResult } from './types.ts';

import { LogbrookError } from './errors.ts';
import {
  MAX_LOADED_EVENTS,
  logsInfiniteQuery,
  nextCursor,
  retryDelayMs,
  shouldRetry,
} from './queries.ts';

/**
 * Builds a page of `count` events with the given next cursor.
 */
function page(count: number, cursor: string | null): SearchResult {
  const events = Array.from({ length: count }, (_, index) => ({
    id: String(index),
    source: 's',
    event_time: index,
    received_at: index,
    level: 30,
    message: 'm',
    service: null,
    logger: null,
    host: null,
    pid: null,
    attributes: {},
  }));
  return { events, next_cursor: cursor };
}

describe('shouldRetry', () => {
  it('retries 429 up to three times', () => {
    const error = new LogbrookError(429, 'busy', 1);

    expect(shouldRetry(0, error)).toBe(true);
    expect(shouldRetry(2, error)).toBe(true);
    expect(shouldRetry(3, error)).toBe(false);
  });

  it('never retries 400', () => {
    expect(shouldRetry(0, new LogbrookError(400, 'bad cursor', null))).toBe(false);
  });
});

describe('retryDelayMs', () => {
  it('honours Retry-After', () => {
    expect(retryDelayMs(0, new LogbrookError(429, 'busy', 2))).toBe(2000);
  });

  it('caps a huge Retry-After', () => {
    expect(retryDelayMs(0, new LogbrookError(429, 'busy', 99_999))).toBe(30_000);
  });

  it('backs off exponentially up to a cap without Retry-After', () => {
    const error = new LogbrookError(503, 'down', null);

    expect(retryDelayMs(0, error)).toBe(1000);
    expect(retryDelayMs(2, error)).toBe(4000);
    expect(retryDelayMs(10, error)).toBe(8000);
  });
});

describe('nextCursor', () => {
  it('returns the cursor of the last page', () => {
    expect(nextCursor(page(1, 'c2'), [page(1, 'c2')])).toBe('c2');
  });

  it('stops on the last page', () => {
    expect(nextCursor(page(1, null), [page(1, null)])).toBeUndefined();
  });

  it('stops at the loaded-event cap even when a cursor exists', () => {
    const full = page(MAX_LOADED_EVENTS, 'more');

    expect(nextCursor(full, [full])).toBeUndefined();
  });
});

describe('logsInfiniteQuery', () => {
  it('never refetches by itself and skips structural sharing', () => {
    const options = logsInfiniteQuery('a', { from: 1, to: 2 });

    expect(options.staleTime).toBe(Infinity);
    expect(options.refetchOnWindowFocus).toBe(false);
    expect(options.refetchOnReconnect).toBe(false);
    expect(options.structuralSharing).toBe(false);
  });

  it('puts the frozen range and the default page size in the key', () => {
    const options = logsInfiniteQuery('a', { from: 1, to: 2 });

    expect(options.queryKey).toEqual(['logbrook', 'logs', 'a', { limit: 500, from: 1, to: 2 }]);
  });
});
