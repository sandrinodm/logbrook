import { describe, expect, it } from 'vite-plus/test';

import type { LogEvent, SearchResult } from '@/lib/logbrook/types';

import { eventAt, locateRow, pageOffsets, rowOfEvent, totalRows } from './rows.ts';

/**
 * Builds a minimal event with the given id.
 */
function event(id: string): LogEvent {
  return {
    id,
    source: 's',
    event_time: 0,
    received_at: 0,
    level: 30,
    message: id,
    service: null,
    logger: null,
    host: null,
    pid: null,
    attributes: {},
  };
}

/**
 * Builds pages from lists of ids.
 */
function pagesOf(...ids: string[][]): SearchResult[] {
  return ids.map((page) => ({ events: page.map(event), next_cursor: null }));
}

describe('pageOffsets', () => {
  it('lists the first row of each page and the total', () => {
    expect(pageOffsets(pagesOf(['a', 'b', 'c'], ['d', 'e', 'f'], ['g', 'h']))).toEqual([
      0, 3, 6, 8,
    ]);
  });

  it('is [0] with no pages', () => {
    expect(totalRows(pageOffsets([]))).toBe(0);
  });
});

describe('locateRow', () => {
  const offsets = pageOffsets(pagesOf(['a', 'b', 'c'], ['d', 'e', 'f'], ['g', 'h']));

  it('finds rows at page boundaries', () => {
    expect(locateRow(offsets, 0)).toEqual({ page: 0, position: 0 });
    expect(locateRow(offsets, 2)).toEqual({ page: 0, position: 2 });
    expect(locateRow(offsets, 3)).toEqual({ page: 1, position: 0 });
    expect(locateRow(offsets, 7)).toEqual({ page: 2, position: 1 });
  });

  it('returns null outside the loaded rows', () => {
    expect(locateRow(offsets, -1)).toBeNull();
    expect(locateRow(offsets, 8)).toBeNull();
    expect(locateRow(offsets, 1.5)).toBeNull();
  });

  it('skips empty pages', () => {
    const withEmpty = pageOffsets(pagesOf(['a'], [], ['b']));
    expect(locateRow(withEmpty, 1)).toEqual({ page: 2, position: 0 });
  });

  it('agrees with plain concatenation for 100 pages of 500 rows', () => {
    const pages = Array.from({ length: 100 }, (_, p) => ({
      events: Array.from({ length: 500 }, (_, i) => event(`${p}-${i}`)),
      next_cursor: null,
    }));
    const big = pageOffsets(pages);
    const flat = pages.flatMap((page) => page.events);
    for (const row of [0, 499, 500, 24_999, 25_000, 49_999]) {
      expect(eventAt(pages, big, row)?.id).toBe(flat[row]?.id);
    }
  });
});

describe('rowOfEvent', () => {
  const pages = pagesOf(['a', 'b'], ['c', 'd']);
  const offsets = pageOffsets(pages);

  it('returns the row of a loaded event', () => {
    expect(rowOfEvent(pages, offsets, 'c')).toBe(2);
  });

  it('returns -1 for an event that is not loaded', () => {
    expect(rowOfEvent(pages, offsets, 'z')).toBe(-1);
  });
});
