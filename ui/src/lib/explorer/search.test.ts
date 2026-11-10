import { Schema } from 'effect';
import { describe, expect, it } from 'vite-plus/test';

import {
  CLEAR_FILTERS_PATCH,
  ExplorerSearchSchema,
  applyPatch,
  attributesOf,
  decodeAttribute,
  describeFilters,
  encodeAttribute,
  facetPatch,
  filtersOf,
  isAbsoluteRange,
  queryDepsOf,
  sharedSearchOf,
  resolveTimeRange,
  toggleAttribute,
  togglePinnedColumn,
  withDefaults,
  toggleSource,
} from './search.ts';

/**
 * Decodes a raw search object the way the router's `validateSearch` does.
 */
function decode(input: unknown) {
  return Schema.decodeUnknownSync(ExplorerSearchSchema)(input);
}

describe('search schema', () => {
  it('accepts an empty search', () => {
    expect(decode({})).toEqual({});
  });

  it('accepts a full search', () => {
    const input = {
      index: 'payments',
      range: '15m',
      at: 1_700_000_000_000,
      source: ['a', 'b'],
      service: 'checkout',
      minLevel: 50,
      attrs: ['s:user=abc'],
      event: 'payments:1',
      cols: ['user'],
      density: 'compact',
      tz: 'utc',
      wrap: 1,
      refresh: 30,
    };
    expect(decode(input)).toEqual(input);
  });

  it('keeps an index named only with digits, which the router parses as a number', () => {
    expect(decode({ index: 123 })).toEqual({ index: 123 });
    expect(filtersOf(decode({ index: 123, service: 'a' }))).toEqual({ service: 'a' });
  });

  it('accepts numbers where the router parsed digits as JSON', () => {
    expect(decode({ message: 404, source: 7 })).toEqual({
      message: 404,
      source: 7,
    });
  });

  it('drops a value of the wrong shape instead of failing the whole search', () => {
    expect(decode({ range: '2y', minLevel: 'x', service: 'api', refresh: 7, from: -5 })).toEqual({
      service: 'api',
    });
  });

  it('drops a fractional level', () => {
    expect(decode({ minLevel: 1.5 })).toEqual({});
  });
});

describe('attribute codec', () => {
  it('round-trips the three scalar types without mixing them up', () => {
    for (const value of ['123', 123, true, 'true', '', 1.5]) {
      const condition = { key: 'k', op: 'eq' as const, value };
      expect(decodeAttribute(encodeAttribute(condition))).toEqual(condition);
    }
  });

  it('keeps "=" and ":" inside the value', () => {
    const condition = { key: 'url', op: 'eq' as const, value: 'a=b:c' };
    expect(decodeAttribute(encodeAttribute(condition))).toEqual(condition);
  });

  it('rejects malformed input', () => {
    for (const text of ['', 'k=v', 'x:k=v', 's:=v', 'n:k=abc', 'n:k=', 'b:k=yes', 'n:k=Infinity']) {
      expect(decodeAttribute(text)).toBeNull();
    }
  });

  it('keeps the last condition for a repeated key and caps the list at 16', () => {
    expect(attributesOf({ attrs: ['s:k=1', 's:k=2'] })).toEqual([
      { key: 'k', op: 'eq', value: '2' },
    ]);
    const many = Array.from({ length: 20 }, (_, index) => `n:k${index}=${index}`);
    expect(attributesOf({ attrs: many })).toHaveLength(16);
  });
});

describe('filtersOf', () => {
  it('maps the search to API filters and leaves empty ones out', () => {
    const filters = filtersOf({
      source: ['a', 'b'],
      service: 'checkout',
      logger: '',
      message: 404,
      minLevel: 50,
      attrs: ['n:status=500', 'bad'],
    });
    expect(filters).toEqual({
      source: ['a', 'b'],
      service: 'checkout',
      message: '404',
      minLevel: 50,
      attributes: [{ key: 'status', op: 'eq', value: 500 }],
    });
  });

  it('is empty for an empty search', () => {
    expect(filtersOf({})).toEqual({});
  });

  it('turns a bare source into a one-item list', () => {
    expect(filtersOf({ source: 'a' }).source).toEqual(['a']);
  });
});

describe('resolveTimeRange', () => {
  it('freezes a relative range at "at"', () => {
    expect(resolveTimeRange({ range: '15m', at: 2_000_000 }, 99)).toEqual({
      from: 2_000_000 - 900_000,
      to: 2_000_000,
    });
  });

  it('is stable across renders because it ignores the clock when "at" is set', () => {
    const search = { range: '1h' as const, at: 5_000_000 };
    expect(resolveTimeRange(search, 1)).toEqual(resolveTimeRange(search, 2));
  });

  it('defaults to one hour and falls back to now without "at"', () => {
    expect(resolveTimeRange({}, 10_000_000)).toEqual({
      from: 10_000_000 - 3_600_000,
      to: 10_000_000,
    });
  });

  it('prefers an absolute range', () => {
    const search = { from: 100, to: 200, range: '15m' as const, at: 9_999 };
    expect(resolveTimeRange(search, 1)).toEqual({ from: 100, to: 200 });
    expect(isAbsoluteRange(search)).toBe(true);
  });

  it('ignores an inverted absolute range', () => {
    expect(isAbsoluteRange({ from: 200, to: 100 })).toBe(false);
  });
});

describe('queryDepsOf', () => {
  it('leaves out event, columns and display settings, so they never reload the data', () => {
    const deps = queryDepsOf({
      index: 'a',
      event: 'x',
      cols: ['c'],
      density: 'compact',
      tz: 'utc',
      wrap: 1,
      refresh: 10,
      service: 's',
    });
    expect(deps).toEqual({ index: 'a', service: 's' });
  });
});

describe('applyPatch', () => {
  it('sets, replaces and removes params', () => {
    const next = applyPatch(
      { service: 'a', host: 'h' },
      { service: 'b', host: undefined, logger: 'l' },
    );
    expect(next).toEqual({ service: 'b', logger: 'l' });
  });

  it('drops empty strings and empty lists', () => {
    expect(applyPatch({}, { message: '', source: [], attrs: ['s:a=b'] })).toEqual({
      attrs: ['s:a=b'],
    });
  });

  it('clears filters but keeps range and display settings', () => {
    const next = applyPatch(
      { range: '1h', density: 'compact', service: 's', attrs: ['s:a=b'], minLevel: 40 },
      CLEAR_FILTERS_PATCH,
    );
    expect(next).toEqual({ range: '1h', density: 'compact' });
  });
});

describe('toggles', () => {
  const user = { key: 'user', op: 'eq' as const, value: 'a' };

  it('adds, removes and replaces attribute conditions', () => {
    expect(toggleAttribute([], user)).toEqual([user]);
    expect(toggleAttribute([user], user)).toEqual([]);
    expect(toggleAttribute([user], { ...user, value: 'b' })).toEqual([{ ...user, value: 'b' }]);
  });

  it('keeps at most 16 attribute conditions', () => {
    const full = Array.from({ length: 16 }, (_, index) => ({
      key: `k${index}`,
      op: 'eq' as const,
      value: index,
    }));
    expect(toggleAttribute(full, user)).toHaveLength(16);
  });

  it('toggles pinned columns and drops the oldest beyond six', () => {
    expect(togglePinnedColumn(['a'], 'a')).toEqual([]);
    expect(togglePinnedColumn(['a', 'b', 'c', 'd', 'e', 'f'], 'g')).toEqual([
      'b',
      'c',
      'd',
      'e',
      'f',
      'g',
    ]);
  });

  it('toggles sources', () => {
    expect(toggleSource(['a'], 'b')).toEqual(['a', 'b']);
    expect(toggleSource(['a', 'b'], 'a')).toEqual(['b']);
  });
});

describe('facetPatch', () => {
  it('selects a value and clears it on the second click', () => {
    expect(facetPatch('service', 'api', {})).toEqual({ service: 'api' });
    expect(facetPatch('service', 'api', { service: 'api' })).toEqual({ service: undefined });
    expect(facetPatch('host', 'h1', { host: 'h2' })).toEqual({ host: 'h1' });
  });

  it('sets the minimum level from the level string', () => {
    expect(facetPatch('level', '50', {})).toEqual({ minLevel: 50 });
    expect(facetPatch('level', '50', { minLevel: 50 })).toEqual({ minLevel: undefined });
  });

  it('toggles sources in the OR list', () => {
    expect(facetPatch('source', 'b', { source: ['a'] })).toEqual({ source: ['a', 'b'] });
    expect(facetPatch('source', 'a', { source: ['a'] })).toEqual({ source: [] });
  });
});

describe('withDefaults', () => {
  it('adds the first index and the moment of the refresh', () => {
    expect(withDefaults({ service: 'a' }, 'payments', 7)).toEqual({
      service: 'a',
      index: 'payments',
      at: 7,
    });
  });

  it('returns null when the search is complete', () => {
    expect(withDefaults({ index: 'a', at: 1 }, 'b', 7)).toBeNull();
  });

  it('needs no "at" for an absolute range', () => {
    expect(withDefaults({ index: 'a', from: 1, to: 2 }, 'b', 7)).toBeNull();
  });

  it('adds only "at" when there is no index to choose', () => {
    expect(withDefaults({}, undefined, 7)).toEqual({ at: 7 });
  });

  it('never loops: applying it twice changes nothing the second time', () => {
    const once = withDefaults({}, 'a', 7);
    expect(once === null ? null : withDefaults(once, 'a', 8)).toBeNull();
  });
});

describe('describeFilters', () => {
  it('lists every active filter in words', () => {
    expect(
      describeFilters({
        source: ['s1'],
        service: 'api',
        minLevel: 50,
        message: 'boom',
        attributes: [{ key: 'n', op: 'eq', value: 5 }],
      }),
    ).toEqual([
      'source = s1',
      'service = api',
      'level >= 50',
      'message contains "boom" (case-sensitive)',
      'n = 5',
    ]);
  });
});

describe('sharedSearchOf', () => {
  it('keeps the index and filters, and drops range, event, columns and display settings', () => {
    const search = {
      index: 'payments',
      source: ['a'],
      service: 'checkout',
      logger: 'http',
      host: 'h1',
      message: 'boom',
      minLevel: 50,
      attrs: ['s:user=abc'],
      range: '15m',
      from: 1,
      to: 2,
      at: 3,
      event: 'payments:1',
      cols: ['user'],
      density: 'compact',
      tz: 'utc',
      wrap: 1,
      refresh: 30,
    };

    expect(sharedSearchOf(search)).toEqual({
      index: 'payments',
      source: ['a'],
      service: 'checkout',
      logger: 'http',
      host: 'h1',
      message: 'boom',
      minLevel: 50,
      attrs: ['s:user=abc'],
    });
  });

  it('leaves out params that are not set', () => {
    expect(sharedSearchOf({ range: '1h', service: undefined })).toEqual({});
  });
});
