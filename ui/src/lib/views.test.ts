import { beforeEach, describe, expect, it } from 'vite-plus/test';

import {
  forgetExplorerView,
  recallExplorerView,
  rememberExplorerView,
  searchForView,
  viewSearch,
} from './views.ts';

const CURRENT = {
  index: 'payments',
  service: 'checkout',
  minLevel: 50,
  range: '6h',
  at: 1,
  event: 'payments:1',
  cols: ['user'],
  tz: 'utc',
};

describe('searchForView', () => {
  it('carries the index and filters, and only those, between Logs and Live Tail', () => {
    const expected = { index: 'payments', service: 'checkout', minLevel: 50 };

    expect(searchForView('/tail', CURRENT)).toEqual(expected);
    expect(searchForView('/', CURRENT)).toEqual(expected);
  });

  it('keeps only the index for Indexes and Ingest', () => {
    expect(searchForView('/indexes', CURRENT)).toEqual({ index: 'payments' });
    expect(searchForView('/ingest', CURRENT)).toEqual({ index: 'payments' });
  });

  it('keeps an index named only with digits', () => {
    expect(searchForView('/tail', { index: 12345 })).toEqual({ index: 12345 });
  });

  it('gives an empty search when nothing is set', () => {
    expect(searchForView('/indexes', {})).toEqual({});
    expect(searchForView('/tail', { range: '1h' })).toEqual({});
  });
});

/**
 * What the explorer had when it was left: a range, display settings, a selected event and a moment
 * of refresh, besides the index and a filter.
 */
const LEFT_EXPLORER = {
  index: 'payments',
  service: 'checkout',
  range: '6h',
  at: 1_700_000_000_000,
  event: 'payments:1',
  cols: ['user', 'session'],
  density: 'comfortable',
  tz: 'local',
  wrap: 1,
  refresh: 10,
};

describe('remembering the explorer', () => {
  beforeEach(() => {
    forgetExplorerView();
  });

  it('keeps the range, pinned columns, density, time zone and wrap, and nothing else', () => {
    rememberExplorerView(LEFT_EXPLORER);

    expect(recallExplorerView()).toEqual({
      range: '6h',
      cols: ['user', 'session'],
      density: 'comfortable',
      tz: 'local',
      wrap: 1,
    });
  });

  it('keeps an absolute range', () => {
    rememberExplorerView({ from: 100, to: 200, at: 5 });

    expect(recallExplorerView()).toEqual({ from: 100, to: 200 });
  });

  it('replaces what was remembered before instead of merging', () => {
    rememberExplorerView({ range: '6h', density: 'comfortable' });
    rememberExplorerView({ range: '1h' });

    expect(recallExplorerView()).toEqual({ range: '1h' });
  });

  it('forgets on request', () => {
    rememberExplorerView({ range: '6h' });
    forgetExplorerView();

    expect(recallExplorerView()).toEqual({});
  });
});

describe('searchForView with restored explorer state', () => {
  const restore = { range: '6h', cols: ['user'], density: 'comfortable', tz: 'local', wrap: 1 };

  it('brings the remembered state back to Logs, under the tail filters', () => {
    const fromTail = { index: 'payments', service: 'billing', minLevel: 40 };

    expect(searchForView('/', fromTail, restore)).toEqual({ ...restore, ...fromTail });
  });

  it('lets a filter changed on the tail win', () => {
    const search = searchForView('/', { index: 'payments', service: 'billing' }, restore);

    expect(search.service).toBe('billing');
    expect(search.range).toBe('6h');
  });

  it('never carries the moment of refresh, so a relative range gets a fresh now', () => {
    rememberExplorerView(LEFT_EXPLORER);

    expect(searchForView('/', { index: 'payments' }, recallExplorerView())).not.toHaveProperty(
      'at',
    );
  });

  it('does not put explorer state into the tail, indexes or ingest links', () => {
    expect(searchForView('/tail', { index: 'payments' }, restore)).toEqual({ index: 'payments' });
    expect(searchForView('/indexes', { index: 'payments' }, restore)).toEqual({
      index: 'payments',
    });
    expect(searchForView('/ingest', { index: 'payments' }, restore)).toEqual({
      index: 'payments',
    });
  });

  it('restores nothing when there is nothing to restore', () => {
    expect(searchForView('/', { index: 'payments' })).toEqual({ index: 'payments' });
  });
});

describe('viewSearch', () => {
  beforeEach(() => {
    forgetExplorerView();
    rememberExplorerView({ range: '6h', density: 'comfortable' });
  });

  it('restores the remembered state when the link starts from another view', () => {
    expect(viewSearch('/', { index: 'payments' }, false)).toEqual({
      index: 'payments',
      range: '6h',
      density: 'comfortable',
    });
  });

  it('does not restore when the link starts from the explorer itself', () => {
    expect(viewSearch('/', { index: 'payments', range: '15m' }, true)).toEqual({
      index: 'payments',
    });
  });

  it('leaves the other views alone', () => {
    expect(viewSearch('/tail', { index: 'payments', range: '15m' }, true)).toEqual({
      index: 'payments',
    });
    expect(viewSearch('/indexes', { index: 'payments' }, false)).toEqual({ index: 'payments' });
  });
});
