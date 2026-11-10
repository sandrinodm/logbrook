import { describe, expect, it } from 'vite-plus/test';

import { appendFilters, appendRange, attributeFilterJson } from './search-params.ts';

describe('attributeFilterJson', () => {
  it('returns null when there are no conditions', () => {
    expect(attributeFilterJson([])).toBeNull();
  });

  it('sorts conditions by key so equal filters serialise equally', () => {
    const a = attributeFilterJson([
      { key: 'zeta', op: 'eq', value: 1 },
      { key: 'alpha', op: 'eq', value: 'x' },
    ]);
    const b = attributeFilterJson([
      { key: 'alpha', op: 'eq', value: 'x' },
      { key: 'zeta', op: 'eq', value: 1 },
    ]);

    expect(a).toBe(b);
    expect(a).toBe(
      '{"attributes":[{"key":"alpha","op":"eq","value":"x"},{"key":"zeta","op":"eq","value":1}]}',
    );
  });

  it('keeps value types: numbers and booleans are not stringified', () => {
    const json = attributeFilterJson([{ key: 'ok', op: 'eq', value: true }]);

    expect(json).toContain('"value":true');
  });
});

describe('appendFilters', () => {
  it('joins sources with commas and maps minLevel to min_level', () => {
    const params = new URLSearchParams();

    appendFilters(params, { source: ['a', 'b'], minLevel: 50 });

    expect(params.get('source')).toBe('a,b');
    expect(params.get('min_level')).toBe('50');
  });

  it('skips empty and missing values instead of sending empty strings', () => {
    const params = new URLSearchParams();

    appendFilters(params, { service: '', source: [], attributes: [] });

    expect(params.toString()).toBe('');
  });

  it('keeps minLevel 0, which is a real value', () => {
    const params = new URLSearchParams();

    appendFilters(params, { minLevel: 0 });

    expect(params.get('min_level')).toBe('0');
  });

  it('encodes the attribute filter as one JSON parameter', () => {
    const params = new URLSearchParams();

    appendFilters(params, { attributes: [{ key: 'runId', op: 'eq', value: 'run-1' }] });

    expect(JSON.parse(params.get('filter') ?? '')).toEqual({
      attributes: [{ key: 'runId', op: 'eq', value: 'run-1' }],
    });
  });
});

describe('appendRange', () => {
  it('sets from and to', () => {
    const params = new URLSearchParams();

    appendRange(params, { from: 1, to: 2 });

    expect(params.toString()).toBe('from=1&to=2');
  });
});
