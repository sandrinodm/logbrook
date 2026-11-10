import { describe, expect, it } from 'vite-plus/test';

import { ERROR_LEVEL, buildPlan, planQueries } from './plan.ts';

describe('buildPlan', () => {
  it('returns null without an index', () => {
    expect(buildPlan({ range: '1h', at: 1 }, 1_000, 0)).toBeNull();
  });

  it('freezes the relative range at "at" and picks an interval for the width', () => {
    const plan = buildPlan({ index: 'a', range: '1h', at: 10_000_000, service: 'api' }, 1_400, 0);
    expect(plan).toEqual({
      index: 'a',
      range: { from: 10_000_000 - 3_600_000, to: 10_000_000 },
      filters: { service: 'api' },
      intervalMs: 30_000,
    });
  });
});

describe('planQueries', () => {
  const plan = {
    index: 'a',
    range: { from: 1_000, to: 5_000 },
    filters: { service: 'api', minLevel: 30 },
    intervalMs: 1_000,
  };

  it('keys the four requests on the same frozen range and filters', () => {
    const queries = planQueries(plan);
    expect(queries.logs.queryKey).toEqual([
      'logbrook',
      'logs',
      'a',
      {
        limit: 500,
        from: 1_000,
        to: 5_000,
        service: 'api',
        minLevel: 30,
      },
    ]);
    expect(queries.facets.queryKey[4]).toEqual({ service: 'api', minLevel: 30 });
  });

  it('asks for errors with min_level 50, or the active level when it is higher', () => {
    expect(planQueries(plan).errors.queryKey[3]).toMatchObject({ minLevel: ERROR_LEVEL });
    const strict = planQueries({ ...plan, filters: { minLevel: 60 } });
    expect(strict.errors.queryKey[3]).toMatchObject({ minLevel: 60 });
  });

  it('gives a new logs key when the range is frozen again, so paging restarts', () => {
    const later = planQueries({ ...plan, range: { from: 2_000, to: 6_000 } });
    expect(later.logs.queryKey).not.toEqual(planQueries(plan).logs.queryKey);
  });
});
