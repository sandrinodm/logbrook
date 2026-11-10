import { describe, expect, it } from 'vite-plus/test';

import { matchAllowedRoute } from './routes.server.ts';

describe('matchAllowedRoute', () => {
  it('allows the read endpoints', () => {
    for (const endpoint of ['logs', 'logs/facets', 'logs/histogram', 'logs/tail']) {
      expect(matchAllowedRoute(`indexes/payments/${endpoint}`)).toEqual({
        path: `/indexes/payments/${endpoint}`,
      });
    }
  });

  it('allows the index list and index info', () => {
    expect(matchAllowedRoute('indexes')).toEqual({ path: '/indexes' });
    expect(matchAllowedRoute('indexes/default')).toEqual({ path: '/indexes/default' });
  });

  it('allows metrics with the read token', () => {
    expect(matchAllowedRoute('metrics')).toEqual({ path: '/metrics' });
  });

  it.each([
    'indexes/payments/logs/count',
    'indexes/payments/logs/ingest',
    'indexes/payments/logs/',
    'indexes/payments/',
    'indexes/',
    'indexes/payments/logs/tail/extra',
    'indexes/Payments/logs',
    'indexes/../metrics',
    'indexes/a%2Fb/logs',
    'indexes/payments/logs/%2e%2e/ingest',
    'logs',
    'logs/tail',
    'health',
    'ready',
    'openapi.json',
    'metrics/',
    '',
  ])('refuses %j', (path) => {
    expect(matchAllowedRoute(path)).toBeNull();
  });
});
