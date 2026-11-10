import { describe, expect, it } from 'vite-plus/test';

import { deriveCapabilities, hasIndexDetail, metricsAccessOf } from './capabilities.ts';
import { LogbrookError } from './errors.ts';

describe('deriveCapabilities', () => {
  it('reports full detail when the list carries sizes', () => {
    const capabilities = deriveCapabilities({ canManageIndexes: false }, [
      { name: 'default', size_bytes: 10, ready: true },
    ]);

    expect(capabilities).toEqual({ canManageIndexes: false, indexDetail: 'full' });
  });

  it('reports names only for the reduced shape of a source-restricted token', () => {
    const capabilities = deriveCapabilities({ canManageIndexes: true }, [{ name: 'default' }]);

    expect(capabilities).toEqual({ canManageIndexes: true, indexDetail: 'names-only' });
  });

  it('cannot judge from an empty list', () => {
    expect(deriveCapabilities({ canManageIndexes: false }, []).indexDetail).toBe('unknown');
  });
});

describe('hasIndexDetail', () => {
  it('is true when size_bytes is present, even at zero', () => {
    expect(hasIndexDetail({ name: 'a', size_bytes: 0 })).toBe(true);
    expect(hasIndexDetail({ name: 'a' })).toBe(false);
  });
});

describe('metricsAccessOf', () => {
  it('maps 403 to forbidden', () => {
    expect(metricsAccessOf(new LogbrookError(403, 'metrics require', null), false)).toBe(
      'forbidden',
    );
  });

  it('is allowed with data and no error, unknown otherwise', () => {
    expect(metricsAccessOf(null, true)).toBe('allowed');
    expect(metricsAccessOf(null, false)).toBe('unknown');
    expect(metricsAccessOf(new LogbrookError(503, 'down', null), false)).toBe('unknown');
    expect(metricsAccessOf(new TypeError('network'), true)).toBe('unknown');
  });
});
