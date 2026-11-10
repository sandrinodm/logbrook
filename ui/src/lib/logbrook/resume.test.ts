import { describe, expect, it } from 'vite-plus/test';

import { isResumeId } from './resume.ts';

describe('isResumeId', () => {
  it('accepts the opaque IDs the server hands out', () => {
    expect(isResumeId('checkout:v1.AbC-_123~=')).toBe(true);
    expect(isResumeId('a'.repeat(1_100))).toBe(true);
  });

  it('refuses anything that could break out of a header value', () => {
    for (const bad of ['', 'a b', 'a\r\nx-evil: 1', 'a\nb', 'a\u0000b', 'é', 'a'.repeat(1_101)]) {
      expect(isResumeId(bad), JSON.stringify(bad)).toBe(false);
    }
  });
});
