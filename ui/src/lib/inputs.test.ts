import { Schema } from 'effect';
import { describe, expect, it } from 'vite-plus/test';

import { IndexNameInput } from './inputs.ts';

describe('IndexNameInput', () => {
  const decode = Schema.decodeUnknownOption(IndexNameInput);

  it('accepts Logbrook index names', () => {
    expect(decode({ name: 'payments-eu_1' })._tag).toBe('Some');
  });

  it.each(['', '-start', 'UPPER', 'a/b', '..', 'a'.repeat(64)])('rejects %j', (name) => {
    expect(decode({ name })._tag).toBe('None');
  });
});
