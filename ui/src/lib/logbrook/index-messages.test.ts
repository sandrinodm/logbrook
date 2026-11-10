import { describe, expect, it } from 'vite-plus/test';

import { describeCreateFailure, describeDeleteFailure } from './index-messages.ts';

describe('describeCreateFailure', () => {
  it('explains the index limit for a 429', () => {
    expect(describeCreateFailure(429, 'too many')).toContain('index limit');
  });

  it('keeps the server text for other statuses', () => {
    expect(describeCreateFailure(400, 'bad name')).toBe('bad name');
  });
});

describe('describeDeleteFailure', () => {
  it('explains protected indexes for a 400 and keeps the server text', () => {
    const message = describeDeleteFailure(400, 'cannot delete default');

    expect(message).toContain('cannot delete default');
    expect(message).toContain('server configuration');
  });

  it('handles an index that is already gone', () => {
    expect(describeDeleteFailure(404, 'unknown index')).toContain('no longer exists');
  });
});
