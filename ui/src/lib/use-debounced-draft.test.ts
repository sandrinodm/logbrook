import { describe, expect, it } from 'vite-plus/test';

import { committed, expired, initialDraft, observed, typed } from './use-debounced-draft.ts';

describe('observed', () => {
  it('replaces the draft when the URL changes from outside', () => {
    const state = typed(initialDraft('a'), 'ab');

    expect(observed(state, 'zzz')).toEqual({ draft: 'zzz', seen: 'zzz', pending: [] });
  });

  it('keeps the draft when the URL reaches a value this box wrote', () => {
    // Type "time", pause (commit), keep typing "out"; the first navigation lands late.
    let state = typed(initialDraft(''), 'time');
    state = committed(state, 'time', 1);
    state = typed(state, 'timeout');

    state = observed(state, 'time');

    expect(state.draft).toBe('timeout');
    expect(state.pending).toEqual([]);
  });

  it('keeps the draft through every echo, including one that skips an intermediate value', () => {
    let state = typed(initialDraft(''), 'a');
    state = committed(state, 'a', 2);
    state = typed(state, 'ab');
    state = committed(state, 'ab', 3);
    state = typed(state, 'abc');

    state = observed(state, 'ab');

    expect(state.draft).toBe('abc');
    expect(state.pending).toEqual([]);
  });

  it('keeps a draft whose spelling differs from the stored value', () => {
    let state = typed(initialDraft(''), 'api,');
    state = committed(state, 'api', 4);

    state = observed(state, 'api');

    expect(state.draft).toBe('api,');
  });

  it('follows an outside change to a value this box wrote long ago', () => {
    let state = typed(initialDraft(''), 'x');
    state = committed(state, 'x', 5);
    state = observed(state, 'x');
    state = observed(state, 'y');

    expect(observed(state, 'x').draft).toBe('x');
    expect(observed(typed(state, 'typing'), 'x').draft).toBe('x');
  });

  it('forgets pending echoes when an outside change arrives', () => {
    let state = typed(initialDraft(''), 'a');
    state = committed(state, 'a', 6);

    state = observed(state, 'back-button');

    expect(state).toEqual({ draft: 'back-button', seen: 'back-button', pending: [] });
  });

  it('does nothing when the value is the one already seen', () => {
    const state = typed(initialDraft('a'), 'ab');

    expect(observed(state, 'a')).toBe(state);
  });
});

describe('expired', () => {
  it('forgets a commit whose echo never came, so an outside change to that value is followed', () => {
    // The box wrote "x", but the navigation never landed: the URL stayed on "old". The user kept
    // typing, then something outside (the back button, a facet click) set the URL to "x".
    let state = typed(initialDraft('old'), 'x');
    state = committed(state, 'x', 1);
    state = typed(state, 'something else');

    state = expired(state, 1);
    state = observed(state, 'x');

    expect(state.draft).toBe('x');
  });

  it('swallows that outside change while the commit is still pending', () => {
    let state = typed(initialDraft('old'), 'x');
    state = committed(state, 'x', 1);
    state = typed(state, 'something else');

    state = observed(state, 'x');

    expect(state.draft).toBe('something else');
  });

  it('leaves the other pending commits alone', () => {
    let state = committed(initialDraft(''), 'a', 1);
    state = committed(state, 'b', 2);

    expect(expired(state, 1).pending).toEqual([{ value: 'b', id: 2 }]);
  });

  it('does nothing when the echo already arrived, and never touches a later commit of the same value', () => {
    let state = committed(initialDraft(''), 'a', 1);
    state = observed(state, 'a');
    state = committed(state, 'a', 2);

    expect(expired(state, 1)).toBe(state);
    expect(state.pending).toEqual([{ value: 'a', id: 2 }]);
  });
});
