import { afterEach, beforeEach, describe, expect, it, vi } from 'vite-plus/test';

import type { VisibilitySource } from './tail-visibility.ts';

import { HIDDEN_CLOSE_DELAY_MS, watchTabVisibility } from './tail-visibility.ts';

/**
 * A fake `document` whose visibility a test flips.
 */
function fakeDocument(initial: DocumentVisibilityState) {
  const listeners = new Set<() => void>();
  const source: VisibilitySource & { visibilityState: DocumentVisibilityState } = {
    visibilityState: initial,
    addEventListener: (_type, listener) => listeners.add(listener),
    removeEventListener: (_type, listener) => listeners.delete(listener),
  };
  const setVisibility = (state: DocumentVisibilityState) => {
    source.visibilityState = state;
    for (const listener of listeners) {
      listener();
    }
  };
  return { source, setVisibility, listeners };
}

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
});

describe('watchTabVisibility', () => {
  it('suspends after the tab stays hidden for a minute', () => {
    const tail = { suspend: vi.fn(), resume: vi.fn() };
    const { source, setVisibility } = fakeDocument('visible');
    watchTabVisibility(tail, source);

    setVisibility('hidden');
    vi.advanceTimersByTime(HIDDEN_CLOSE_DELAY_MS - 1);
    expect(tail.suspend).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);

    expect(tail.suspend).toHaveBeenCalledTimes(1);
  });

  it('keeps the stream when the tab returns in time', () => {
    const tail = { suspend: vi.fn(), resume: vi.fn() };
    const { source, setVisibility } = fakeDocument('visible');
    watchTabVisibility(tail, source);

    setVisibility('hidden');
    vi.advanceTimersByTime(30_000);
    setVisibility('visible');
    vi.advanceTimersByTime(HIDDEN_CLOSE_DELAY_MS * 2);

    expect(tail.suspend).not.toHaveBeenCalled();
  });

  it('resumes when the tab is shown again', () => {
    const tail = { suspend: vi.fn(), resume: vi.fn() };
    const { source, setVisibility } = fakeDocument('visible');
    watchTabVisibility(tail, source);

    setVisibility('hidden');
    vi.advanceTimersByTime(HIDDEN_CLOSE_DELAY_MS);
    setVisibility('visible');

    expect(tail.resume).toHaveBeenCalledTimes(1);
  });

  it('starts the timer at once when it begins hidden', () => {
    const tail = { suspend: vi.fn(), resume: vi.fn() };
    const { source } = fakeDocument('hidden');

    watchTabVisibility(tail, source, 1_000);
    vi.advanceTimersByTime(1_000);

    expect(tail.suspend).toHaveBeenCalledTimes(1);
  });

  it('stops watching and cancels the timer when disposed', () => {
    const tail = { suspend: vi.fn(), resume: vi.fn() };
    const { source, setVisibility, listeners } = fakeDocument('visible');
    const stop = watchTabVisibility(tail, source);

    setVisibility('hidden');
    stop();
    vi.advanceTimersByTime(HIDDEN_CLOSE_DELAY_MS * 2);

    expect(tail.suspend).not.toHaveBeenCalled();
    expect(listeners.size).toBe(0);
  });
});
