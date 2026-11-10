import { describe, expect, it } from 'vite-plus/test';

import { isServerErrorFrame, missedWindow, parseLogFrame } from './tail-frames.ts';

describe('parseLogFrame', () => {
  it('returns the event of a log frame', () => {
    const event = parseLogFrame('{"id":"7","message":"hi","level":30}');

    expect(event?.id).toBe('7');
    expect(event?.message).toBe('hi');
  });

  it('skips data that is not an event', () => {
    expect(parseLogFrame('caught up')).toBeNull();
    expect(parseLogFrame('42')).toBeNull();
    expect(parseLogFrame('null')).toBeNull();
    expect(parseLogFrame('{"id":7}')).toBeNull();
    expect(parseLogFrame('{"message":"no id"}')).toBeNull();
  });
});

describe('isServerErrorFrame', () => {
  it('tells an error frame from a connection failure', () => {
    expect(isServerErrorFrame(new MessageEvent('error', { data: 'tail query failed' }))).toBe(true);
    expect(isServerErrorFrame(new Event('error'))).toBe(false);
  });
});

describe('missedWindow', () => {
  it('starts at the last event, minus the margin, and ends just after now', () => {
    expect(missedWindow(10_000, 1_000, 20_000, 2_000)).toEqual({ from: 8_000, to: 20_001 });
  });

  it('falls back to when the tail opened', () => {
    expect(missedWindow(null, 5_000, 9_000, 1_000)).toEqual({ from: 4_000, to: 9_001 });
  });

  it('never starts before zero', () => {
    expect(missedWindow(null, 10, 20, 1_000).from).toBe(0);
  });
});
