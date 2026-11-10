import { describe, expect, it } from 'vite-plus/test';

import { dayKey, formatClock, formatDateTime, formatDay, splitHighlight } from './format.ts';

describe('time formatting', () => {
  const ms = Date.UTC(2026, 10, 10, 13, 5, 9, 42);

  it('formats a UTC clock with milliseconds', () => {
    expect(formatClock(ms, 'utc')).toBe('13:05:09.042');
  });

  it('formats a date and time with the zone name', () => {
    expect(formatDateTime(ms, 'utc')).toBe('2026-11-10 13:05:09.042 UTC');
  });

  it('keys days by calendar date in UTC, across midnight', () => {
    expect(dayKey(Date.UTC(2026, 10, 10, 23, 59, 59, 999), 'utc')).toBe('2026-11-10');
    expect(dayKey(Date.UTC(2026, 10, 11, 0, 0, 0, 0), 'utc')).toBe('2026-11-11');
  });

  it('labels a day', () => {
    expect(formatDay(ms, 'utc')).toBe('Tue, 10 Nov 2026');
  });
});

describe('splitHighlight', () => {
  it('returns one plain segment for an empty needle', () => {
    expect(splitHighlight('abc', '')).toEqual([{ text: 'abc', match: false }]);
  });

  it('marks every occurrence and keeps the text between', () => {
    expect(splitHighlight('a-err-b-err', 'err')).toEqual([
      { text: 'a-', match: false },
      { text: 'err', match: true },
      { text: '-b-', match: false },
      { text: 'err', match: true },
    ]);
  });

  it('is case-sensitive, like the server', () => {
    expect(splitHighlight('ERR err', 'err').filter((segment) => segment.match)).toHaveLength(1);
  });

  it('reassembles to the original text', () => {
    const text = 'aaaa';
    expect(
      splitHighlight(text, 'aa')
        .map((segment) => segment.text)
        .join(''),
    ).toBe(text);
  });
});
