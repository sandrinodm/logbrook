import { describe, expect, it } from 'vite-plus/test';

import {
  MAX_HISTOGRAM_BUCKETS,
  freezeRange,
  pickIntervalMs,
  bucketsToRange,
  selectionToRange,
  sumBuckets,
  widerRange,
  zeroFill,
  zoomOutRange,
} from './range.ts';

describe('freezeRange', () => {
  it('ends at the given moment and reaches back by the range length', () => {
    expect(freezeRange('15m', 1_000_000)).toEqual({ from: 1_000_000 - 900_000, to: 1_000_000 });
  });

  it('gives the same bounds for the same moment, so cursors stay valid', () => {
    expect(freezeRange('1h', 5_000_000)).toEqual(freezeRange('1h', 5_000_000));
  });

  it('gives new bounds for a new moment', () => {
    expect(freezeRange('1h', 5_000_000)).not.toEqual(freezeRange('1h', 5_001_000));
  });
});

describe('widerRange', () => {
  it('returns the next longer range', () => {
    expect(widerRange(3_600_000)).toBe('6h');
  });

  it('returns null at the longest range', () => {
    expect(widerRange(30 * 86_400_000)).toBeNull();
  });
});

describe('zoomOutRange', () => {
  it('doubles the span around the same centre', () => {
    expect(zoomOutRange({ from: 1_000, to: 2_000 }, 10_000)).toEqual({ from: 500, to: 2_500 });
  });

  it('does not reach past now', () => {
    expect(zoomOutRange({ from: 1_000, to: 2_000 }, 2_100)).toEqual({ from: 100, to: 2_100 });
  });
});

describe('pickIntervalMs', () => {
  it('rounds one hour on a wide chart up to a nice step', () => {
    expect(pickIntervalMs(3_600_000, 1_400)).toBe(30_000);
  });

  it('uses larger buckets on a narrow chart', () => {
    expect(pickIntervalMs(3_600_000, 240)).toBe(120_000);
  });

  it('never exceeds the server bucket limit, for any span and width', () => {
    for (const span of [1, 999, 60_000, 3_600_000, 86_400_000, 31 * 86_400_000]) {
      for (const width of [0, 100, 600, 1_200, 4_000]) {
        const interval = pickIntervalMs(span, width);
        expect(Math.ceil(span / interval)).toBeLessThanOrEqual(MAX_HISTOGRAM_BUCKETS);
        expect(interval).toBeGreaterThan(0);
      }
    }
  });

  it('picks a whole number of milliseconds for a tiny span', () => {
    expect(Number.isInteger(pickIntervalMs(7, 1_000))).toBe(true);
  });
});

describe('zeroFill', () => {
  const range = { from: 1_000, to: 1_500 };

  it('has one slot per interval, including a partial last one', () => {
    expect(zeroFill([], range, 200).map((slot) => slot.time)).toEqual([1_000, 1_200, 1_400]);
  });

  it('places the server buckets and fills the gaps with zero', () => {
    const filled = zeroFill([{ time: 1_200, count: 7 }], range, 200);
    expect(filled.map((slot) => slot.count)).toEqual([0, 7, 0]);
  });

  it('ignores buckets outside the range', () => {
    const filled = zeroFill(
      [
        { time: 900, count: 3 },
        { time: 1_600, count: 4 },
      ],
      range,
      200,
    );
    expect(sumBuckets(filled)).toBe(0);
  });

  it('is empty for an empty range', () => {
    expect(zeroFill([], { from: 5, to: 5 }, 10)).toEqual([]);
  });
});

describe('sumBuckets', () => {
  it('adds the counts, which is the total of matching events', () => {
    expect(
      sumBuckets([
        { time: 0, count: 2 },
        { time: 10, count: 5 },
      ]),
    ).toBe(7);
  });
});

describe('selectionToRange', () => {
  const chart = { widthPx: 1_000, range: { from: 0, to: 100_000 }, intervalMs: 1_000 };

  it('snaps a drag outwards to bucket edges', () => {
    // 100 slots of 10 px: 255..412 covers slots 25 to 41 inclusive.
    expect(selectionToRange({ fromPx: 255, toPx: 412 }, chart)).toEqual({
      from: 25_000,
      to: 42_000,
    });
  });

  it('accepts a drag from right to left', () => {
    expect(selectionToRange({ fromPx: 412, toPx: 255 }, chart)).toEqual({
      from: 25_000,
      to: 42_000,
    });
  });

  it('treats a tiny drag as a click', () => {
    expect(selectionToRange({ fromPx: 100, toPx: 102 }, chart)).toBeNull();
  });

  it('stays inside the chart and the range', () => {
    expect(selectionToRange({ fromPx: -50, toPx: 5_000 }, chart)).toEqual({ from: 0, to: 100_000 });
  });

  it('does not extend past a short last bucket', () => {
    const short = { widthPx: 100, range: { from: 0, to: 250 }, intervalMs: 100 };
    expect(selectionToRange({ fromPx: 50, toPx: 100 }, short)).toEqual({ from: 100, to: 250 });
  });
});

describe('bucketsToRange', () => {
  const chart = { range: { from: 1_000, to: 11_500 }, intervalMs: 1_000 };

  it('covers the first to the last bucket, inclusive', () => {
    expect(bucketsToRange(2, 4, chart)).toEqual({ from: 3_000, to: 6_000 });
    expect(bucketsToRange(0, 0, chart)).toEqual({ from: 1_000, to: 2_000 });
  });

  it('stops at the end of the range when the last bucket is shorter', () => {
    expect(bucketsToRange(9, 10, chart)).toEqual({ from: 10_000, to: 11_500 });
  });
});
