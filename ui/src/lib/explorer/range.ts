import type { HistogramBucket, TimeRange } from '@/lib/logbrook/types';

/**
 * Relative ranges the range picker offers, shortest first.
 */
export const RELATIVE_RANGES = ['5m', '15m', '1h', '6h', '24h', '7d', '30d'] as const;

/**
 * One of the relative ranges, such as `15m`.
 */
export type RelativeRange = (typeof RELATIVE_RANGES)[number];

/**
 * Range used when the URL names none.
 */
export const DEFAULT_RANGE: RelativeRange = '1h';

/**
 * Most buckets the server returns for one histogram request.
 */
export const MAX_HISTOGRAM_BUCKETS = 2_000;

/**
 * Smallest and largest bucket counts the interval picker aims for.
 */
const MIN_TARGET_BUCKETS = 24;

/**
 * Upper end of the bucket count the interval picker aims for; "about 150" in the plan.
 */
const MAX_TARGET_BUCKETS = 150;

/**
 * Narrowest bar the histogram draws, in pixels. Wider charts get more buckets, up to the cap.
 */
const MIN_BAR_PX = 8;

/**
 * Length of each relative range in milliseconds.
 */
const RANGE_LENGTH_MS: Readonly<Record<RelativeRange, number>> = {
  /**
   * Length of five minutes in milliseconds.
   */
  '5m': 5 * 60_000,

  /**
   * Length of fifteen minutes in milliseconds.
   */
  '15m': 15 * 60_000,

  /**
   * Length of one hour in milliseconds.
   */
  '1h': 3_600_000,

  /**
   * Length of six hours in milliseconds.
   */
  '6h': 6 * 3_600_000,

  /**
   * Length of 24 hours in milliseconds.
   */
  '24h': 24 * 3_600_000,

  /**
   * Length of seven days in milliseconds.
   */
  '7d': 7 * 86_400_000,

  /**
   * Length of 30 days in milliseconds.
   */
  '30d': 30 * 86_400_000,
};

/**
 * "Nice" bucket durations in ascending order, from a millisecond to a week.
 */
const NICE_INTERVALS_MS: ReadonlyArray<number> = [
  1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 15_000, 30_000, 60_000, 120_000,
  300_000, 600_000, 900_000, 1_800_000, 3_600_000, 7_200_000, 10_800_000, 21_600_000, 43_200_000,
  86_400_000, 604_800_000,
];

/**
 * Returns the length of a relative range in milliseconds.
 */
export function rangeLengthMs(range: RelativeRange): number {
  return RANGE_LENGTH_MS[range];
}

/**
 * Freezes a relative range into absolute bounds that end at `now`. A cursor is only valid for the
 * exact `from` and `to` it was issued for, so the explorer resolves a relative range once per
 * refresh and reuses the result until the next one.
 */
export function freezeRange(range: RelativeRange, now: number): TimeRange {
  return { from: now - RANGE_LENGTH_MS[range], to: now };
}

/**
 * Returns the next longer relative range, or `null` when `span` already reaches the longest one.
 * Used by the empty state's "widen the range" action.
 */
export function widerRange(span: number): RelativeRange | null {
  return RELATIVE_RANGES.find((range) => RANGE_LENGTH_MS[range] > span) ?? null;
}

/**
 * Returns the range that is twice as long and has the same centre, ending no later than `now`.
 */
export function zoomOutRange(range: TimeRange, now: number): TimeRange {
  const span = range.to - range.from;
  const to = Math.min(now, range.to + Math.floor(span / 2));
  return { from: Math.max(0, to - span * 2), to };
}

/**
 * Picks the histogram bucket duration for a time span drawn on a chart `widthPx` wide.
 *
 * It aims for one bucket per `MIN_BAR_PX` pixels, between 24 and 150 buckets, and rounds the
 * duration up to a "nice" step so labels read well and small width changes do not change the
 * interval (and the cache key). The result always keeps the bucket count within the server's limit
 * of 2,000.
 */
export function pickIntervalMs(spanMs: number, widthPx: number): number {
  const span = Math.max(1, spanMs);
  const target = Math.min(
    MAX_TARGET_BUCKETS,
    Math.max(MIN_TARGET_BUCKETS, Math.floor(widthPx / MIN_BAR_PX)),
  );
  const wanted = span / target;
  const step = NICE_INTERVALS_MS.find((interval) => interval >= wanted);
  const interval = step ?? Math.ceil(wanted);
  return Math.max(interval, Math.ceil(span / MAX_HISTOGRAM_BUCKETS));
}

/**
 * Turns the non-empty buckets the server returns into one entry per interval slot of the range,
 * with zero for the slots it left out. Slot `i` starts at `from + i * intervalMs`, which is how the
 * server aligns buckets. Buckets outside the range are ignored.
 */
export function zeroFill(
  buckets: ReadonlyArray<HistogramBucket>,
  range: TimeRange,
  intervalMs: number,
): ReadonlyArray<HistogramBucket> {
  const slots = Math.max(0, Math.ceil((range.to - range.from) / intervalMs));
  const counts = Array.from({ length: slots }, () => 0);
  for (const bucket of buckets) {
    const slot = Math.floor((bucket.time - range.from) / intervalMs);
    if (slot >= 0 && slot < slots) {
      counts[slot] = (counts[slot] ?? 0) + bucket.count;
    }
  }

  return counts.map((count, slot) => ({ time: range.from + slot * intervalMs, count }));
}

/**
 * Adds up the counts of a list of buckets. With the unfiltered histogram this is the number of
 * matching events, so the explorer needs no `/logs/count` request.
 */
export function sumBuckets(buckets: ReadonlyArray<HistogramBucket>): number {
  let total = 0;
  for (const bucket of buckets) {
    total += bucket.count;
  }

  return total;
}

/**
 * Converts a horizontal drag over the histogram into the time range it selects.
 *
 * `fromPx` and `toPx` are positions inside a chart `widthPx` wide that shows `slots` buckets of
 * `intervalMs` from `range.from`. The selection snaps outwards to bucket edges and stays inside the
 * range. Returns `null` for a drag narrower than `minDragPx`, which is a click rather than a zoom.
 */
export function selectionToRange(
  drag: { readonly fromPx: number; readonly toPx: number },
  chart: { readonly widthPx: number; readonly range: TimeRange; readonly intervalMs: number },
  minDragPx = 4,
): TimeRange | null {
  const { widthPx, range, intervalMs } = chart;
  if (Math.abs(drag.toPx - drag.fromPx) < minDragPx || widthPx <= 0) {
    return null;
  }

  const slots = Math.max(1, Math.ceil((range.to - range.from) / intervalMs));
  const slotPx = widthPx / slots;
  const first = Math.floor(clamp(Math.min(drag.fromPx, drag.toPx), 0, widthPx) / slotPx);
  const last = Math.ceil(clamp(Math.max(drag.fromPx, drag.toPx), 0, widthPx) / slotPx);
  if (last <= first) {
    return null;
  }

  return {
    from: range.from + first * intervalMs,
    to: Math.min(range.to, range.from + last * intervalMs),
  };
}

/**
 * The time range covered by buckets `first` to `last`, inclusive, of a chart that shows buckets of
 * `intervalMs` from `range.from`. The end stays inside the range, because the last bucket may be
 * shorter than the interval.
 */
export function bucketsToRange(
  first: number,
  last: number,
  chart: { readonly range: TimeRange; readonly intervalMs: number },
): TimeRange {
  const { range, intervalMs } = chart;
  return {
    from: range.from + first * intervalMs,
    to: Math.min(range.to, range.from + (last + 1) * intervalMs),
  };
}

/**
 * Limits `value` to the closed interval from `low` to `high`.
 */
function clamp(value: number, low: number, high: number): number {
  return Math.min(high, Math.max(low, value));
}
