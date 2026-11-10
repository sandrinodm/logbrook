/**
 * Milliseconds in one second.
 */
const SECOND_MS = 1000;

/**
 * Milliseconds in one minute.
 */
const MINUTE_MS = 60 * SECOND_MS;

/**
 * Milliseconds in one hour.
 */
const HOUR_MS = 60 * MINUTE_MS;

/**
 * Milliseconds in one day.
 */
const DAY_MS = 24 * HOUR_MS;

/**
 * Units of `formatBytes`, from bytes up. Decimal, as the server's own size limits are.
 */
const BYTE_UNITS = ['B', 'KB', 'MB', 'GB', 'TB'];

/**
 * Formats a byte count with decimal units (1 KB = 1000 B) and about three significant digits.
 * `null` and `undefined` print as an en dash, because the value is unknown, not zero.
 */
export function formatBytes(bytes: number | null | undefined): string {
  if (bytes === null || bytes === undefined || Number.isNaN(bytes)) {
    return '–';
  }

  if (bytes < 1000) {
    return `${Math.round(bytes)} B`;
  }

  const unit = Math.min(Math.floor(Math.log10(bytes) / 3), BYTE_UNITS.length - 1);
  const value = bytes / 1000 ** unit;
  return `${value.toFixed(fractionDigits(value))} ${BYTE_UNITS[unit]}`;
}

/**
 * Decimals that keep about three significant digits: none from 100, one from 10, otherwise two.
 */
function fractionDigits(value: number): number {
  if (value >= 100) {
    return 0;
  }

  return value >= 10 ? 1 : 2;
}

/**
 * Formats a retention in milliseconds using the largest unit that divides it exactly, so 7 days
 * reads `7d` and 36 hours `36h`. Seconds are only used below a day; longer values that no larger
 * unit divides print with one decimal in days.
 */
export function formatRetention(ms: number | undefined): string {
  if (ms === undefined) {
    return '–';
  }

  for (const [unitMs, label] of [
    [DAY_MS, 'd'],
    [HOUR_MS, 'h'],
    [MINUTE_MS, 'm'],
    [SECOND_MS, 's'],
  ] as const) {
    if (ms >= unitMs && ms % unitMs === 0 && (unitMs >= MINUTE_MS || ms < DAY_MS)) {
      return `${ms / unitMs}${label}`;
    }
  }

  return ms >= DAY_MS ? `${(ms / DAY_MS).toFixed(1)}d` : `${ms} ms`;
}

/**
 * Formats a count with thousands separators; `null` prints as an en dash.
 */
export function formatCount(count: number | null): string {
  return count === null ? '–' : Math.round(count).toLocaleString('en-US');
}

/**
 * Formats a rate in units per second with two significant digits for small values.
 */
export function formatRate(perSecond: number | null): string {
  if (perSecond === null) {
    return '–';
  }

  return perSecond >= 10 ? Math.round(perSecond).toLocaleString('en-US') : perSecond.toFixed(1);
}

/**
 * Formats a duration in seconds as milliseconds below one second, for example `12 ms`. `Infinity`
 * means beyond the slowest histogram bucket (5 s) and reads `> 5 s`.
 */
export function formatSeconds(seconds: number | null): string {
  if (seconds === null) {
    return '–';
  }

  if (!Number.isFinite(seconds)) {
    return '> 5 s';
  }

  if (seconds < 1) {
    return `${seconds < 0.01 ? (seconds * 1000).toFixed(1) : Math.round(seconds * 1000)} ms`;
  }

  return `${seconds.toFixed(1)} s`;
}

/**
 * Share of a size limit in use, as a percentage with one decimal, or `null` when there is no limit
 * or the size is unknown.
 */
export function usagePercent(sizeBytes: number | undefined, limitBytes: number | null | undefined) {
  if (sizeBytes === undefined || limitBytes === null || limitBytes === undefined) {
    return null;
  }

  return limitBytes === 0 ? null : Math.round((sizeBytes / limitBytes) * 1000) / 10;
}
