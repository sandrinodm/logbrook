/**
 * Time zone the explorer shows timestamps in: UTC, or the browser's own.
 */
export type TimeZoneMode = 'utc' | 'local';

/**
 * One segment of a message split around the search text.
 */
export type HighlightSegment = {
  /**
   * The segment's text.
   */
  readonly text: string;

  /**
   * Whether the segment is an occurrence of the search text.
   */
  readonly match: boolean;
};

/**
 * Clock formatter for UTC. Module-level, because building an `Intl.DateTimeFormat` costs far more
 * than using one and the table formats a timestamp per visible row.
 */
const CLOCK_UTC = new Intl.DateTimeFormat('en-GB', {
  timeZone: 'UTC',
  hourCycle: 'h23',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  fractionalSecondDigits: 3,
});

/**
 * Clock formatter for the browser's time zone.
 */
const CLOCK_LOCAL = new Intl.DateTimeFormat('en-GB', {
  hourCycle: 'h23',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  fractionalSecondDigits: 3,
});

/**
 * Calendar-day key formatter (`2026-11-10`) for UTC.
 */
const DAY_KEY_UTC = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'UTC',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
});

/**
 * Calendar-day key formatter for the browser's time zone.
 */
const DAY_KEY_LOCAL = new Intl.DateTimeFormat('en-CA', {
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
});

/**
 * Label formatter for a day separator in UTC, such as `Tue, 10 Nov 2026`.
 */
const DAY_LABEL_UTC = new Intl.DateTimeFormat('en-GB', {
  timeZone: 'UTC',
  weekday: 'short',
  day: 'numeric',
  month: 'short',
  year: 'numeric',
});

/**
 * Label formatter for a day separator in the browser's time zone.
 */
const DAY_LABEL_LOCAL = new Intl.DateTimeFormat('en-GB', {
  weekday: 'short',
  day: 'numeric',
  month: 'short',
  year: 'numeric',
});

/**
 * Formats a Unix-millisecond timestamp as `HH:mm:ss.SSS`, the one fixed format of the table.
 */
export function formatClock(ms: number, zone: TimeZoneMode): string {
  const formatter = zone === 'utc' ? CLOCK_UTC : CLOCK_LOCAL;
  return formatter.format(ms);
}

/**
 * Returns a key that is equal for two timestamps on the same calendar day in `zone`.
 */
export function dayKey(ms: number, zone: TimeZoneMode): string {
  const formatter = zone === 'utc' ? DAY_KEY_UTC : DAY_KEY_LOCAL;
  return formatter.format(ms);
}

/**
 * Formats the day of a timestamp for a day separator.
 */
export function formatDay(ms: number, zone: TimeZoneMode): string {
  const formatter = zone === 'utc' ? DAY_LABEL_UTC : DAY_LABEL_LOCAL;
  return formatter.format(ms);
}

/**
 * Formats a timestamp as `YYYY-MM-DD HH:mm:ss.SSS` followed by the zone name.
 */
export function formatDateTime(ms: number, zone: TimeZoneMode): string {
  const suffix = zone === 'utc' ? 'UTC' : 'local';
  return `${dayKey(ms, zone)} ${formatClock(ms, zone)} ${suffix}`;
}

/**
 * Splits `text` around every occurrence of `needle`, matching case-sensitively like the server's
 * `message` filter. An empty needle yields the whole text as one plain segment.
 */
export function splitHighlight(text: string, needle: string): ReadonlyArray<HighlightSegment> {
  if (needle === '') {
    return [{ text, match: false }];
  }

  const segments: HighlightSegment[] = [];
  let cursor = 0;
  let found = text.indexOf(needle, cursor);
  while (found >= 0) {
    if (found > cursor) {
      segments.push({ text: text.slice(cursor, found), match: false });
    }

    segments.push({ text: needle, match: true });
    cursor = found + needle.length;
    found = text.indexOf(needle, cursor);
  }

  if (cursor < text.length) {
    segments.push({ text: text.slice(cursor), match: false });
  }

  return segments;
}
