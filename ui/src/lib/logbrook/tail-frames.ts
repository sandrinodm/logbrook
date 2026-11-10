import type { LogEvent, TimeRange } from './types.ts';

/**
 * Whether an `EventSource` `error` event is an `event: error` frame sent by the server, as opposed
 * to the browser reporting a connection failure.
 *
 * Both arrive under the name `error`. Only the server's frame is a `MessageEvent` with text data; a
 * connection failure is a plain `Event`.
 */
export function isServerErrorFrame(event: Event): event is MessageEvent<string> {
  return event instanceof MessageEvent && typeof event.data === 'string';
}

/**
 * Parses the data of a `log` frame into an event. Returns `null` for anything that is not a JSON
 * object with a string `id`, so one malformed frame is skipped instead of ending the tail.
 */
export function parseLogFrame(data: string): LogEvent | null {
  try {
    const value: unknown = JSON.parse(data);
    if (typeof value !== 'object' || value === null || !('id' in value)) {
      return null;
    }

    return typeof value.id === 'string' ? (value as LogEvent) : null;
  } catch {
    return null;
  }
}

/**
 * The time window a reader should search to recover what a broken tail missed.
 *
 * It starts at the newest event the tail did receive (or when the tail was opened, if none arrived)
 * and ends now. `event_time` can trail ingestion, so the start is moved back by `marginMs`.
 */
export function missedWindow(
  lastEventTime: number | null,
  openedAt: number,
  now: number,
  marginMs: number,
): TimeRange {
  const start = (lastEventTime ?? openedAt) - marginMs;
  return { from: Math.max(0, start), to: now + 1 };
}
