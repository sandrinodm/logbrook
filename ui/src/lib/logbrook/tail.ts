import type { LogEvent, LogFilters, TimeRange } from './types.ts';

import { browserClient } from './client.ts';
import { errorFromResponse } from './errors.ts';
import { RESUME_PARAM } from './resume.ts';
import { RingBuffer, TAIL_BUFFER_CAPACITY } from './tail-buffer.ts';
import { isServerErrorFrame, missedWindow, parseLogFrame } from './tail-frames.ts';
import { backoffDelayMs, decideAfterFailure } from './tail-reconnect.ts';

/**
 * How far before the newest received event the "missed window" starts, in milliseconds. Events are
 * tailed in ingestion order, and a producer's `event_time` can trail ingestion.
 */
const MISSED_WINDOW_MARGIN_MS = 60_000;

/**
 * What the tail follows: an index and the filters a log must match.
 */
export type TailTarget = {
  /**
   * Index to follow.
   */
  readonly index: string;

  /**
   * Filters, as the search endpoints take them.
   */
  readonly filters: LogFilters;
};

/**
 * Where the connection stands.
 *
 * - `idle`: nothing is open.
 * - `connecting`: the first connection is being made.
 * - `live`: the stream is open.
 * - `reconnecting`: the stream dropped or was refused, and a retry is scheduled or running.
 * - `suspended`: closed on purpose while the tab was hidden; it resumes when the tab returns.
 * - `stopped`: ended for good by a `gap`, an `error` frame or a refusal a retry cannot fix.
 */
export type TailStatus = 'idle' | 'connecting' | 'live' | 'reconnecting' | 'suspended' | 'stopped';

/**
 * Why the tail needs the reader's attention.
 *
 * - `gap`: the server ended the stream because events were lost.
 * - `error`: the server's tail query failed.
 * - `limit`: the server has no free tail subscriber slot (`429`).
 * - `unauthorized`: Logbrook rejected the server's configured token (`401`).
 * - `forbidden`: the token is valid but its scope does not cover this index or these sources.
 * - `failed`: the server refused the request (bad filters, unknown index, event too large).
 * - `offline`: the connection dropped or the server could not be reached.
 */
export type TailNoticeKind =
  | 'gap'
  | 'error'
  | 'limit'
  | 'unauthorized'
  | 'forbidden'
  | 'failed'
  | 'offline';

/**
 * A condition the view shows next to the list.
 */
export type TailNotice = {
  /**
   * What kind of condition this is.
   */
  readonly kind: TailNoticeKind;

  /**
   * Explanation for people, from the server where it sent one.
   */
  readonly message: string;

  /**
   * For `gap` and `error`: the time window to search to recover missed events. Otherwise `null`.
   */
  readonly window: TimeRange | null;

  /**
   * When the next automatic attempt happens, in Unix milliseconds, or `null` when none is planned.
   */
  readonly retryAt: number | null;
};

/**
 * The immutable state the view renders. A new object is published whenever anything changes, at
 * most once per animation frame, and its `events` array is never modified.
 */
export type TailSnapshot = {
  /**
   * Buffered events, oldest first, at most `TAIL_BUFFER_CAPACITY`.
   */
  readonly events: ReadonlyArray<LogEvent>;

  /**
   * Number of events received since the tail was opened, including those already dropped. The
   * difference between two snapshots is the number of events that arrived between them.
   */
  readonly total: number;

  /**
   * Number of events dropped from the buffer because it was full.
   */
  readonly dropped: number;

  /**
   * Counts how many times the tail was opened or restarted. A view that keeps an old snapshot (for
   * example while the reader is scrolled up) must drop it when this changes: `total` starts over
   * and the events belong to another stream.
   */
  readonly epoch: number;

  /**
   * Connection status.
   */
  readonly status: TailStatus;

  /**
   * Whether the server has said the stream caught up with the newest committed event.
   */
  readonly caughtUp: boolean;

  /**
   * The condition to explain, or `null`.
   */
  readonly notice: TailNotice | null;
};

/**
 * The part of `EventSource` the store uses, so tests can substitute a fake.
 */
export type TailSource = {
  /**
   * Registers a listener for an event name.
   */
  addEventListener: (type: string, listener: (event: Event) => void) => void;

  /**
   * Closes the connection for good; the source never reconnects by itself afterwards.
   */
  close: () => void;
};

/**
 * What a failed connection attempt found out when probed over `fetch`.
 */
export type ProbeResult = {
  /**
   * HTTP status, or `null` when there was no response at all.
   */
  readonly status: number | null;

  /**
   * Seconds from `Retry-After`, or `null`.
   */
  readonly retryAfterSeconds: number | null;

  /**
   * The server's error message, or a description of the failure.
   */
  readonly message: string;
};

/**
 * Everything the store takes from its environment, replaced in tests.
 */
export type TailDeps = {
  /**
   * Opens an event stream at a URL.
   */
  readonly createSource: (url: string) => TailSource;

  /**
   * Requests the stream once with `fetch` to learn the HTTP status that `EventSource` hides. It
   * must stop reading as soon as the headers arrive.
   */
  readonly probe: (url: string, signal: AbortSignal) => Promise<ProbeResult>;

  /**
   * Runs a callback before the next paint and returns a function that cancels it.
   */
  readonly requestFrame: (callback: () => void) => () => void;

  /**
   * Current time in Unix milliseconds.
   */
  readonly now: () => number;

  /**
   * Random number in [0, 1), for backoff jitter.
   */
  readonly random: () => number;

  /**
   * URL of the tail stream for a target; the store adds the resume parameter.
   */
  readonly streamUrl: (target: TailTarget) => string;

  /**
   * How many events the buffer keeps.
   */
  readonly capacity: number;
};

/**
 * A listener of store changes.
 */
type Listener = () => void;

/**
 * One open stream and what has happened on it.
 */
type Connection = {
  /**
   * The event stream.
   */
  readonly source: TailSource;

  /**
   * Whether the browser reported the stream open, which means the server answered `200`.
   */
  opened: boolean;
};

/**
 * The snapshot before anything is opened. Also the server snapshot, since the tail never runs on
 * the server.
 */
const IDLE_SNAPSHOT: TailSnapshot = {
  /**
   * No events.
   */
  events: [],

  /**
   * None received.
   */
  total: 0,

  /**
   * Never opened.
   */
  epoch: 0,

  /**
   * None dropped.
   */
  dropped: 0,

  /**
   * Nothing is open.
   */
  status: 'idle',

  /**
   * Not caught up, because not connected.
   */
  caughtUp: false,

  /**
   * Nothing to explain.
   */
  notice: null,
};

/**
 * Appends the resume ID to a stream URL.
 */
export function withResumeId(url: string, resumeId: string | null): string {
  if (resumeId === null) {
    return url;
  }

  const separator = url.includes('?') ? '&' : '?';
  return `${url}${separator}${RESUME_PARAM}=${encodeURIComponent(resumeId)}`;
}

/**
 * Follows one index's live tail outside React.
 *
 * Frames go straight into a ring buffer. React sees them through immutable snapshots that are
 * published at most once per animation frame, so a burst of events costs one render per frame. The
 * store reconnects with the last resume ID and exponential backoff, and stops for good on `gap`,
 * `error` frames and refusals a retry cannot fix.
 *
 * `EventSource` reports every connection failure as one bare `error` event with no status code and,
 * on any non-`200` response, gives up and closes. The store closes the source itself on every
 * failure and, when the stream never opened, repeats the request once with `fetch` (the probe) to
 * read the status and `Retry-After`. That costs a request only on failures, never on a healthy
 * stream, and tells a `429` or `401` from a network drop.
 */
export class TailStore {
  /**
   * Everything taken from the environment.
   */
  private readonly deps: TailDeps;

  /**
   * The newest events, up to the capacity.
   */
  private readonly ring: RingBuffer<LogEvent>;

  /**
   * Functions to call after each publish.
   */
  private readonly listeners = new Set<Listener>();

  /**
   * The last published snapshot.
   */
  private snapshot: TailSnapshot = IDLE_SNAPSHOT;

  /**
   * The event array of the last snapshot, reused until events change.
   */
  private cachedEvents: ReadonlyArray<LogEvent> = [];

  /**
   * Whether events changed since `cachedEvents` was copied.
   */
  private eventsChanged = false;

  /**
   * What is being followed, or `null` when closed.
   */
  private target: TailTarget | null = null;

  /**
   * The current stream; `null` while waiting to reconnect or when stopped.
   */
  private connection: Connection | null = null;

  /**
   * The SSE id of the last `log` or `cursor` frame, sent as `Last-Event-ID` when resuming.
   */
  private resumeId: string | null = null;

  /**
   * Connection status.
   */
  private status: TailStatus = 'idle';

  /**
   * The condition to explain, if any.
   */
  private notice: TailNotice | null = null;

  /**
   * Whether a `cursor` frame has arrived.
   */
  private caughtUp = false;

  /**
   * Events received since `open`.
   */
  private total = 0;

  /**
   * How many times `open` ran.
   */
  private epoch = 0;

  /**
   * Consecutive failed attempts since a frame last arrived; drives the backoff.
   */
  private attempt = 0;

  /**
   * When `open` was called, in Unix milliseconds.
   */
  private openedAt = 0;

  /**
   * `event_time` of the newest received event.
   */
  private lastEventTime: number | null = null;

  /**
   * Timer of the scheduled reconnect.
   */
  private retryTimer: ReturnType<typeof setTimeout> | null = null;

  /**
   * Cancels the probe that is running.
   */
  private probeAbort: AbortController | null = null;

  /**
   * Cancels the scheduled publish.
   */
  private cancelFrame: (() => void) | null = null;

  constructor(deps: TailDeps) {
    this.deps = deps;
    this.ring = new RingBuffer<LogEvent>(deps.capacity);
  }

  /**
   * Registers a listener. Stable per store, so React never resubscribes.
   */
  readonly subscribe = (listener: Listener): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  /**
   * The current snapshot. It is the same object until the next publish.
   */
  readonly getSnapshot = (): TailSnapshot => this.snapshot;

  /**
   * The snapshot for server rendering: always idle.
   */
  readonly getServerSnapshot = (): TailSnapshot => IDLE_SNAPSHOT;

  /**
   * Starts a fresh tail of `target`, discarding anything buffered and any previous stream.
   */
  open(target: TailTarget): void {
    this.teardown();
    this.ring.clear();
    this.eventsChanged = true;
    this.epoch += 1;
    this.target = target;
    this.resumeId = null;
    this.notice = null;
    this.caughtUp = false;
    this.total = 0;
    this.attempt = 0;
    this.lastEventTime = null;
    this.openedAt = this.deps.now();
    this.connect();
  }

  /**
   * Closes the stream and goes idle. The buffer is kept until the next `open`.
   */
  close(): void {
    this.teardown();
    this.target = null;
    this.notice = null;
    this.setStatus('idle');
  }

  /**
   * Closes the stream while the tab is hidden, keeping the buffer and the resume ID. Does nothing
   * unless a stream is open or being retried.
   */
  suspend(): void {
    if (this.status === 'idle' || this.status === 'stopped' || this.status === 'suspended') {
      return;
    }

    this.teardown();
    this.notice = null;
    this.setStatus('suspended');
  }

  /**
   * Reopens a suspended tail from the last resume ID. Does nothing in any other state.
   */
  resume(): void {
    if (this.status !== 'suspended') {
      return;
    }

    this.attempt = 0;
    this.connect();
  }

  /**
   * Retries a tail that stopped on an `error` frame or a refusal, resuming after the last event.
   * After a `gap` use `restart`, because resuming cannot recover expired events.
   */
  reconnect(): void {
    if (this.status !== 'stopped' || this.notice?.kind === 'gap') {
      return;
    }

    this.attempt = 0;
    this.connect();
  }

  /**
   * Skips the wait before a scheduled retry and connects now. Does nothing unless a retry is
   * scheduled.
   */
  retryNow(): void {
    if (this.status === 'reconnecting' && this.retryTimer !== null) {
      this.attempt = 0;
      this.connect();
    }
  }

  /**
   * Starts over on the same target from the newest event, dropping the buffer.
   */
  restart(): void {
    if (this.target !== null) {
      this.open(this.target);
    }
  }

  /**
   * Opens the stream, resuming after the last resume ID when there is one, and publishes.
   */
  private connect(): void {
    const target = this.target;
    if (target === null) {
      return;
    }

    this.clearRetry();
    this.notice = null;
    const url = withResumeId(this.deps.streamUrl(target), this.resumeId);
    const source = this.deps.createSource(url);
    const connection: Connection = { source, opened: false };
    this.connection = connection;
    this.status = this.total === 0 && this.resumeId === null ? 'connecting' : 'reconnecting';
    this.listen(connection, url);
    this.publishNow();
  }

  /**
   * Attaches the frame and connection listeners of one stream. Events from a stream that is no
   * longer current are ignored.
   */
  private listen(connection: Connection, url: string): void {
    const { source } = connection;
    /**
     * Whether this stream is still the one the store follows.
     */
    const current = () => this.connection === connection;
    source.addEventListener('open', () => {
      if (current()) {
        connection.opened = true;
        this.status = 'live';
        this.schedule();
      }
    });
    source.addEventListener('log', (event) => {
      if (current()) {
        this.onLog(event as MessageEvent<string>);
      }
    });
    source.addEventListener('cursor', (event) => {
      if (current()) {
        this.onCursor(event as MessageEvent<string>);
      }
    });
    source.addEventListener('gap', (event) => {
      if (current()) {
        this.stop('gap', (event as MessageEvent<string>).data);
      }
    });
    source.addEventListener('error', (event) => {
      if (!current()) {
        return;
      }

      if (isServerErrorFrame(event)) {
        this.stop('error', event.data);
      } else {
        this.onConnectionError(connection, url);
      }
    });
  }

  /**
   * Buffers one event and remembers its resume ID.
   */
  private onLog(frame: MessageEvent<string>): void {
    const event = parseLogFrame(frame.data);
    if (event === null) {
      return;
    }

    this.ring.push(event);
    this.eventsChanged = true;
    this.total += 1;
    this.lastEventTime = event.event_time;
    this.acknowledge(frame.lastEventId);
  }

  /**
   * Marks the stream caught up and remembers its resume point.
   */
  private onCursor(frame: MessageEvent<string>): void {
    this.caughtUp = true;
    this.acknowledge(frame.lastEventId);
  }

  /**
   * Records that a frame arrived: the stream works, so backoff starts over.
   */
  private acknowledge(frameId: string): void {
    if (frameId !== '') {
      this.resumeId = frameId;
    }

    this.attempt = 0;
    this.status = 'live';
    this.notice = null;
    this.schedule();
  }

  /**
   * Ends the stream for good with a `gap` or `error` notice and the window to search instead.
   */
  private stop(kind: 'gap' | 'error', message: string): void {
    this.teardown();
    const window = missedWindow(
      this.lastEventTime,
      this.openedAt,
      this.deps.now(),
      MISSED_WINDOW_MARGIN_MS,
    );
    this.notice = { kind, message, window, retryAt: null };
    this.setStatus('stopped');
  }

  /**
   * Handles a failed or dropped connection. A stream that had opened was dropped: retry with
   * backoff. One that never opened was refused or unreachable: probe to learn why.
   */
  private onConnectionError(connection: Connection, url: string): void {
    connection.source.close();
    this.connection = null;
    if (connection.opened) {
      this.scheduleRetry('offline', 'Connection lost.', this.backoff());
      return;
    }

    const abort = new AbortController();
    this.probeAbort = abort;
    this.deps.probe(url, abort.signal).then(
      (result) => {
        if (this.probeAbort === abort) {
          this.probeAbort = null;
          this.onProbe(result);
        }
      },
      () => {
        if (this.probeAbort === abort) {
          this.probeAbort = null;
          this.scheduleRetry('offline', 'Could not reach the server.', this.backoff());
        }
      },
    );
  }

  /**
   * Acts on what the probe found. A `200` means the failure has passed; retry.
   */
  private onProbe(result: ProbeResult): void {
    const status = result.status === 200 ? null : result.status;
    const decision = decideAfterFailure(
      status,
      result.retryAfterSeconds,
      this.attempt,
      this.deps.random,
    );
    if (decision.action === 'stop') {
      this.teardown();
      const kind = stopKind(decision);
      this.notice = { kind, message: result.message, window: null, retryAt: null };
      this.setStatus('stopped');
      return;
    }

    const kind = status === 429 ? 'limit' : 'offline';
    this.scheduleRetry(kind, result.message, decision.delayMs);
  }

  /**
   * The backoff delay for the current attempt number.
   */
  private backoff(): number {
    return backoffDelayMs(this.attempt, this.deps.random);
  }

  /**
   * Schedules the next connection attempt and publishes the reason.
   */
  private scheduleRetry(kind: 'limit' | 'offline', message: string, delayMs: number): void {
    this.attempt += 1;
    this.notice = { kind, message, window: null, retryAt: this.deps.now() + delayMs };
    this.clearRetry();
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, delayMs);
    this.setStatus('reconnecting');
  }

  /**
   * Closes the stream and cancels the retry, probe and pending publish.
   */
  private teardown(): void {
    this.clearRetry();
    this.probeAbort?.abort();
    this.probeAbort = null;
    this.connection?.source.close();
    this.connection = null;
    this.cancelFrame?.();
    this.cancelFrame = null;
  }

  /**
   * Cancels the scheduled reconnect, if any.
   */
  private clearRetry(): void {
    if (this.retryTimer !== null) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
  }

  /**
   * Changes the status and publishes immediately. Status changes are rare, unlike frames.
   */
  private setStatus(status: TailStatus): void {
    this.status = status;
    this.publishNow();
  }

  /**
   * Asks for a publish before the next paint, unless one is already waiting. Frame handlers call
   * this, so any number of frames in one animation frame cost one snapshot.
   */
  private schedule(): void {
    if (this.cancelFrame !== null) {
      return;
    }

    this.cancelFrame = this.deps.requestFrame(() => {
      this.cancelFrame = null;
      this.publish();
    });
  }

  /**
   * Publishes now and drops a pending scheduled publish.
   */
  private publishNow(): void {
    this.cancelFrame?.();
    this.cancelFrame = null;
    this.publish();
  }

  /**
   * Builds a new snapshot and tells the listeners. The event array is copied from the ring only
   * when events changed; a status-only change reuses the previous array.
   */
  private publish(): void {
    if (this.eventsChanged) {
      this.cachedEvents = this.ring.toArray();
      this.eventsChanged = false;
    }

    this.snapshot = {
      events: this.cachedEvents,
      total: this.total,
      dropped: this.ring.dropped,
      epoch: this.epoch,
      status: this.status,
      caughtUp: this.caughtUp,
      notice: this.notice,
    };
    for (const listener of this.listeners) {
      listener();
    }
  }
}

/**
 * Requests the stream with `fetch` and reports the status. The body is cancelled as soon as the
 * headers arrive, so a `200` does not hold a subscriber slot.
 */
async function probeStream(url: string, signal: AbortSignal): Promise<ProbeResult> {
  try {
    const response = await fetch(url, { headers: { accept: 'text/event-stream' }, signal });
    if (response.ok) {
      await response.body?.cancel();
      return { status: response.status, retryAfterSeconds: null, message: 'Connection failed.' };
    }

    const error = await errorFromResponse(response);
    return {
      status: error.status,
      retryAfterSeconds: error.retryAfterSeconds,
      message: error.message,
    };
  } catch (error) {
    if (signal.aborted) {
      throw error;
    }

    return { status: null, retryAfterSeconds: null, message: 'Could not reach the server.' };
  }
}

/**
 * Runs `callback` before the next paint. Falls back to a 16 ms timer where `requestAnimationFrame`
 * is missing; browsers also pause it in background tabs, which is fine because the ring buffer
 * stays bounded without it.
 */
function requestFrame(callback: () => void): () => void {
  if (typeof requestAnimationFrame === 'function') {
    const handle = requestAnimationFrame(callback);
    return () => cancelAnimationFrame(handle);
  }

  const timer = setTimeout(callback, 16);
  return () => clearTimeout(timer);
}

/**
 * The dependencies the app uses: the real `EventSource`, `fetch`, clock and the proxy URL.
 */
export const browserTailDeps: TailDeps = {
  /**
   * Opens a native `EventSource`.
   */
  createSource: (url) => new EventSource(url),

  /**
   * Repeats the request with `fetch` to read the status.
   */
  probe: probeStream,

  /**
   * Runs the publish before the next paint.
   */
  requestFrame,

  /**
   * The system clock.
   */
  now: () => Date.now(),

  /**
   * Jitter source for the backoff.
   */
  random: () => Math.random(),

  /**
   * The proxy URL of the tail for a target, with its filters.
   */
  streamUrl: (target) => browserClient.tailUrl(target.index, target.filters),

  /**
   * The ring buffer size.
   */
  capacity: TAIL_BUFFER_CAPACITY,
};

/**
 * The app's one tail store. Views open and close it; only one tail runs at a time, which also
 * respects the server's limit of eight tail subscribers.
 */
export const tailStore = new TailStore(browserTailDeps);

/**
 * The notice kind for a refusal the tail does not retry: a rejected token, a token whose scope does
 * not cover the target, or a request the server rejected.
 */
function stopKind(decision: { unauthorized: boolean; forbidden: boolean }): TailNoticeKind {
  if (decision.unauthorized) {
    return 'unauthorized';
  }

  return decision.forbidden ? 'forbidden' : 'failed';
}
