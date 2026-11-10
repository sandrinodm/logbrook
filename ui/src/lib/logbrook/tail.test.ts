import { afterEach, beforeEach, describe, expect, it, vi } from 'vite-plus/test';

import type { ProbeResult, TailDeps, TailSource } from './tail.ts';

import { TailStore, withResumeId } from './tail.ts';

/**
 * A stream the test drives by hand.
 */
class FakeSource implements TailSource {
  readonly url: string;

  closed = false;

  private readonly listeners = new Map<string, Array<(event: Event) => void>>();

  constructor(url: string) {
    this.url = url;
  }

  addEventListener(type: string, listener: (event: Event) => void): void {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }

  close(): void {
    this.closed = true;
  }

  /**
   * Delivers an event of `type`, as the browser would.
   */
  emit(type: string, event: Event): void {
    for (const listener of this.listeners.get(type) ?? []) {
      listener(event);
    }
  }

  /**
   * Delivers a server frame with an SSE id.
   */
  frame(type: string, data: string, id = ''): void {
    this.emit(type, new MessageEvent(type, { data, lastEventId: id }));
  }

  /**
   * Delivers a `log` frame for an event with the given id and time.
   */
  log(id: string, eventTime = 1_000): void {
    const event = { id, message: `m${id}`, level: 30, event_time: eventTime };
    this.frame('log', JSON.stringify(event), `idx:v1.${id}`);
  }
}

/**
 * A store wired to fakes. Frames run when the test calls `flushFrame`.
 */
function setup(options: { capacity?: number; probe?: ProbeResult } = {}) {
  const sources: Array<FakeSource> = [];
  let pendingFrame: (() => void) | null = null;
  let requestedFrames = 0;
  const probe = vi.fn(
    async (): Promise<ProbeResult> =>
      options.probe ?? { status: null, retryAfterSeconds: null, message: 'down' },
  );
  const deps: TailDeps = {
    createSource: (url) => {
      const source = new FakeSource(url);
      sources.push(source);
      return source;
    },
    probe,
    requestFrame: (callback) => {
      requestedFrames += 1;
      pendingFrame = callback;
      return () => {
        pendingFrame = null;
      };
    },
    now: () => Date.now(),
    random: () => 0,
    streamUrl: (target) => `/api/lb/indexes/${target.index}/logs/tail?service=checkout`,
    capacity: options.capacity ?? 100,
  };
  const store = new TailStore(deps);
  const publishes = vi.fn();
  store.subscribe(publishes);
  return {
    store,
    sources,
    probe,
    publishes,
    flushFrame: () => {
      const run = pendingFrame;
      pendingFrame = null;
      run?.();
    },
    frameRequests: () => requestedFrames,
    latest: () => sources[sources.length - 1] as FakeSource,
  };
}

const TARGET = { index: 'idx', filters: { service: 'checkout' } };

beforeEach(() => {
  vi.useFakeTimers();
  vi.setSystemTime(1_000_000);
});

afterEach(() => {
  vi.useRealTimers();
});

describe('withResumeId', () => {
  it('appends the id with the right separator and encoding', () => {
    expect(withResumeId('/t?a=1', 'i:v1.x')).toBe('/t?a=1&last_event_id=i%3Av1.x');
    expect(withResumeId('/t?', 'i')).toBe('/t?&last_event_id=i');
    expect(withResumeId('/t', 'i')).toBe('/t?last_event_id=i');
    expect(withResumeId('/t', null)).toBe('/t');
  });
});

describe('frame handling', () => {
  it('buffers log frames oldest first and counts them', () => {
    const { store, latest, flushFrame } = setup();
    store.open(TARGET);

    latest().log('1');
    latest().log('2');
    flushFrame();

    const snapshot = store.getSnapshot();
    expect(snapshot.events.map((event) => event.id)).toEqual(['1', '2']);
    expect(snapshot.total).toBe(2);
    expect(snapshot.status).toBe('live');
  });

  it('skips malformed log data without stopping', () => {
    const { store, latest, flushFrame } = setup();
    store.open(TARGET);

    latest().frame('log', 'not json');
    latest().log('1');
    flushFrame();

    expect(store.getSnapshot().events).toHaveLength(1);
    expect(store.getSnapshot().status).toBe('live');
  });

  it('marks the stream caught up on a cursor frame', () => {
    const { store, latest, flushFrame } = setup();
    store.open(TARGET);
    expect(store.getSnapshot().caughtUp).toBe(false);

    latest().frame('cursor', 'caught up', 'idx:v1.c');
    flushFrame();

    expect(store.getSnapshot().caughtUp).toBe(true);
  });

  it('ends on a gap frame with the window to search, and refuses to resume', () => {
    const { store, latest, sources } = setup();
    store.open(TARGET);
    latest().log('1', 500_000);

    latest().frame('gap', 'tail cursor expired');

    const snapshot = store.getSnapshot();
    expect(snapshot.status).toBe('stopped');
    expect(snapshot.notice).toEqual({
      kind: 'gap',
      message: 'tail cursor expired',
      window: { from: 440_000, to: 1_000_001 },
      retryAt: null,
    });
    expect(latest().closed).toBe(true);
    store.reconnect();
    expect(sources).toHaveLength(1);
  });

  it('ends on a server error frame, and reconnect resumes from the last id', () => {
    const { store, latest, sources } = setup();
    store.open(TARGET);
    latest().log('1');

    latest().emit('error', new MessageEvent('error', { data: 'tail query failed' }));

    expect(store.getSnapshot().notice).toMatchObject({
      kind: 'error',
      message: 'tail query failed',
    });
    expect(store.getSnapshot().status).toBe('stopped');
    store.reconnect();
    expect(sources).toHaveLength(2);
    expect(latest().url).toContain('last_event_id=idx%3Av1.1');
  });
});

describe('snapshot batching', () => {
  it('publishes once per frame however many events arrive', () => {
    const { store, latest, flushFrame, frameRequests, publishes } = setup({ capacity: 2_000 });
    store.open(TARGET);
    const before = publishes.mock.calls.length;
    const requestsBefore = frameRequests();

    for (let id = 0; id < 1_000; id += 1) {
      latest().log(String(id));
    }

    expect(publishes.mock.calls.length).toBe(before);
    expect(frameRequests() - requestsBefore).toBe(1);
    flushFrame();
    expect(publishes.mock.calls.length).toBe(before + 1);
    expect(store.getSnapshot().events).toHaveLength(1_000);
  });

  it('returns the same snapshot until something changes', () => {
    const { store, latest, flushFrame } = setup();
    store.open(TARGET);
    latest().log('1');
    flushFrame();

    const first = store.getSnapshot();

    expect(store.getSnapshot()).toBe(first);
    latest().log('2');
    flushFrame();
    expect(store.getSnapshot()).not.toBe(first);
    expect(first.events).toHaveLength(1);
  });

  it('keeps the events array when only the status changes', () => {
    const { store, latest, flushFrame } = setup();
    store.open(TARGET);
    latest().log('1');
    flushFrame();
    const events = store.getSnapshot().events;

    store.suspend();

    expect(store.getSnapshot().events).toBe(events);
    expect(store.getSnapshot().status).toBe('suspended');
  });

  it('caps the buffer and counts the dropped events', () => {
    const { store, latest, flushFrame } = setup({ capacity: 5 });
    store.open(TARGET);

    for (let id = 1; id <= 12; id += 1) {
      latest().log(String(id));
    }
    flushFrame();

    const snapshot = store.getSnapshot();
    expect(snapshot.events.map((event) => event.id)).toEqual(['8', '9', '10', '11', '12']);
    expect(snapshot.total).toBe(12);
    expect(snapshot.dropped).toBe(7);
  });
});

describe('reconnect and resume', () => {
  it('reconnects after a drop with the last id, after a delay', () => {
    const { store, latest, sources } = setup();
    store.open(TARGET);
    latest().emit('open', new Event('open'));
    latest().log('1');

    latest().emit('error', new Event('error'));

    expect(store.getSnapshot().status).toBe('reconnecting');
    expect(latest().closed).toBe(true);
    expect(sources).toHaveLength(1);
    vi.advanceTimersByTime(999);
    expect(sources).toHaveLength(1);
    vi.advanceTimersByTime(1);
    expect(sources).toHaveLength(2);
    expect(latest().url).toBe(
      '/api/lb/indexes/idx/logs/tail?service=checkout&last_event_id=idx%3Av1.1',
    );
  });

  it('backs off while no frame arrives and starts over once one does', () => {
    const { store, latest, sources } = setup();
    store.open(TARGET);
    latest().emit('open', new Event('open'));

    latest().emit('error', new Event('error'));
    vi.advanceTimersByTime(1_000);
    latest().emit('open', new Event('open'));
    latest().emit('error', new Event('error'));
    vi.advanceTimersByTime(1_999);
    expect(sources).toHaveLength(2);
    vi.advanceTimersByTime(1);
    expect(sources).toHaveLength(3);

    latest().emit('open', new Event('open'));
    latest().log('1');
    latest().emit('error', new Event('error'));
    vi.advanceTimersByTime(1_000);

    expect(sources).toHaveLength(4);
  });

  it('ignores events from a stream that was replaced', () => {
    const { store, latest, flushFrame, sources } = setup();
    store.open(TARGET);
    const old = latest();

    store.open({ index: 'other', filters: {} });
    old.log('stale');
    old.emit('error', new Event('error'));
    flushFrame();

    expect(old.closed).toBe(true);
    expect(sources).toHaveLength(2);
    expect(store.getSnapshot().total).toBe(0);
  });

  it('counts every open and restart as a new epoch, so views can drop snapshots of an old stream', () => {
    const { store } = setup();
    expect(store.getSnapshot().epoch).toBe(0);

    store.open(TARGET);
    const first = store.getSnapshot().epoch;
    store.open({ index: 'other', filters: {} });
    const second = store.getSnapshot().epoch;
    store.restart();
    const third = store.getSnapshot().epoch;

    expect([first, second, third]).toEqual([1, 2, 3]);
  });

  it('starts a new target without resuming from the old one', () => {
    const { store, latest } = setup();
    store.open(TARGET);
    latest().log('1');

    store.open({ index: 'other', filters: {} });

    expect(latest().url).not.toContain('last_event_id');
    expect(store.getSnapshot().events).toEqual([]);
  });
});

describe('failures before the stream starts', () => {
  it('explains the subscriber limit and retries after Retry-After', async () => {
    const { store, latest, sources } = setup({
      probe: { status: 429, retryAfterSeconds: 7, message: 'tail subscriber limit exhausted' },
    });
    store.open(TARGET);

    latest().emit('error', new Event('error'));
    await vi.advanceTimersByTimeAsync(0);

    expect(store.getSnapshot().status).toBe('reconnecting');
    expect(store.getSnapshot().notice).toEqual({
      kind: 'limit',
      message: 'tail subscriber limit exhausted',
      window: null,
      retryAt: 1_007_000,
    });
    await vi.advanceTimersByTimeAsync(7_000);
    expect(sources).toHaveLength(2);
  });

  it('stops and reports a rejected server token on 401', async () => {
    const { store, latest, sources } = setup({
      probe: { status: 401, retryAfterSeconds: null, message: 'unauthorized' },
    });
    store.open(TARGET);

    latest().emit('error', new Event('error'));
    await vi.advanceTimersByTimeAsync(60_000);

    expect(store.getSnapshot().status).toBe('stopped');
    expect(store.getSnapshot().notice?.kind).toBe('unauthorized');
    expect(sources).toHaveLength(1);
  });

  it('stops on a 403 as a scope refusal, not a rejected token', async () => {
    const { store, latest, sources } = setup({
      probe: { status: 403, retryAfterSeconds: null, message: 'source not allowed' },
    });
    store.open(TARGET);

    latest().emit('error', new Event('error'));
    await vi.advanceTimersByTimeAsync(60_000);

    expect(store.getSnapshot().status).toBe('stopped');
    expect(store.getSnapshot().notice).toMatchObject({
      kind: 'forbidden',
      message: 'source not allowed',
    });
    expect(sources).toHaveLength(1);
  });

  it('stops on a request the server rejects', async () => {
    const { store, latest } = setup({
      probe: { status: 400, retryAfterSeconds: null, message: 'invalid filter' },
    });
    store.open(TARGET);

    latest().emit('error', new Event('error'));
    await vi.advanceTimersByTimeAsync(0);

    expect(store.getSnapshot().notice).toMatchObject({ kind: 'failed', message: 'invalid filter' });
  });

  it('treats an unreachable server as a network drop and retries', async () => {
    const { store, latest, sources } = setup();
    store.open(TARGET);

    latest().emit('error', new Event('error'));
    await vi.advanceTimersByTimeAsync(0);

    expect(store.getSnapshot().notice?.kind).toBe('offline');
    await vi.advanceTimersByTimeAsync(1_000);
    expect(sources).toHaveLength(2);
  });

  it('does not act on a probe that finishes after the tail was closed', async () => {
    const { store, latest, sources } = setup({
      probe: { status: 429, retryAfterSeconds: 1, message: 'busy' },
    });
    store.open(TARGET);
    latest().emit('error', new Event('error'));

    store.close();
    await vi.advanceTimersByTimeAsync(10_000);

    expect(store.getSnapshot().status).toBe('idle');
    expect(sources).toHaveLength(1);
  });
});

describe('suspend and resume', () => {
  it('closes the stream, keeps the events, and resumes with the last id', () => {
    const { store, latest, flushFrame, sources } = setup();
    store.open(TARGET);
    latest().log('1');
    flushFrame();

    store.suspend();

    expect(latest().closed).toBe(true);
    expect(store.getSnapshot().events).toHaveLength(1);
    store.resume();
    expect(sources).toHaveLength(2);
    expect(latest().url).toContain('last_event_id=idx%3Av1.1');
  });

  it('cancels a pending retry when suspended', () => {
    const { store, latest, sources } = setup();
    store.open(TARGET);
    latest().emit('open', new Event('open'));
    latest().emit('error', new Event('error'));

    store.suspend();
    vi.advanceTimersByTime(60_000);

    expect(sources).toHaveLength(1);
  });

  it('does nothing to a stopped or idle tail', () => {
    const { store, sources } = setup();

    store.suspend();
    store.resume();

    expect(store.getSnapshot().status).toBe('idle');
    expect(sources).toHaveLength(0);
  });
});

describe('retryNow', () => {
  it('connects at once instead of waiting for the backoff', async () => {
    const { store, latest, sources } = setup({
      probe: { status: 429, retryAfterSeconds: 30, message: 'busy' },
    });
    store.open(TARGET);
    latest().emit('error', new Event('error'));
    await vi.advanceTimersByTimeAsync(0);

    store.retryNow();

    expect(sources).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(60_000);
    expect(sources).toHaveLength(2);
  });
});

describe('restart', () => {
  it('starts over from the newest event after a gap', () => {
    const { store, latest, sources } = setup();
    store.open(TARGET);
    latest().log('1');
    latest().frame('gap', 'expired');

    store.restart();

    expect(sources).toHaveLength(2);
    expect(latest().url).not.toContain('last_event_id');
    expect(store.getSnapshot().events).toEqual([]);
    expect(store.getSnapshot().notice).toBeNull();
  });
});
