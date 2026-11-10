/**
 * How long the tab may stay hidden before the tail closes, in milliseconds. A short switch to
 * another tab keeps the stream; a long one frees the server's subscriber slot.
 */
export const HIDDEN_CLOSE_DELAY_MS = 60_000;

/**
 * What the watcher needs from a tail store.
 */
export type SuspendableTail = {
  /**
   * Closes the stream but keeps the buffer and the resume point.
   */
  readonly suspend: () => void;

  /**
   * Reopens a suspended stream.
   */
  readonly resume: () => void;
};

/**
 * What the watcher needs from `document`.
 */
export type VisibilitySource = {
  /**
   * `'hidden'` while the tab is in the background.
   */
  readonly visibilityState: DocumentVisibilityState;

  /**
   * Registers a `visibilitychange` listener.
   */
  addEventListener: (type: 'visibilitychange', listener: () => void) => void;

  /**
   * Removes a `visibilitychange` listener.
   */
  removeEventListener: (type: 'visibilitychange', listener: () => void) => void;
};

/**
 * Suspends `tail` once the tab has been hidden for `delayMs`, and resumes it when the tab is shown
 * again. Returns a function that stops watching and cancels a pending timer.
 *
 * Coming back before the delay passes only cancels the timer: the stream never closed.
 */
export function watchTabVisibility(
  tail: SuspendableTail,
  source: VisibilitySource,
  delayMs: number = HIDDEN_CLOSE_DELAY_MS,
): () => void {
  let timer: ReturnType<typeof setTimeout> | null = null;

  /**
   * Cancels the pending suspend, if any.
   */
  const cancel = () => {
    if (timer !== null) {
      clearTimeout(timer);
      timer = null;
    }
  };

  /**
   * Starts the suspend timer when the tab is hidden; cancels it and resumes when shown.
   */
  const onChange = () => {
    cancel();
    if (source.visibilityState === 'hidden') {
      timer = setTimeout(() => {
        timer = null;
        tail.suspend();
      }, delayMs);
      return;
    }

    tail.resume();
  };

  source.addEventListener('visibilitychange', onChange);
  if (source.visibilityState === 'hidden') {
    onChange();
  }

  return () => {
    cancel();
    source.removeEventListener('visibilitychange', onChange);
  };
}
