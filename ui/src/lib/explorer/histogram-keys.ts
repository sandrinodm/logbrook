/**
 * Where the keyboard is in the histogram: the bucket under the cursor, and the bucket where a
 * selection was started with Shift, if any. Both are bucket numbers counted from the left.
 */
export type HistogramKeyState = {
  /**
   * Bucket the cursor is on.
   */
  readonly cursor: number;

  /**
   * Bucket where Shift started the selection, or `null` when nothing is selected.
   */
  readonly anchor: number | null;
};

/**
 * What a key press asks of the histogram.
 */
export type HistogramKeyAction =
  | { readonly kind: 'state'; readonly state: HistogramKeyState }
  | { readonly kind: 'zoom'; readonly first: number; readonly last: number }
  | { readonly kind: 'clear' }
  | { readonly kind: 'ignore' };

/**
 * The buckets the keyboard has selected, as an inclusive pair. A cursor without an anchor selects
 * its own bucket.
 */
export function selectedBuckets(state: HistogramKeyState): { first: number; last: number } {
  const anchor = state.anchor ?? state.cursor;
  return { first: Math.min(anchor, state.cursor), last: Math.max(anchor, state.cursor) };
}

/**
 * Whether the keyboard has a selection of its own, started with Shift, as opposed to a bare cursor.
 */
export function hasKeySelection(state: HistogramKeyState | null): boolean {
  return state !== null && state.anchor !== null;
}

/**
 * Applies one key press to the keyboard state of a histogram of `slots` buckets.
 *
 * - Left and Right move the cursor one bucket and drop the selection. The first press, with no cursor
 *   yet, puts the cursor on the newest bucket (the right edge), which is where the table starts
 *   too.
 * - Home and End jump to the first and the last bucket.
 * - With Shift, those keys keep or start the selection at the bucket the cursor was on, so it grows
 *   from there.
 * - Enter zooms to the selection, or to the cursor's bucket when nothing is selected.
 * - Escape clears a selection or a bare cursor, and is ignored when there is neither, so the page's
 *   own Escape handling still runs.
 *
 * Every other key is ignored. Returns `ignore` for a chart with no buckets.
 */
export function applyHistogramKey(
  state: HistogramKeyState | null,
  key: string,
  shift: boolean,
  slots: number,
): HistogramKeyAction {
  if (slots <= 0) {
    return { kind: 'ignore' };
  }

  const last = slots - 1;
  const cursor = state === null ? last : Math.min(state.cursor, last);
  if (key === 'Enter') {
    return state === null ? { kind: 'ignore' } : { kind: 'zoom', ...selectedBuckets(state) };
  }

  if (key === 'Escape') {
    return state === null ? { kind: 'ignore' } : { kind: 'clear' };
  }

  const target = targetBucket(key, state === null ? null : cursor, last);
  if (target === null) {
    return { kind: 'ignore' };
  }

  const anchor = shift ? (state?.anchor ?? cursor) : null;
  return { kind: 'state', state: { cursor: target, anchor } };
}

/**
 * The bucket a movement key leads to, or `null` for a key that does not move the cursor. With no
 * cursor yet (`from` is `null`) the first press lands on the newest bucket.
 */
function targetBucket(key: string, from: number | null, last: number): number | null {
  if (key === 'Home') {
    return 0;
  }

  if (key === 'End') {
    return last;
  }

  if (key !== 'ArrowLeft' && key !== 'ArrowRight') {
    return null;
  }

  if (from === null) {
    return last;
  }

  const step = key === 'ArrowLeft' ? -1 : 1;
  return Math.min(last, Math.max(0, from + step));
}
