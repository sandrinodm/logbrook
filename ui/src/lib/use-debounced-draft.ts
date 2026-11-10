import { useEffect, useEffectEvent, useRef, useState } from 'react';

/**
 * How long a text field waits after the last keystroke before it changes the URL, in milliseconds.
 */
export const DRAFT_DEBOUNCE_MS = 250;

/**
 * How long a commit may wait for its echo in the URL before it is forgotten, in milliseconds. A
 * navigation normally lands within a few frames; one that never lands (it was superseded, or it
 * failed) must not keep its entry, or a later outside change to that exact value would be taken for
 * the echo of a commit that never happened, and the box would write its old text over it.
 */
export const PENDING_ECHO_TTL_MS = 5_000;

/**
 * A URL value this box has written and whose echo it is waiting for.
 */
export type PendingEcho = {
  /**
   * The URL value the commit wrote.
   */
  readonly value: string;

  /**
   * Identifies the commit, so the timer that expires it never removes a later commit of the same
   * value.
   */
  readonly id: number;
};

/**
 * What a debounced text box tracks between keystrokes and navigations.
 */
export type DraftState = {
  /**
   * The text in the box.
   */
  readonly draft: string;

  /**
   * The last URL value this state has taken into account.
   */
  readonly seen: string;

  /**
   * Commits whose echo in the URL this box has not seen yet, oldest first. When the URL reaches one
   * of their values the change came from this box, so the draft already holds (or has moved past)
   * that text and must not be overwritten. An entry that is never echoed is dropped by `expired`.
   */
  readonly pending: ReadonlyArray<PendingEcho>;
};

/**
 * The state of a box that starts out showing `value`.
 */
export function initialDraft(value: string): DraftState {
  return { draft: value, seen: value, pending: [] };
}

/**
 * The state after the user edited the box.
 */
export function typed(state: DraftState, draft: string): DraftState {
  return { ...state, draft };
}

/**
 * The state after the box wrote `urlValue` (the URL value its text produces) to the URL. `id` names
 * the commit for `expired`; it must differ from the ids of the commits still pending.
 */
export function committed(state: DraftState, urlValue: string, id: number): DraftState {
  return { ...state, pending: [...state.pending, { value: urlValue, id }] };
}

/**
 * The state after commit `id` waited `PENDING_ECHO_TTL_MS` for its echo: the entry is forgotten
 * when it is still pending, and the state is returned unchanged when the echo came or an outside
 * change cleared it already.
 */
export function expired(state: DraftState, id: number): DraftState {
  const pending = state.pending.filter((entry) => entry.id !== id);
  return pending.length === state.pending.length ? state : { ...state, pending };
}

/**
 * The state after the URL value became `value`.
 *
 * If this box wrote that value itself, it was only the echo of an earlier commit: the draft stays
 * as typed (it may be ahead of the URL, or differ in spelling such as a trailing comma), and the
 * echoes up to and including this one are forgotten. Any other value is an outside change (back or
 * forward, clear filters, a facet click) and replaces the draft. A commit that waited longer than
 * `PENDING_ECHO_TTL_MS` is no longer pending (see `expired`), so it cannot be mistaken for the
 * origin of an outside change to the same value.
 */
export function observed(state: DraftState, value: string): DraftState {
  if (value === state.seen) {
    return state;
  }

  const echo = state.pending.findIndex((entry) => entry.value === value);
  if (echo >= 0) {
    return { ...state, seen: value, pending: state.pending.slice(echo + 1) };
  }

  return { draft: value, seen: value, pending: [] };
}

/**
 * Options of `useDebouncedDraft`.
 */
type DebouncedDraftOptions = {
  /**
   * The value currently in the URL.
   */
  readonly value: string;

  /**
   * Writes the draft to the URL. Called `DRAFT_DEBOUNCE_MS` after the last keystroke, and only when
   * the draft would change the URL value.
   */
  readonly onCommit: (draft: string) => void;

  /**
   * The URL value a draft produces, when that differs from the draft itself (a list typed as `api,`
   * is stored as `api`). Defaults to the draft. Must not change between renders.
   */
  readonly toValue?: ((draft: string) => string) | undefined;
};

/**
 * Local text for a box whose value lives in the URL: edits are kept in the draft and written to the
 * URL after the user pauses, and the draft is replaced only when the URL changes for a reason other
 * than this box's own commits. Returns the draft and a setter.
 */
export function useDebouncedDraft(
  options: DebouncedDraftOptions,
): readonly [string, (draft: string) => void] {
  const { value, onCommit, toValue = identity } = options;
  const [state, setState] = useState(() => initialDraft(value));
  const commits = useRef(0);
  if (state.seen !== value) {
    setState(observed(state, value));
  }

  const target = toValue(state.draft);

  // The callback changes on every render of the parent; an effect event keeps the timer from
  // restarting each time.
  const commit = useEffectEvent((text: string) => {
    commits.current += 1;
    const id = commits.current;
    setState((current) => committed(current, toValue(text), id));
    // If the URL never reaches this value, stop waiting for it.
    setTimeout(() => setState((current) => expired(current, id)), PENDING_ECHO_TTL_MS);
    onCommit(text);
  });

  useEffect(() => {
    if (target === value) {
      return undefined;
    }

    const timer = setTimeout(() => commit(state.draft), DRAFT_DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [state.draft, target, value]);

  return [state.draft, (draft) => setState((current) => typed(current, draft))];
}

/**
 * Returns its argument.
 */
function identity(text: string): string {
  return text;
}
