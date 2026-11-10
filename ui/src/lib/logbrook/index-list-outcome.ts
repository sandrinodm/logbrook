import type { IndexInfo } from './types.ts';

import { LogbrookError } from './errors.ts';

/**
 * Why an index server function failed, as a plain value. A thrown `LogbrookError` loses its class,
 * its status and its `Retry-After` on the way from a server function to the browser, so the retry
 * policy could never recognise a `429`; a returned object keeps them.
 */
export type IndexFailure = {
  /**
   * Always `false`; tells a failure from a success.
   */
  readonly ok: false;

  /**
   * HTTP status of the failed request, `502` when Logbrook could not be reached, or `501` when
   * index management needs an admin token the server does not have.
   */
  readonly status: number;

  /**
   * Explanation for people.
   */
  readonly message: string;

  /**
   * Seconds Logbrook asked to wait before trying again, or `null`.
   */
  readonly retryAfterSeconds: number | null;
};

/**
 * What the index-list server functions return: the indexes, or the failure.
 */
export type IndexListOutcome =
  | { readonly ok: true; readonly indexes: ReadonlyArray<IndexInfo> }
  | IndexFailure;

/**
 * Builds the failure to return for an error caught on the server: Logbrook's own status, message
 * and `Retry-After` for a `LogbrookError`, else `502` for a network failure.
 */
export function failureOf(error: unknown): IndexFailure {
  if (error instanceof LogbrookError) {
    return {
      ok: false,
      status: error.status,
      message: error.message,
      retryAfterSeconds: error.retryAfterSeconds,
    };
  }

  return { ok: false, status: 502, message: 'Cannot reach Logbrook.', retryAfterSeconds: null };
}

/**
 * Returns the indexes of a successful outcome, or throws the `LogbrookError` it describes, so that
 * Query's retry policy sees the status and `Retry-After` in the browser as it would for a request
 * made there.
 */
export function unwrapIndexList(outcome: IndexListOutcome): ReadonlyArray<IndexInfo> {
  if (outcome.ok) {
    return outcome.indexes;
  }

  throw new LogbrookError(outcome.status, outcome.message, outcome.retryAfterSeconds);
}

/**
 * Awaits the outcome of a server function call and turns a rejection into a failure. A server
 * function rejects when the request to the app itself fails (offline, a dropped connection, a
 * server error), and a dialog that does not catch that shows nothing at all.
 */
export async function settleOutcome<T extends { readonly ok: boolean }>(
  pending: Promise<T>,
): Promise<T | IndexFailure> {
  try {
    return await pending;
  } catch {
    return {
      ok: false,
      status: 502,
      message: 'Could not reach the server. Check your connection and try again.',
      retryAfterSeconds: null,
    };
  }
}
