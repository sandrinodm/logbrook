import type { IndexInfo } from './types.ts';

import { LogbrookError } from './errors.ts';

/**
 * What the server's configured tokens can do, as far as the UI can know without trying every
 * endpoint.
 */
export type AccessCapabilities = {
  /**
   * Whether the server has an admin token configured, which allows creating and deleting indexes.
   */
  readonly canManageIndexes: boolean;

  /**
   * How much the read token learns from `GET /indexes`: `full` with sizes and policy, `names-only`
   * for a source-restricted token, or `unknown` while there are no indexes to judge by.
   */
  readonly indexDetail: 'full' | 'names-only' | 'unknown';
};

/**
 * Whether `/metrics` is usable by the configured read token.
 */
export type MetricsAccess = 'allowed' | 'forbidden' | 'unknown';

/**
 * Whether an index entry carries sizes and policy. A source-restricted read token receives
 * `{"name"}` only.
 */
export function hasIndexDetail(index: IndexInfo): boolean {
  return index.size_bytes !== undefined;
}

/**
 * Works out what the server's tokens can do from the access info and the index list the read token
 * got.
 *
 * The read token's source restriction is invisible to the UI, but it shows in the list: a token
 * restricted to sources gets names only, one without gets sizes. Index restrictions only shorten
 * the list.
 */
export function deriveCapabilities(
  access: { readonly canManageIndexes: boolean },
  readableIndexes: ReadonlyArray<IndexInfo>,
): AccessCapabilities {
  return {
    canManageIndexes: access.canManageIndexes,
    indexDetail: indexDetailOf(readableIndexes),
  };
}

/**
 * Reads the outcome of the latest `/metrics` request. A `403` means the read token is restricted to
 * sources or indexes, so metrics cannot work until the operator configures another read token.
 * Success with data is `allowed`; anything else, such as a request still running or a network
 * failure, is `unknown`.
 */
export function metricsAccessOf(error: unknown, hasData: boolean): MetricsAccess {
  if (error instanceof LogbrookError && error.status === 403) {
    return 'forbidden';
  }

  return hasData && error === null ? 'allowed' : 'unknown';
}

/**
 * Judges the detail level from the first entry; the server treats all entries the same way.
 */
function indexDetailOf(indexes: ReadonlyArray<IndexInfo>): AccessCapabilities['indexDetail'] {
  const first = indexes[0];
  if (first === undefined) {
    return 'unknown';
  }

  return hasIndexDetail(first) ? 'full' : 'names-only';
}
