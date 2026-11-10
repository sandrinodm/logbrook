import { QueryClient } from '@tanstack/react-query';

import { retryDelayMs, shouldRetry } from './logbrook/queries.ts';

/**
 * How long a query counts as fresh unless it says otherwise, in milliseconds. Fresh data is not
 * refetched when the browser hydrates server-rendered queries.
 */
const DEFAULT_STALE_MS = 5_000;

/**
 * Creates a `QueryClient` with the app's retry policy: only `429` and `503` are repeated, waiting
 * for `Retry-After` when the server sends it, and a `400` never is.
 *
 * The router calls this once per server request and once in the browser, so one visitor's cached
 * data never reaches another.
 */
export function createQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: DEFAULT_STALE_MS,
        retry: shouldRetry,
        retryDelay: retryDelayMs,
      },
    },
  });
}
