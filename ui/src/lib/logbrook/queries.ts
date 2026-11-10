import { infiniteQueryOptions, keepPreviousData, queryOptions } from '@tanstack/react-query';

import type { LogbrookClient } from './client.ts';
import type {
  HistogramParams,
  IndexInfo,
  LogFilters,
  SearchParams,
  SearchResult,
  TimeRange,
} from './types.ts';

import { browserClient } from './client.ts';
import { LogbrookError, isRetryable } from './errors.ts';

/**
 * Default page size of a log search. The server allows up to 1,000; 500 keeps a page well under its
 * 8 MiB response limit for typical events.
 */
export const LOG_PAGE_SIZE = 500;

/**
 * Most events the explorer keeps loaded. Cursors only lead to older events, so pages cannot be
 * dropped and refetched; past this the view asks the user to narrow the time range.
 */
export const MAX_LOADED_EVENTS = 50_000;

/**
 * How many times a rate-limited or briefly unavailable request is repeated after the first attempt.
 */
const MAX_RETRIES = 3;

/**
 * Longest wait between two attempts, in milliseconds, when the server gives no `Retry-After`.
 */
const MAX_BACKOFF_MS = 8_000;

/**
 * Longest wait honoured from a `Retry-After` header, in milliseconds. A misconfigured or hostile
 * response must not park a request for minutes.
 */
const MAX_RETRY_AFTER_MS = 30_000;

/**
 * First wait of the exponential backoff, in milliseconds.
 */
const BASE_BACKOFF_MS = 1_000;

/**
 * How long the index list counts as fresh, in milliseconds. Sizes change as data arrives, but the
 * list is only a navigation aid.
 */
const INDEXES_STALE_MS = 30_000;

/**
 * Whether Query should repeat a failed request: only for `429` and `503`, and at most `MAX_RETRIES`
 * times. A `400` is never repeated, because the same request would fail again.
 */
export function shouldRetry(failureCount: number, error: unknown): boolean {
  return failureCount < MAX_RETRIES && isRetryable(error);
}

/**
 * Returns the wait before the next attempt in milliseconds: the server's `Retry-After` when it sent
 * one (at most `MAX_RETRY_AFTER_MS`), otherwise exponential backoff from `BASE_BACKOFF_MS`, capped
 * at `MAX_BACKOFF_MS`.
 *
 * `attemptIndex` is zero for the first retry.
 */
export function retryDelayMs(attemptIndex: number, error: unknown): number {
  if (error instanceof LogbrookError && error.retryAfterSeconds !== null) {
    return Math.min(error.retryAfterSeconds * 1000, MAX_RETRY_AFTER_MS);
  }

  return Math.min(BASE_BACKOFF_MS * 2 ** attemptIndex, MAX_BACKOFF_MS);
}

/**
 * Query keys. Each key holds the exact parameters of its request, so two components asking for the
 * same data share one cache entry. The parameters must be plain JSON values: numbers and strings,
 * never `Date`.
 */
export const logbrookKeys = {
  /**
   * Key of the index list the read token can reach. Every other index key starts with it, so
   * invalidating it refreshes them all.
   */
  indexes: () => ['logbrook', 'indexes'] as const,

  /**
   * Key of the index list for the management view, fetched with the admin token when the server has
   * one.
   */
  managedIndexes: () => ['logbrook', 'indexes', 'managed'] as const,

  /**
   * Key of the parsed `/metrics` scrape.
   */
  metrics: () => ['logbrook', 'metrics'] as const,

  /**
   * Key of a log search; the params include the frozen time range and filters.
   */
  logs: (index: string, params: SearchParams) => ['logbrook', 'logs', index, params] as const,

  /**
   * Key of the facets of a range and filters.
   */
  facets: (index: string, range: TimeRange, filters: LogFilters) =>
    ['logbrook', 'facets', index, range, filters] as const,

  /**
   * Key of a histogram request.
   */
  histogram: (index: string, params: HistogramParams) =>
    ['logbrook', 'histogram', index, params] as const,
};

/**
 * Query options for the list of indexes the server's read token can see.
 */
export function indexesQuery(client: LogbrookClient = browserClient) {
  return queryOptions({
    queryKey: logbrookKeys.indexes(),
    queryFn: ({ signal }) => client.listIndexes(signal),
    staleTime: INDEXES_STALE_MS,
  });
}

/**
 * Query options for the management view's index list. `fetchIndexes` is how the list is obtained: a
 * server function, because the admin token never reaches the browser.
 */
export function managedIndexesQuery(fetchIndexes: () => Promise<ReadonlyArray<IndexInfo>>) {
  return queryOptions({
    queryKey: logbrookKeys.managedIndexes(),
    queryFn: fetchIndexes,
    staleTime: INDEXES_STALE_MS,
  });
}

/**
 * Infinite query options for the pages of a log search.
 *
 * `params` must hold a frozen time range: a cursor is only valid for the exact `from`, `to`,
 * filters and `limit` it came from, so a refresh is a new key with a new "now". The query never
 * refetches itself, because a stale infinite query would refetch every loaded page one after the
 * other with cursors that may have expired. Events never change once stored, so structural sharing,
 * which deep-compares every loaded event on each new page, is switched off. Loading stops at
 * `MAX_LOADED_EVENTS`.
 */
export function logsInfiniteQuery(
  index: string,
  params: SearchParams,
  client: LogbrookClient = browserClient,
) {
  const searchParams = { limit: LOG_PAGE_SIZE, ...params };
  return infiniteQueryOptions({
    queryKey: logbrookKeys.logs(index, searchParams),
    queryFn: ({ pageParam, signal }) => client.searchLogs(index, searchParams, pageParam, signal),
    initialPageParam: null as string | null,
    getNextPageParam: nextCursor,
    staleTime: Infinity,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    structuralSharing: false,
    placeholderData: keepPreviousData,
  });
}

/**
 * Returns the cursor of the next page, or `undefined` to stop: on the last page, or once
 * `MAX_LOADED_EVENTS` events are loaded.
 */
export function nextCursor(
  lastPage: SearchResult,
  allPages: ReadonlyArray<SearchResult>,
): string | undefined {
  if (lastPage.next_cursor === null || countEvents(allPages) >= MAX_LOADED_EVENTS) {
    return undefined;
  }

  return lastPage.next_cursor;
}

/**
 * Query options for the facets of a time range and filters.
 */
export function facetsQuery(
  index: string,
  range: TimeRange,
  filters: LogFilters,
  client: LogbrookClient = browserClient,
) {
  return queryOptions({
    queryKey: logbrookKeys.facets(index, range, filters),
    queryFn: ({ signal }) => client.getFacets(index, range, filters, signal),
    staleTime: Infinity,
    refetchOnWindowFocus: false,
    placeholderData: keepPreviousData,
  });
}

/**
 * Query options for a histogram. The explorer also asks for one with `minLevel: 50` to draw errors
 * on top of the total, and sums the buckets of the unfiltered one for the event count instead of
 * calling `/logs/count`.
 */
export function histogramQuery(
  index: string,
  params: HistogramParams,
  client: LogbrookClient = browserClient,
) {
  return queryOptions({
    queryKey: logbrookKeys.histogram(index, params),
    queryFn: ({ signal }) => client.getHistogram(index, params, signal),
    staleTime: Infinity,
    refetchOnWindowFocus: false,
    placeholderData: keepPreviousData,
  });
}

/**
 * Counts the events in a list of pages.
 */
function countEvents(pages: ReadonlyArray<SearchResult>): number {
  let total = 0;
  for (const page of pages) {
    total += page.events.length;
  }

  return total;
}
