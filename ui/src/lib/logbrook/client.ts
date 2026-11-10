import type {
  Facets,
  Histogram,
  HistogramParams,
  IndexInfo,
  IndexList,
  LogFilters,
  SearchParams,
  SearchResult,
  TimeRange,
} from './types.ts';

import { errorFromResponse } from './errors.ts';
import { appendFilters, appendRange, searchQueryString } from './search-params.ts';

/**
 * Where the browser reaches the Logbrook proxy of the BFF.
 */
export const PROXY_BASE_URL = '/api/lb';

/**
 * Configuration of a Logbrook client.
 *
 * `baseUrl` is the proxy path in the browser, or the Logbrook server's own URL when the client runs
 * in the BFF. `authorize` adds credentials to every request, so a direct client can set a bearer
 * token while the browser relies on the proxy. `fetch` replaces the global one in tests.
 */
export type LogbrookClientOptions = {
  /**
   * Base URL of the Logbrook API or of the proxy, with or without a trailing slash.
   */
  readonly baseUrl: string;

  /**
   * Adds credentials to the headers of every request. Omit it when the proxy in front adds the
   * credentials.
   */
  readonly authorize?: (headers: Headers) => void;

  /**
   * Replacement for the global `fetch`, for tests.
   */
  readonly fetch?: typeof fetch;
};

/**
 * The Logbrook endpoints the UI reads. Every method takes an `AbortSignal`, so leaving a view
 * cancels the request, and the proxy passes that cancellation on to DuckDB.
 */
export type LogbrookClient = {
  /**
   * Lists the indexes the credential can reach.
   */
  readonly listIndexes: (signal?: AbortSignal) => Promise<ReadonlyArray<IndexInfo>>;

  /**
   * Fetches one index by name.
   */
  readonly getIndex: (index: string, signal?: AbortSignal) => Promise<IndexInfo>;

  /**
   * Fetches one page of a search. Pass `null` as the cursor for the first page and a page's
   * `next_cursor` for the next.
   */
  readonly searchLogs: (
    index: string,
    params: SearchParams,
    cursor: string | null,
    signal?: AbortSignal,
  ) => Promise<SearchResult>;

  /**
   * Fetches the facets of the matching events.
   */
  readonly getFacets: (
    index: string,
    range: TimeRange,
    filters: LogFilters,
    signal?: AbortSignal,
  ) => Promise<Facets>;

  /**
   * Fetches the histogram buckets of the matching events.
   */
  readonly getHistogram: (
    index: string,
    params: HistogramParams,
    signal?: AbortSignal,
  ) => Promise<Histogram>;

  /**
   * Fetches the Prometheus text of `/metrics`. Needs an unrestricted read token; any other read
   * token gets a `403` `LogbrookError`.
   */
  readonly getMetrics: (signal?: AbortSignal) => Promise<string>;

  /**
   * Builds the URL an `EventSource` opens for the live tail. It carries the filters only; time
   * bounds and paging do not apply to a tail.
   */
  readonly tailUrl: (index: string, filters: LogFilters) => string;
};

/**
 * Creates a client for a Logbrook server or for the BFF proxy in front of it.
 *
 * Every failed response becomes a `LogbrookError` carrying the status and `Retry-After`. Response
 * bodies are not validated at runtime: the server is ours, and decoding thousands of events per
 * page would cost more than it protects.
 */
export function createLogbrookClient(options: LogbrookClientOptions): LogbrookClient {
  const baseUrl = options.baseUrl.replace(/\/+$/, '');
  const fetchImpl = options.fetch ?? fetch;

  /**
   * Sends an authenticated GET and decodes the JSON body. Throws a `LogbrookError` for any non-2xx
   * status.
   */
  async function getJson<T>(path: string, signal: AbortSignal | undefined): Promise<T> {
    const headers = new Headers({ accept: 'application/json' });
    options.authorize?.(headers);
    const init: RequestInit = signal === undefined ? { headers } : { headers, signal };
    const response = await fetchImpl(`${baseUrl}${path}`, init);
    if (!response.ok) {
      throw await errorFromResponse(response);
    }

    return (await response.json()) as T;
  }

  /**
   * Sends an authenticated GET and returns the body as text. Throws a `LogbrookError` for any
   * non-2xx status.
   */
  async function getText(path: string, signal: AbortSignal | undefined): Promise<string> {
    const headers = new Headers({ accept: 'text/plain' });
    options.authorize?.(headers);
    const init: RequestInit = signal === undefined ? { headers } : { headers, signal };
    const response = await fetchImpl(`${baseUrl}${path}`, init);
    if (!response.ok) {
      throw await errorFromResponse(response);
    }

    return response.text();
  }

  return {
    listIndexes: async (signal) => {
      const list = await getJson<IndexList>('/indexes', signal);
      return list.indexes;
    },
    getIndex: (index, signal) => getJson<IndexInfo>(indexPath(index), signal),
    searchLogs: (index, params, cursor, signal) => {
      const query = searchQueryString(params, cursor);
      return getJson<SearchResult>(`${indexPath(index)}/logs?${query}`, signal);
    },
    getFacets: (index, range, filters, signal) => {
      const query = rangeQuery(range, filters);
      return getJson<Facets>(`${indexPath(index)}/logs/facets?${query}`, signal);
    },
    getHistogram: (index, params, signal) => {
      const query = histogramQuery(params);
      return getJson<Histogram>(`${indexPath(index)}/logs/histogram?${query}`, signal);
    },
    getMetrics: (signal) => getText('/metrics', signal),
    tailUrl: (index, filters) => {
      const params = new URLSearchParams();
      appendFilters(params, filters);
      return `${baseUrl}${indexPath(index)}/logs/tail?${params.toString()}`;
    },
  };
}

/**
 * The client the browser uses: it talks to the proxy and authenticates with the server's token (the
 * proxy adds it).
 */
export const browserClient: LogbrookClient = createLogbrookClient({ baseUrl: PROXY_BASE_URL });

/**
 * Returns `/indexes/{index}` with the name URL-encoded.
 */
function indexPath(index: string): string {
  return `/indexes/${encodeURIComponent(index)}`;
}

/**
 * Builds the query string of a request that takes a range and filters only.
 */
function rangeQuery(range: TimeRange, filters: LogFilters): string {
  const query = new URLSearchParams();
  appendRange(query, range);
  appendFilters(query, filters);
  return query.toString();
}

/**
 * Builds the query string of a histogram request.
 */
function histogramQuery(params: HistogramParams): string {
  const query = new URLSearchParams(rangeQuery(params, params));
  if (params.intervalMs !== undefined) {
    query.set('interval_ms', String(params.intervalMs));
  }

  return query.toString();
}
