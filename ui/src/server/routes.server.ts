/**
 * A request path the proxy may forward: the path on the Logbrook server. The proxy always sends the
 * configured read token. The admin token never leaves the server, so a browser cannot use it to
 * read more than the read token allows; index management goes through server functions instead.
 */
export type AllowedRoute = {
  /**
   * Path on the Logbrook server, starting with a slash.
   */
  readonly path: string;
};

/**
 * Index names as Logbrook accepts them: 1 to 63 lowercase letters, digits, `-` or `_`, starting
 * with a letter or digit.
 */
const INDEX_NAME = '[a-z0-9][a-z0-9_-]{0,62}';

/**
 * Per-index endpoints that read events. `/logs/count` is deliberately absent: the explorer sums
 * histogram buckets instead, to stay inside the browser's six connections per origin. Ingestion is
 * never reachable.
 */
const LOG_ENDPOINTS = new Set(['logs', 'logs/facets', 'logs/histogram', 'logs/tail']);

/**
 * Matches `indexes/{index}` and `indexes/{index}/{endpoint}`.
 */
const INDEX_PATH = new RegExp(`^indexes/(${INDEX_NAME})(?:/(.+))?$`);

/**
 * Matches a path (the part after `/api/lb/`) against the proxy allowlist.
 *
 * Returns the upstream path, with a leading slash, or `null` for anything else. The match is exact:
 * no trailing slash, no dot segments, no encoded separators, so a client cannot reach another
 * Logbrook route by path tricks. Only `GET` is ever proxied; the caller checks the method.
 */
export function matchAllowedRoute(splat: string): AllowedRoute | null {
  if (splat === 'metrics') {
    return { path: '/metrics' };
  }

  if (splat === 'indexes') {
    return { path: '/indexes' };
  }

  const match = INDEX_PATH.exec(splat);
  if (match === null) {
    return null;
  }

  const [, index, endpoint] = match;
  if (endpoint === undefined) {
    return { path: `/indexes/${index}` };
  }

  if (!LOG_ENDPOINTS.has(endpoint)) {
    return null;
  }

  return { path: `/indexes/${index}/${endpoint}` };
}
