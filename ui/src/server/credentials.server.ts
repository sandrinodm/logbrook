import type { IndexInfo } from '../lib/logbrook/types.ts';

import { createLogbrookClient } from '../lib/logbrook/client.ts';
import { errorFromResponse, LogbrookError } from '../lib/logbrook/errors.ts';
import { retryDelayMs, shouldRetry } from '../lib/logbrook/queries.ts';

/**
 * What a token turned out to be when tried against Logbrook.
 */
export type TokenRole = 'read' | 'admin' | 'invalid';

/**
 * The Logbrook server the BFF talks to, and the `fetch` to reach it with (replaced in tests).
 */
export type UpstreamDeps = {
  /**
   * Base URL of the Logbrook server, without a trailing slash.
   */
  readonly upstreamUrl: string;

  /**
   * The `fetch` used to reach the server.
   */
  readonly fetch: typeof fetch;

  /**
   * Waits for the given number of milliseconds before a repeated request. Defaults to a timer;
   * tests replace it.
   */
  readonly sleep?: (millis: number) => Promise<void>;
};

/**
 * The tokens the BFF is configured with.
 */
export type ConfiguredTokens = {
  /**
   * The configured read token.
   */
  readonly readToken: string;

  /**
   * The configured admin token, or `null` when none is set.
   */
  readonly adminToken: string | null;
};

/**
 * Index that exists on every server, used to tell the roles apart.
 */
const PROBE_INDEX = 'default';

/**
 * Finds out whether `token` is a read token, an admin token or neither.
 *
 * Both roles may list indexes, so that call only proves the token is valid. Telling them apart
 * relies on the roles not overlapping: a count request with no time range is rejected with `400`
 * for a read token but with `401` for an admin token, because authentication runs before parameter
 * validation. No data is read or changed.
 *
 * Logbrook serves `GET /indexes` and `/metrics` from one permit and answers `429` while it is
 * taken, so a `429` or `503` is repeated with the app's retry policy before it counts as an answer.
 * Throws a `LogbrookError` for a response that still says nothing about the token after the last
 * attempt, and lets network errors propagate.
 */
export async function classifyToken(token: string, deps: UpstreamDeps): Promise<TokenRole> {
  const [list, probe] = await Promise.all([
    requestWithRetry(deps, '/indexes', token),
    requestWithRetry(deps, `/indexes/${PROBE_INDEX}/logs/count`, token),
  ]);

  if (list.status === 401) {
    await discardBody(list);
    await discardBody(probe);
    return 'invalid';
  }

  if (!list.ok) {
    await discardBody(probe);
    throw await errorFromResponse(list);
  }

  await discardBody(list);
  await discardBody(probe);
  return probe.status === 401 ? 'admin' : 'read';
}

/**
 * Checks the configured tokens against Logbrook once, for the boot log. Returns one warning per
 * problem, in words an operator can act on, and an empty list when everything is as configured.
 *
 * It never throws and never includes a token in a warning: Logbrook may simply start later than the
 * UI, so a failure is information, not a reason to stop. The tokens are tried one after the other,
 * because Logbrook answers `429` while another request holds the permit of `GET /indexes`.
 */
export async function checkConfiguredTokens(
  tokens: ConfiguredTokens,
  deps: UpstreamDeps,
): Promise<ReadonlyArray<string>> {
  const warnings: string[] = [];
  try {
    const readRole = await classifyToken(tokens.readToken, deps);
    warnings.push(...readTokenWarnings(readRole));
    if (tokens.adminToken !== null) {
      const adminRole = await classifyToken(tokens.adminToken, deps);
      warnings.push(...adminTokenWarnings(adminRole));
    }
  } catch (error) {
    warnings.push(describeFailure(error, deps.upstreamUrl));
  }

  return warnings;
}

/**
 * Lists the indexes `token` can reach with `GET /indexes`, as the server sends them: full entries
 * for an admin token or an unrestricted read token, names only for a source-restricted one. Throws
 * a `LogbrookError` for a non-2xx answer.
 */
export function listIndexes(token: string, deps: UpstreamDeps): Promise<ReadonlyArray<IndexInfo>> {
  const client = createLogbrookClient({
    baseUrl: deps.upstreamUrl,
    fetch: deps.fetch,
    authorize: (headers) => headers.set('authorization', `Bearer ${token}`),
  });
  return client.listIndexes();
}

/**
 * Creates an index with the admin token. `PUT` is idempotent, so an existing index is not an error.
 */
export async function createIndex(
  name: string,
  adminToken: string,
  deps: UpstreamDeps,
): Promise<void> {
  const response = await request(deps, `/indexes/${encodeURIComponent(name)}`, adminToken, 'PUT');
  await ensureOk(response);
}

/**
 * Deletes an index with the admin token. The server refuses `default` and indexes declared in its
 * configuration with a `400`, which surfaces as a `LogbrookError`.
 */
export async function deleteIndex(
  name: string,
  adminToken: string,
  deps: UpstreamDeps,
): Promise<void> {
  const response = await request(
    deps,
    `/indexes/${encodeURIComponent(name)}`,
    adminToken,
    'DELETE',
  );
  await ensureOk(response);
}

/**
 * What the read token turned out to be, as warnings: none when it is a read token.
 */
function readTokenWarnings(role: TokenRole): ReadonlyArray<string> {
  if (role === 'invalid') {
    return [
      'Logbrook rejected LOGBROOK_READ_TOKEN. Every page will show an error until it is fixed.',
    ];
  }

  if (role === 'admin') {
    return [
      'LOGBROOK_READ_TOKEN is an admin token. Logbrook admin tokens cannot read events; configure a read token.',
    ];
  }

  return [];
}

/**
 * What the admin token turned out to be, as warnings: none when it is an admin token.
 */
function adminTokenWarnings(role: TokenRole): ReadonlyArray<string> {
  if (role === 'invalid') {
    return ['Logbrook rejected LOGBROOK_ADMIN_TOKEN. Creating and deleting indexes will fail.'];
  }

  if (role === 'read') {
    return ['LOGBROOK_ADMIN_TOKEN is a read token. Creating and deleting indexes will fail.'];
  }

  return [];
}

/**
 * Sends one authenticated request to Logbrook.
 */
function request(
  deps: UpstreamDeps,
  path: string,
  token: string,
  method = 'GET',
): Promise<Response> {
  return deps.fetch(`${deps.upstreamUrl}${path}`, {
    method,
    headers: { authorization: `Bearer ${token}`, accept: 'application/json' },
    redirect: 'manual',
  });
}

/**
 * Sends a `GET` like `request`, repeating it while Logbrook answers `429` or `503` and the retry
 * policy allows another attempt, waiting as the policy says (`Retry-After` when sent). Returns the
 * first other response. When the attempts run out it throws the last answer's `LogbrookError`.
 */
async function requestWithRetry(
  deps: UpstreamDeps,
  path: string,
  token: string,
): Promise<Response> {
  const sleep = deps.sleep ?? defaultSleep;
  for (let attempt = 0; ; attempt += 1) {
    const response = await request(deps, path, token);
    if (response.status !== 429 && response.status !== 503) {
      return response;
    }

    const error = await errorFromResponse(response);
    if (!shouldRetry(attempt, error)) {
      throw error;
    }

    await sleep(retryDelayMs(attempt, error));
  }
}

/**
 * Resolves after `millis` milliseconds.
 */
function defaultSleep(millis: number): Promise<void> {
  return new Promise((done) => setTimeout(done, millis));
}

/**
 * Releases a response whose body nobody will read, so the connection is not held open. A body that
 * was already read is left alone.
 */
async function discardBody(response: Response): Promise<void> {
  if (!response.bodyUsed) {
    await response.body?.cancel();
  }
}

/**
 * Throws the response's `LogbrookError` unless it succeeded; a success body is discarded.
 */
async function ensureOk(response: Response): Promise<void> {
  if (!response.ok) {
    throw await errorFromResponse(response);
  }

  await response.body?.cancel();
}

/**
 * Words for a failed boot check: the server's own message for an HTTP error, or the address that
 * could not be reached for a network failure. Only the origin is named, in case the URL carries
 * credentials.
 */
function describeFailure(error: unknown, upstreamUrl: string): string {
  if (error instanceof LogbrookError) {
    return `Logbrook answered ${error.status} when the tokens were checked: ${error.message}`;
  }

  return `Cannot reach Logbrook at ${new URL(upstreamUrl).origin}. Check LOGBROOK_URL; the UI works once Logbrook is up.`;
}
