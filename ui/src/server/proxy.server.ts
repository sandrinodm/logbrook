import { RESUME_PARAM, isResumeId } from '../lib/logbrook/resume.ts';
import { matchAllowedRoute } from './routes.server.ts';

/**
 * Path prefix of the proxy route. Requests below it are forwarded to Logbrook.
 */
export const PROXY_PREFIX = '/api/lb/';

/**
 * Smallest JSON body worth compressing, in bytes. Below it the gzip header and CPU cost outweigh
 * the saving.
 */
const MIN_COMPRESSIBLE_BYTES = 1024;

/**
 * Upstream response headers copied to the browser. Everything else, including `Set-Cookie`, is
 * dropped.
 */
const FORWARDED_RESPONSE_HEADERS = ['content-type', 'retry-after'];

/**
 * What the proxy needs from its environment. Tests substitute `fetch`.
 */
export type ProxyDeps = {
  /**
   * Base URL of the Logbrook server, without a trailing slash.
   */
  readonly upstreamUrl: string;

  /**
   * The configured read token, sent as the bearer token of every upstream request.
   */
  readonly readToken: string;

  /**
   * The `fetch` used for upstream requests.
   */
  readonly fetch: typeof fetch;

  /**
   * Aborted when the process is shutting down. It ends the event streams so that a graceful stop
   * does not wait for a tail that would never finish. Other requests are left to complete.
   */
  readonly shutdownSignal?: AbortSignal;
};

/**
 * Forwards an allowlisted `GET` to Logbrook with the configured read token. The app does no
 * authentication of its own; whoever can reach it gets that token's access.
 *
 * The method and path are checked here. The request goes upstream with the bearer token, the
 * browser's `Accept` and `Last-Event-ID`, and the browser's abort signal, so an abandoned query is
 * cancelled in Logbrook too. The upstream body is streamed through without buffering. Status, the
 * `{"error"}` body and `Retry-After` pass through unchanged. Large JSON bodies are gzip-compressed
 * when the browser accepts it; the event stream never is.
 */
export async function handleProxyRequest(request: Request, deps: ProxyDeps): Promise<Response> {
  if (request.method !== 'GET') {
    return errorResponse(405, 'Only GET is supported', { allow: 'GET' });
  }

  const url = new URL(request.url);
  const route = url.pathname.startsWith(PROXY_PREFIX)
    ? matchAllowedRoute(url.pathname.slice(PROXY_PREFIX.length))
    : null;
  if (route === null) {
    return errorResponse(404, 'Unknown route');
  }

  // Only the tail honours a resume ID. Anywhere else the parameter is left alone and Logbrook
  // answers for it, so it can never become a header on a route that has no use for it.
  const resumeId = route.path.endsWith('/logs/tail') ? url.searchParams.get(RESUME_PARAM) : null;
  if (resumeId !== null && !isResumeId(resumeId)) {
    return errorResponse(400, 'Invalid last_event_id');
  }

  const search = resumeId === null ? url.search : withoutResumeParam(url);
  if (search === null) {
    return errorResponse(400, 'Invalid query string');
  }

  const isTail = route.path.endsWith('/logs/tail');
  const signal =
    isTail && deps.shutdownSignal !== undefined
      ? AbortSignal.any([request.signal, deps.shutdownSignal])
      : request.signal;
  const upstream = await fetchUpstream(request, deps, `${route.path}${search}`, resumeId, signal);
  if (upstream === null) {
    return errorResponse(502, 'Logbrook is unreachable');
  }

  return toBrowserResponse(request, upstream);
}

/**
 * The query string of `url` without the resume parameter, for the upstream request. Other
 * parameters keep their original encoding. Returns `null` when a parameter name has a malformed
 * percent-escape, which cannot be compared with the resume parameter's name.
 */
function withoutResumeParam(url: URL): string | null {
  const kept: string[] = [];
  for (const pair of url.search.slice(1).split('&')) {
    const name = decodeName(pair.split('=')[0] ?? '');
    if (name === null) {
      return null;
    }

    if (pair !== '' && name !== RESUME_PARAM) {
      kept.push(pair);
    }
  }

  return kept.length === 0 ? '' : `?${kept.join('&')}`;
}

/**
 * Percent-decodes a query parameter name, or returns `null` when it has a malformed escape.
 */
function decodeName(name: string): string | null {
  try {
    return decodeURIComponent(name);
  } catch {
    return null;
  }
}

/**
 * Sends the upstream request. Returns `null` when Logbrook cannot be reached. A browser abort
 * rejects with the signal's reason, which the handler lets propagate: nobody is waiting for the
 * answer.
 */
async function fetchUpstream(
  request: Request,
  deps: ProxyDeps,
  pathAndQuery: string,
  resumeId: string | null,
  signal: AbortSignal,
): Promise<Response | null> {
  const headers = new Headers({
    authorization: `Bearer ${deps.readToken}`,
    'accept-encoding': 'identity',
  });
  copyHeader(request.headers, headers, 'accept');
  copyHeader(request.headers, headers, 'last-event-id');
  if (resumeId !== null && !headers.has('last-event-id')) {
    headers.set('last-event-id', resumeId);
  }

  try {
    return await deps.fetch(`${deps.upstreamUrl}${pathAndQuery}`, {
      method: 'GET',
      headers,
      signal,
      redirect: 'manual',
    });
  } catch (error) {
    if (signal.aborted) {
      throw error;
    }

    return null;
  }
}

/**
 * Builds the response for the browser from the upstream one, streaming the body.
 */
function toBrowserResponse(request: Request, upstream: Response): Response {
  const headers = new Headers();
  for (const name of FORWARDED_RESPONSE_HEADERS) {
    copyHeader(upstream.headers, headers, name);
  }

  const contentType = upstream.headers.get('content-type') ?? '';
  if (contentType.startsWith('text/event-stream')) {
    headers.set('cache-control', 'no-cache, no-transform');
    headers.set('x-accel-buffering', 'no');
    return new Response(upstream.body, { status: upstream.status, headers });
  }

  headers.set('cache-control', 'no-store');
  headers.append('vary', 'accept-encoding');
  if (shouldCompress(request, upstream, contentType)) {
    headers.set('content-encoding', 'gzip');
    const body = upstream.body?.pipeThrough(new CompressionStream('gzip')) ?? null;
    return new Response(body, { status: upstream.status, headers });
  }

  copyHeader(upstream.headers, headers, 'content-length');
  return new Response(upstream.body, { status: upstream.status, headers });
}

/**
 * Whether to gzip the response: a successful JSON body that is not known to be small, for a browser
 * that accepts gzip.
 */
function shouldCompress(request: Request, upstream: Response, contentType: string): boolean {
  if (!upstream.ok || upstream.body === null || !contentType.startsWith('application/json')) {
    return false;
  }

  if (!/\bgzip\b/.test(request.headers.get('accept-encoding') ?? '')) {
    return false;
  }

  const length = Number(upstream.headers.get('content-length') ?? Number.POSITIVE_INFINITY);
  return length >= MIN_COMPRESSIBLE_BYTES;
}

/**
 * Copies one header from `source` to `target` when present.
 */
function copyHeader(source: Headers, target: Headers, name: string): void {
  const value = source.get(name);
  if (value !== null) {
    target.set(name, value);
  }
}

/**
 * Builds a JSON error response in Logbrook's own `{"error"}` shape.
 */
export function errorResponse(
  status: number,
  message: string,
  extraHeaders: Record<string, string> = {},
): Response {
  return Response.json(
    { error: message },
    { status, headers: { 'cache-control': 'no-store', ...extraHeaders } },
  );
}
