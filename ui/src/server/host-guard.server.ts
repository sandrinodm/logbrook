import { Schema } from 'effect';

/**
 * Address the server binds when neither `NITRO_HOST` nor `HOST` is set. The app has no
 * authentication, so the default must not be reachable from other machines.
 */
export const DEFAULT_BIND_HOST = '127.0.0.1';

/**
 * Host names that always reach the app. A browser sends the host name of the URL it was given, and
 * a page on another origin cannot make it send one of these, so accepting them does not open the
 * app to DNS rebinding. It keeps the container health check and a local `curl` working when
 * `ALLOWED_HOSTS` names only the public host.
 */
const LOOPBACK_HOSTS: readonly string[] = ['localhost', '127.0.0.1', '[::1]'];

/**
 * Longest host name accepted in `ALLOWED_HOSTS`, in characters (the DNS limit).
 */
const MAX_HOST_LENGTH = 253;

/**
 * A DNS name, an IPv4 address or a bracketed IPv6 address, lower case, without a port. No
 * wildcards.
 */
const HOST_PATTERN = /^(?:\[[0-9a-f:.]+\]|[a-z0-9_](?:[a-z0-9_.-]*[a-z0-9_])?)$/;

/**
 * One entry of `ALLOWED_HOSTS` after normalisation.
 */
const AllowedHost = Schema.String.check(
  Schema.makeFilter((value: string) => {
    if (value.length > MAX_HOST_LENGTH || !HOST_PATTERN.test(value)) {
      return 'must be a host name or IP address without scheme, port, path or wildcard';
    }

    return true;
  }),
);

/**
 * Environment variables as `process.env` exposes them.
 */
type Env = Readonly<Record<string, string | undefined>>;

/**
 * How the host check is set up.
 */
export type HostGuard = {
  /**
   * Returns the response that refuses the request, or `null` when its host is allowed.
   */
  readonly check: (request: Request) => Response | null;

  /**
   * A warning for the boot log, or `null`. Set when every host is accepted because the server is
   * bound to a non-loopback address and `ALLOWED_HOSTS` is not set.
   */
  readonly warning: string | null;
};

/**
 * The address the server binds: `NITRO_HOST`, else `HOST`, else the loopback default. Empty and
 * whitespace-only values count as not set, as Nitro treats them. This is what the Nitro Node server
 * reads, so the guard and the listener agree.
 */
export function bindHostOf(env: Env): string {
  return setValue(env.NITRO_HOST) ?? setValue(env.HOST) ?? DEFAULT_BIND_HOST;
}

/**
 * Fills `HOST` with the loopback default when neither `NITRO_HOST` nor `HOST` is set. Nitro's Node
 * server listens on all interfaces when no host is given, and reads the variables once, when its
 * entry module starts.
 */
export function applyDefaultHost(env: Record<string, string | undefined>): void {
  if (setValue(env.NITRO_HOST) === undefined && setValue(env.HOST) === undefined) {
    env.HOST = DEFAULT_BIND_HOST;
  }
}

/**
 * Whether a bind address is a loopback one: `localhost`, `127.0.0.0/8` or `::1`.
 */
export function isLoopbackAddress(host: string): boolean {
  const name =
    host.includes(':') && !host.startsWith('[') ? host.trim().toLowerCase() : normalizeHost(host);
  return (
    name === 'localhost' || name === '::1' || name === '[::1]' || /^127(?:\.\d{1,3}){3}$/.test(name)
  );
}

/**
 * Parses `ALLOWED_HOSTS`: comma-separated host names, ports ignored, case-insensitive. Returns
 * `null` when the variable is missing or empty. Throws an error naming the variable and the
 * offending entry when one is not a plain host name.
 */
export function parseAllowedHosts(value: string | undefined): readonly string[] | null {
  const text = setValue(value);
  if (text === undefined) {
    return null;
  }

  const hosts: string[] = [];
  for (const entry of text.split(',')) {
    if (entry.trim() === '') {
      continue;
    }

    const host = normalizeHost(entry);
    try {
      hosts.push(Schema.decodeUnknownSync(AllowedHost)(host));
    } catch {
      throw new Error(
        `Invalid BFF environment: ALLOWED_HOSTS entry "${entry.trim()}" must be a host name or IP address without scheme, port, path or wildcard`,
      );
    }
  }

  return hosts.length === 0 ? null : hosts;
}

/**
 * Builds the host check from the environment.
 *
 * With `ALLOWED_HOSTS` set, only those hosts (and the loopback names) pass. Without it, a server
 * bound to a loopback address accepts only the loopback names, and a server bound elsewhere (a
 * container) accepts any host and gets a boot warning instead. A refused request gets `421` with a
 * short plain-text body and never reaches a route, so no upstream call is made for it.
 */
export function createHostGuard(env: Env): HostGuard {
  const configured = parseAllowedHosts(env.ALLOWED_HOSTS);
  const bindHost = bindHostOf(env);
  if (configured === null && !isLoopbackAddress(bindHost)) {
    return {
      check: () => null,
      warning:
        'ALLOWED_HOSTS is not set and the server listens on a non-loopback address, so any Host header is accepted. Set ALLOWED_HOSTS to the public hostname (comma-separated for several) to block DNS rebinding.',
    };
  }

  const allowed = new Set([...LOOPBACK_HOSTS, ...(configured ?? [])]);
  return {
    check: (request) => (allowed.has(requestHost(request)) ? null : refused()),
    warning: null,
  };
}

/**
 * The host of a request without its port, lower case. The `Host` header is used; a request without
 * one (an internal call made with a relative URL) falls back to the host of its URL.
 */
function requestHost(request: Request): string {
  const header = request.headers.get('host');
  if (header !== null) {
    return normalizeHost(header);
  }

  try {
    return normalizeHost(new URL(request.url).host);
  } catch {
    return '';
  }
}

/**
 * Lower-cases a host, drops its port and a trailing dot, and keeps IPv6 addresses in brackets.
 */
function normalizeHost(value: string): string {
  const host = value.trim().toLowerCase();
  const withoutPort = host.startsWith('[')
    ? host.replace(/\](?::\d*)?$/, ']')
    : host.replace(/:\d*$/, '');
  return withoutPort.replace(/\.$/, '');
}

/**
 * The refusal for a host that is not allowed. `421 Misdirected Request` says the request reached a
 * server that does not serve that name. The body does not repeat the header.
 */
function refused(): Response {
  return new Response(
    'Misdirected request: this host name is not allowed. The operator must add it to ALLOWED_HOSTS.\n',
    {
      status: 421,
      headers: { 'content-type': 'text/plain; charset=utf-8', 'cache-control': 'no-store' },
    },
  );
}

/**
 * Returns `value` unless it is missing, empty or only whitespace, which all count as not set.
 */
function setValue(value: string | undefined): string | undefined {
  return value === undefined || value.trim() === '' ? undefined : value.trim();
}
