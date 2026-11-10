import { Schema } from 'effect';

/**
 * Shortest Logbrook token, in characters.
 */
const MIN_TOKEN_LENGTH = 16;

/**
 * Longest Logbrook token, in characters.
 */
const MAX_TOKEN_LENGTH = 512;

/**
 * Visible ASCII only, which is all a Logbrook token may contain: no whitespace, no control
 * characters.
 */
const TOKEN_PATTERN = new RegExp(`^[\\x21-\\x7e]{${MIN_TOKEN_LENGTH},${MAX_TOKEN_LENGTH}}$`);

/**
 * Where a local Logbrook listens by default, so `pnpm dev` works against `logbrook serve` without
 * any URL configuration.
 */
const DEFAULT_LOGBROOK_URL = 'http://127.0.0.1:3100';

/**
 * A base URL with an `http` or `https` scheme and no trailing slash.
 */
const BaseUrl = Schema.String.check(
  Schema.makeFilter((value: string) => {
    if (!isHttpUrl(value)) {
      return 'must be an absolute http or https URL';
    }

    return true;
  }),
);

/**
 * The URL variable of the environment the BFF reads. The tokens are checked separately, by
 * `checkToken`, because a schema error would quote the offending value and a token must never reach
 * a message.
 */
const EnvSchema = Schema.Struct({ LOGBROOK_URL: BaseUrl });

/**
 * Validated settings of the BFF. They stay on the server: no token is ever sent to the browser.
 */
export type ServerConfig = {
  /**
   * Base URL of the Logbrook server, without a trailing slash.
   */
  readonly logbrookUrl: string;

  /**
   * Read token the proxy and the index list send to Logbrook.
   */
  readonly readToken: string;

  /**
   * Admin token for creating and deleting indexes and for the full index list, or `null` when none
   * is configured, which makes index management unavailable.
   */
  readonly adminToken: string | null;
};

/**
 * Environment variables as `process.env` exposes them.
 */
export type Env = Readonly<Record<string, string | undefined>>;

/**
 * Reads and validates the BFF settings from `env`.
 *
 * `LOGBROOK_URL` defaults to a local server. `LOGBROOK_READ_TOKEN` is required, in development too:
 * there is no fallback token. `LOGBROOK_ADMIN_TOKEN` is optional. A variable that is empty or only
 * whitespace counts as not set. Both tokens must have the shape Logbrook accepts and must differ.
 * Throws an error that names the variable and the broken rule, never the value.
 */
export function parseServerConfig(env: Env): ServerConfig {
  const url = setValue(env.LOGBROOK_URL) ?? DEFAULT_LOGBROOK_URL;
  const readToken = setValue(env.LOGBROOK_READ_TOKEN);
  const adminToken = setValue(env.LOGBROOK_ADMIN_TOKEN) ?? null;

  const decodedUrl = decodeUrl(url);
  if (readToken === undefined) {
    throw invalid('LOGBROOK_READ_TOKEN is required: set it to a Logbrook read token');
  }

  assertToken('LOGBROOK_READ_TOKEN', readToken);
  if (adminToken !== null) {
    assertToken('LOGBROOK_ADMIN_TOKEN', adminToken);
  }

  if (adminToken === readToken) {
    throw invalid('LOGBROOK_READ_TOKEN and LOGBROOK_ADMIN_TOKEN must be different tokens');
  }

  return { logbrookUrl: decodedUrl, readToken, adminToken };
}

/**
 * Settings read from `process.env` the first time they are needed, and reused afterwards.
 */
export function serverConfig(): ServerConfig {
  cachedConfig ??= parseServerConfig(process.env);
  return cachedConfig;
}

/**
 * Cache behind `serverConfig`.
 */
let cachedConfig: ServerConfig | undefined;

/**
 * Validates `LOGBROOK_URL` and returns it without trailing slashes.
 */
function decodeUrl(url: string): string {
  try {
    return Schema.decodeUnknownSync(EnvSchema)({ LOGBROOK_URL: url }).LOGBROOK_URL.replace(
      /\/+$/,
      '',
    );
  } catch (error) {
    throw new Error(`Invalid BFF environment: ${describeProblem(error)}`, { cause: error });
  }
}

/**
 * Throws unless `value` has the shape of a Logbrook token. The message names the variable and the
 * rule only.
 */
function assertToken(name: string, value: string): void {
  if (!TOKEN_PATTERN.test(value)) {
    throw invalid(
      `${name} must be ${MIN_TOKEN_LENGTH} to ${MAX_TOKEN_LENGTH} visible ASCII characters without whitespace`,
    );
  }
}

/**
 * The error for a broken setting.
 */
function invalid(problem: string): Error {
  return new Error(`Invalid BFF environment: ${problem}`);
}

/**
 * Returns `value` unless it is missing, empty or only whitespace, which all count as not set. An
 * `.env` file copied from `.env.example` leaves such entries behind.
 */
function setValue(value: string | undefined): string | undefined {
  return value === undefined || value.trim() === '' ? undefined : value;
}

/**
 * Whether `value` parses as an absolute http or https URL.
 */
function isHttpUrl(value: string): boolean {
  try {
    const { protocol } = new URL(value);
    return protocol === 'http:' || protocol === 'https:';
  } catch {
    return false;
  }
}

/**
 * Returns the first lines of a schema error, which name the variable and the broken rule.
 */
function describeProblem(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.split('\n').slice(0, 3).join(' ').trim();
}
