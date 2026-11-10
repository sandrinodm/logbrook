/**
 * An error response from Logbrook, or from the proxy in front of it.
 *
 * `status` is the HTTP status. `retryAfterSeconds` is the parsed `Retry-After` header of a `429`,
 * and `null` when the response had none. `message` is the server's `{"error"}` text, which is for
 * people: callers branch on `status`, never on the message.
 */
export class LogbrookError extends Error {
  /**
   * HTTP status of the failed response.
   */
  readonly status: number;

  /**
   * Seconds the server asked to wait before retrying, or `null`.
   */
  readonly retryAfterSeconds: number | null;

  constructor(status: number, message: string, retryAfterSeconds: number | null) {
    super(message);
    this.name = 'LogbrookError';
    this.status = status;
    this.retryAfterSeconds = retryAfterSeconds;
  }
}

/**
 * Parses a `Retry-After` header given in seconds. HTTP dates are not used by Logbrook and yield
 * `null`, as do missing, negative and non-numeric values.
 */
export function parseRetryAfter(header: string | null): number | null {
  if (header === null || !/^\d+$/.test(header.trim())) {
    return null;
  }

  return Number(header.trim());
}

/**
 * Reads the `{"error"}` message out of a failed response, falling back to the status line when the
 * body is empty, not JSON or has no `error` string (for example a gateway's HTML error page).
 */
async function readErrorMessage(response: Response): Promise<string> {
  const fallback = `${response.status} ${response.statusText}`.trim();
  try {
    const body: unknown = await response.json();
    return errorMessageOf(body) ?? fallback;
  } catch {
    return fallback;
  }
}

/**
 * Returns the `error` string of a decoded error body, or `undefined` when the body has another
 * shape.
 */
function errorMessageOf(body: unknown): string | undefined {
  if (typeof body !== 'object' || body === null || !('error' in body)) {
    return undefined;
  }

  return typeof body.error === 'string' ? body.error : undefined;
}

/**
 * Builds a `LogbrookError` from a non-2xx response, consuming its body.
 */
export async function errorFromResponse(response: Response): Promise<LogbrookError> {
  const message = await readErrorMessage(response);
  const retryAfter = parseRetryAfter(response.headers.get('retry-after'));
  return new LogbrookError(response.status, message, retryAfter);
}

/**
 * Whether a failed request is worth repeating: only rate limiting (`429`) and storage that is
 * briefly unavailable (`503`). `400` means the request itself is wrong, so repeating it cannot
 * help; `401` (the server's token was rejected) and `403` (outside the token's scope) need the
 * operator; `504` means the query hit its deadline and would again.
 */
export function isRetryable(error: unknown): boolean {
  return error instanceof LogbrookError && (error.status === 429 || error.status === 503);
}

/**
 * What to tell a reader when Logbrook answers `401`: the read token in the server's configuration
 * is wrong, which only the operator can fix. Retrying cannot help until it is fixed.
 */
export const READ_TOKEN_REJECTED_MESSAGE =
  "The server's Logbrook token was rejected. The operator must fix LOGBROOK_READ_TOKEN.";
