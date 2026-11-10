import { LogbrookError } from '@/lib/logbrook/errors';

/**
 * What the user can do about an error.
 */
export type ErrorAction = 'retry' | 'narrow' | 'wait' | 'none';

/**
 * An error ready to show: a short title, an explanation, the server's own text when it has one, and
 * the one thing worth offering.
 */
export type ErrorDescription = {
  /**
   * One-line summary.
   */
  readonly title: string;

  /**
   * Plain explanation of the cause and what to change.
   */
  readonly detail: string;

  /**
   * The server's `{"error"}` text, shown verbatim when it adds information; `null` otherwise.
   */
  readonly serverMessage: string | null;

  /**
   * The suggested way out.
   */
  readonly action: ErrorAction;

  /**
   * Seconds the server asked the client to wait, for `429`; `null` otherwise.
   */
  readonly retryAfterSeconds: number | null;
};

/**
 * Explains a failed request in terms of what to do next. `400` quotes the server, because its
 * message names the offending parameter; `504` and `413` suggest a smaller query; `401` means the
 * server's configured token was rejected; `403` and `404` are about access and the index name.
 */
export function describeError(error: unknown): ErrorDescription {
  if (!(error instanceof LogbrookError)) {
    const message = error instanceof Error ? error.message : String(error);
    return describe('Could not reach the server', 'The request failed before an answer arrived.', {
      serverMessage: message,
      action: 'retry',
    });
  }

  const known = describeStatus(error.status);
  const serverMessage = error.message === '' ? null : error.message;
  return { ...known, serverMessage, retryAfterSeconds: error.retryAfterSeconds };
}

/**
 * Describes a status code, without the server's text.
 */
function describeStatus(status: number): ErrorDescription {
  switch (status) {
    case 400:
      return describe(
        'The server rejected this query',
        'A parameter is invalid. Check the filters and the time range, then adjust them.',
        { action: 'none' },
      );
    case 401:
      return describe(
        "The server's Logbrook token was rejected",
        'The operator must fix LOGBROOK_READ_TOKEN in the server configuration. Reloading or retrying cannot help until then.',
        { action: 'none' },
      );
    case 403:
      return describe(
        'This token cannot read that',
        'The read token is limited to other sources or indexes. Remove the source filter, pick another index, or ask the operator for a broader read token.',
        { action: 'none' },
      );
    case 404:
      return describe(
        'Index not found',
        'The index does not exist, or this token cannot see it. Choose another index.',
        { action: 'none' },
      );
    case 413:
      return describe(
        'The result is too large',
        'A page exceeded the response size limit. Narrow the time range or add filters.',
        { action: 'narrow' },
      );
    case 429:
      return describe(
        'The server is busy',
        'Too many requests are running. The explorer retries on its own; you can also try again.',
        { action: 'wait' },
      );
    case 504:
      return describe(
        'The query took too long',
        'It hit the server deadline of a few seconds. Narrow the time range or add filters.',
        { action: 'narrow' },
      );
    default:
      return describe('The server returned an error', `The request failed with status ${status}.`, {
        action: 'retry',
      });
  }
}

/**
 * Builds an `ErrorDescription` with defaults for the optional parts.
 */
function describe(
  title: string,
  detail: string,
  extra: { readonly action: ErrorAction; readonly serverMessage?: string },
): ErrorDescription {
  return {
    title,
    detail,
    serverMessage: extra.serverMessage ?? null,
    action: extra.action,
    retryAfterSeconds: null,
  };
}

/**
 * What to offer when fetching an older page fails while earlier pages are on screen.
 */
export type NextPageOptions = {
  /**
   * Request the same page again.
   */
  readonly retry: boolean;

  /**
   * Start the search over. Offered when the cursor itself was refused (`400`: expired or invalid),
   * because asking for the same page again would fail again.
   */
  readonly restart: boolean;

  /**
   * Narrow the time range (`413` and `504`).
   */
  readonly narrow: boolean;
};

/**
 * Decides which actions fit a failed next-page request. The rows already loaded stay on screen, so
 * the actions concern only the failed page.
 */
export function nextPageOptions(error: unknown): NextPageOptions {
  const status = error instanceof LogbrookError ? error.status : null;
  const action = describeError(error).action;
  return {
    retry: status !== 400 && status !== 401,
    restart: status === 400,
    narrow: action === 'narrow',
  };
}
