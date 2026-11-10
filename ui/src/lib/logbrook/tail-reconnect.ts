/**
 * First delay before reconnecting, in milliseconds.
 */
const BASE_DELAY_MS = 1_000;

/**
 * Longest delay between reconnect attempts, in milliseconds.
 */
const MAX_DELAY_MS = 30_000;

/**
 * Longest wait honoured from a `Retry-After` header, in milliseconds. A misconfigured server must
 * not park the tail for an hour.
 */
const MAX_RETRY_AFTER_MS = 120_000;

/**
 * What the tail does after a failed connection attempt.
 */
export type FailureDecision =
  | {
      /**
       * Try again after `delayMs`.
       */
      readonly action: 'retry';

      /**
       * How long to wait, in milliseconds.
       */
      readonly delayMs: number;

      /**
       * Whether the failure is the subscriber limit (`429`), which the view explains.
       */
      readonly limit: boolean;
    }
  | {
      /**
       * Give up; trying again cannot help until the user changes something.
       */
      readonly action: 'stop';

      /**
       * Whether the server's configured token was rejected (`401`).
       */
      readonly unauthorized: boolean;

      /**
       * Whether the server refused the token's scope (`403`): the token is valid, but it may not
       * read this index or these sources.
       */
      readonly forbidden: boolean;
    };

/**
 * The delay before reconnect attempt number `attempt` (0 for the first retry): exponential from one
 * second up to thirty, with up to 25 percent jitter so many tabs do not retry in step.
 *
 * `random` returns a number in [0, 1); tests pass a constant.
 */
export function backoffDelayMs(attempt: number, random: () => number): number {
  const exponential = Math.min(MAX_DELAY_MS, BASE_DELAY_MS * 2 ** Math.min(attempt, 10));
  return Math.round(exponential * (1 + random() * 0.25));
}

/**
 * Decides what to do about an HTTP status seen before the stream started.
 *
 * - `429` (subscriber limit) and `503` (storage not ready) are temporary: retry, waiting at least as
 *   long as `Retry-After` asks.
 * - Other `5xx` (the proxy or Logbrook did not answer) and a status of `null` (no response at all)
 *   are treated as a network drop: retry with backoff.
 * - `401` means the server's configured token was rejected, which only the operator can fix. `403` is
 *   a scope refusal: the token is valid but may not read this index or these sources, so a retry
 *   cannot help. `400`, `404`, `413` and anything else are the request's own fault, so repeating it
 *   would only repeat the failure.
 */
export function decideAfterFailure(
  status: number | null,
  retryAfterSeconds: number | null,
  attempt: number,
  random: () => number,
): FailureDecision {
  if (status === 401) {
    return { action: 'stop', unauthorized: true, forbidden: false };
  }

  if (status === 403) {
    return { action: 'stop', unauthorized: false, forbidden: true };
  }

  const temporary = status === null || status === 429 || status >= 500;
  if (!temporary) {
    return { action: 'stop', unauthorized: false, forbidden: false };
  }

  const backoff = backoffDelayMs(attempt, random);
  const asked = Math.min(MAX_RETRY_AFTER_MS, (retryAfterSeconds ?? 0) * 1_000);
  return { action: 'retry', delayMs: Math.max(backoff, asked), limit: status === 429 };
}
