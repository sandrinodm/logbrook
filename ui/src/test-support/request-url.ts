/**
 * Returns the URL text of the first argument of `fetch`, which may be a string, a `URL` or a
 * `Request`. Test doubles for `fetch` use it to record what was requested.
 */
export function requestUrl(input: Parameters<typeof fetch>[0]): string {
  if (typeof input === 'string') {
    return input;
  }

  return input instanceof URL ? input.href : input.url;
}
