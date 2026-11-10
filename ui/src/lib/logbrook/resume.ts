/**
 * Query parameter that carries a tail resume ID to the proxy. A browser `EventSource` sends
 * `Last-Event-ID` by itself only when the browser reconnects it, so a connection the tail store
 * opens anew passes the ID in the URL. The proxy turns it into the header and does not forward the
 * parameter. This module is shared because the browser bundle cannot import the proxy.
 */
export const RESUME_PARAM = 'last_event_id';

/**
 * Longest resume ID the proxy accepts, in characters. The server's own cursors are at most 1,024
 * bytes including the index prefix.
 */
const MAX_RESUME_ID_LENGTH = 1_100;

/**
 * Visible ASCII without spaces, which is what the server's opaque IDs are made of. The point of the
 * check is that nothing a visitor puts in the URL can contain a line break or other control
 * character on its way into a request header.
 */
const RESUME_ID_PATTERN = new RegExp(`^[\\x21-\\x7e]{1,${MAX_RESUME_ID_LENGTH}}$`);

/**
 * Whether `value` is safe to send upstream as a `Last-Event-ID` header.
 */
export function isResumeId(value: string): boolean {
  return RESUME_ID_PATTERN.test(value);
}
