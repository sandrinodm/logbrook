import type { SearchParams } from '@/lib/logbrook/types';

import { searchQueryString } from '@/lib/logbrook/search-params';

/**
 * Address printed in copied commands. The browser only knows the BFF, so the command targets a
 * default local Logbrook; the user replaces it with their own server.
 */
const EXAMPLE_BASE_URL = 'http://127.0.0.1:3100';

/**
 * Builds a `curl` command that runs a search against the Logbrook API directly.
 *
 * The query string comes from the same serialiser the UI uses, so the command returns what the
 * explorer shows. The token is read from `$LOGBROOK_READ_TOKEN` and never inlined.
 */
export function buildSearchCurl(index: string, params: SearchParams): string {
  const query = searchQueryString(params, null);
  const url = `${EXAMPLE_BASE_URL}/indexes/${encodeURIComponent(index)}/logs?${query}`;
  return [
    'curl --fail-with-body --silent --show-error \\',
    '  -H "Authorization: Bearer $LOGBROOK_READ_TOKEN" \\',
    `  '${url.replaceAll("'", '%27')}'`,
  ].join('\n');
}

/**
 * Builds the search that returns every event of the millisecond an event happened in, no filters
 * applied. The API has no lookup by id, so this is the narrowest query that reproduces an event.
 */
export function eventWindowParams(eventTime: number): SearchParams {
  return { from: eventTime, to: eventTime + 1 };
}
