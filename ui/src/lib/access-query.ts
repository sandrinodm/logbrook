import { queryOptions } from '@tanstack/react-query';

import { getAccessFn } from '@/functions/access.functions';

/**
 * Query options for what the server's tokens allow (see `getAccessFn`). It only changes when the
 * operator restarts the server with other tokens, so it never goes stale: the layout loads it on
 * the server for the first page and the browser reuses that answer for the rest of the visit,
 * without a request per navigation.
 */
export function accessQuery() {
  return queryOptions({
    queryKey: ['access'],
    queryFn: () => getAccessFn(),
    staleTime: Number.POSITIVE_INFINITY,
  });
}
