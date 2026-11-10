import { queryOptions } from '@tanstack/react-query';

import type { LogbrookClient } from './client.ts';
import type { MetricsSnapshot } from './metrics.ts';

import { browserClient } from './client.ts';
import { LogbrookError } from './errors.ts';
import { summarizeMetrics } from './metrics.ts';
import { parsePrometheusText } from './prometheus.ts';
import { logbrookKeys } from './queries.ts';

/**
 * How often the metrics view scrapes `/metrics` while it is open, in milliseconds. Query pauses the
 * timer while the browser tab is hidden.
 */
export const METRICS_POLL_MS = 5_000;

/**
 * Query options for `/metrics`: fetched through the proxy, parsed, and stamped with the time it
 * arrived so rates can be computed between scrapes. Polls every `METRICS_POLL_MS`, except after a
 * `403`, which a retry cannot fix because the read token is restricted.
 */
export function metricsQuery(client: LogbrookClient = browserClient) {
  return queryOptions({
    queryKey: logbrookKeys.metrics(),
    queryFn: async ({ signal }): Promise<MetricsSnapshot> => {
      const text = await client.getMetrics(signal);
      return { takenAt: Date.now(), summary: summarizeMetrics(parsePrometheusText(text)) };
    },
    staleTime: 0,
    refetchInterval: (query) => (isForbidden(query.state.error) ? false : METRICS_POLL_MS),
    refetchOnWindowFocus: false,
  });
}

/**
 * Whether an error is a `403` from Logbrook.
 */
function isForbidden(error: unknown): boolean {
  return error instanceof LogbrookError && error.status === 403;
}
