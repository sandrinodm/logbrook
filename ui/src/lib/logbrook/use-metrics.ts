import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';

import type { MetricsSnapshot } from './metrics.ts';

import { metricsAccessOf } from './capabilities.ts';
import { LogbrookError } from './errors.ts';
import { metricsQuery } from './metrics-query.ts';

/**
 * The metrics view's state: the newest scrape, the one before it, and how the last request ended.
 */
export type MetricsState = {
  /**
   * The newest successful scrape, or `null` before the first.
   */
  readonly current: MetricsSnapshot | null;

  /**
   * The scrape before `current`, for rates; `null` until a second one arrives.
   */
  readonly previous: MetricsSnapshot | null;

  /**
   * Whether `/metrics` is usable for this token, as the last request showed.
   */
  readonly access: 'allowed' | 'forbidden' | 'unknown';

  /**
   * The error message of the last failed request, or `null`.
   */
  readonly errorMessage: string | null;

  /**
   * HTTP status of the last failed request, or `null` when it succeeded or failed without a
   * response (a network error).
   */
  readonly errorStatus: number | null;

  /**
   * Whether the first request is still running.
   */
  readonly isPending: boolean;

  /**
   * Starts a scrape now.
   */
  readonly refetch: () => void;
};

/**
 * Polls `/metrics` while the calling component is mounted and keeps the last two scrapes, so the
 * view can show per-second rates between them. Query pauses polling while the tab is hidden.
 */
export function useMetrics(): MetricsState {
  const query = useQuery(metricsQuery());
  const [history, setHistory] = useState<{
    readonly previous: MetricsSnapshot | null;
    readonly current: MetricsSnapshot | null;
  }>({ previous: null, current: null });

  const fresh = query.data;
  if (fresh !== undefined && fresh !== history.current) {
    setHistory({ previous: history.current, current: fresh });
  }

  return {
    current: history.current ?? fresh ?? null,
    previous: history.previous,
    access: metricsAccessOf(query.error, fresh !== undefined),
    errorMessage: query.error === null ? null : query.error.message,
    errorStatus: query.error instanceof LogbrookError ? query.error.status : null,
    isPending: query.isPending,
    refetch: () => void query.refetch(),
  };
}
