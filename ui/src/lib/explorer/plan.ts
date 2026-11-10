import type { QueryClient } from '@tanstack/react-query';

import { noop } from '@tanstack/react-query';

import type { LogbrookClient } from '@/lib/logbrook/client';
import type { LogFilters, TimeRange } from '@/lib/logbrook/types';

import { browserClient } from '@/lib/logbrook/client';
import { facetsQuery, histogramQuery, logsInfiniteQuery } from '@/lib/logbrook/queries';

import type { ExplorerSearch } from './search.ts';

import { pickIntervalMs } from './range.ts';
import { filtersOf, resolveTimeRange, textOf } from './search.ts';

/**
 * Lowest level counted as an error in the histogram overlay.
 */
export const ERROR_LEVEL = 50;

/**
 * Horizontal padding of the histogram, in pixels, which the loader subtracts from the window width
 * to guess the chart width before the chart has been measured.
 */
export const HISTOGRAM_PADDING_PX = 32;

/**
 * Everything the explorer's four requests are built from: the index, the frozen range, the filters
 * and the histogram interval.
 */
export type ExplorerPlan = {
  /**
   * Index to read.
   */
  readonly index: string;

  /**
   * Frozen absolute range.
   */
  readonly range: TimeRange;

  /**
   * Filters shared by every request.
   */
  readonly filters: LogFilters;

  /**
   * Histogram bucket duration in milliseconds.
   */
  readonly intervalMs: number;
};

/**
 * Builds the plan for a search and a chart width, or `null` when there is no index to read.
 *
 * The route loader and the component both call this, the loader with a width guessed from the
 * window and the component with the measured one. Because the interval is a rounded step, both
 * normally agree and the component finds its data already in the cache.
 */
export function buildPlan(
  search: ExplorerSearch,
  chartWidthPx: number,
  now: number,
): ExplorerPlan | null {
  const index = textOf(search.index);
  if (index === undefined || index === '') {
    return null;
  }

  const range = resolveTimeRange(search, now);
  const intervalMs = pickIntervalMs(range.to - range.from, chartWidthPx);
  return { index, range, filters: filtersOf(search), intervalMs };
}

/**
 * Returns the query options of the four requests: the log pages, the facets, the histogram of all
 * matching events (whose bucket sum is the total) and the histogram of errors for the overlay.
 *
 * The error histogram uses the stricter of 50 and the active minimum level, because `min_level` is
 * the only level filter the API has.
 */
export function planQueries(plan: ExplorerPlan, client: LogbrookClient = browserClient) {
  const { index, range, filters, intervalMs } = plan;
  const errorFilters = { ...filters, minLevel: Math.max(ERROR_LEVEL, filters.minLevel ?? 0) };
  return {
    logs: logsInfiniteQuery(index, { ...range, ...filters }, client),
    facets: facetsQuery(index, range, filters, client),
    histogram: histogramQuery(index, { ...range, ...filters, intervalMs }, client),
    errors: histogramQuery(index, { ...range, ...errorFilters, intervalMs }, client),
  };
}

/**
 * Starts the four requests without waiting for them, so they run while the route's component chunk
 * downloads. Failures are shown by the component that reads the same queries.
 */
export function startPlanQueries(queryClient: QueryClient, plan: ExplorerPlan): void {
  const queries = planQueries(plan);
  void queryClient.infiniteQuery(queries.logs).catch(noop);
  void queryClient.query(queries.facets).catch(noop);
  void queryClient.query(queries.histogram).catch(noop);
  void queryClient.query(queries.errors).catch(noop);
}

/**
 * Guesses the chart width before it is measured: the window minus the histogram padding.
 */
export function guessChartWidth(): number {
  if (typeof window === 'undefined') {
    return 1_200;
  }

  return Math.max(0, window.innerWidth - HISTOGRAM_PADDING_PX);
}
