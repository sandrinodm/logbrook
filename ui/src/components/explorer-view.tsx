import type { RefCallback } from 'react';

import { useInfiniteQuery, useQuery, useQueryClient } from '@tanstack/react-query';
import { getRouteApi } from '@tanstack/react-router';
import { Activity, Suspense, lazy, useEffect, useEffectEvent, useRef, useState } from 'react';

import type { Density } from '@/components/display-controls';
import type { TimeZoneMode } from '@/lib/explorer/format';
import type { ExplorerPlan } from '@/lib/explorer/plan';
import type { RelativeRange } from '@/lib/explorer/range';
import type {
  ExplorerSearch,
  FacetField,
  RefreshSeconds,
  SearchPatch,
} from '@/lib/explorer/search';
import type { AttributeCondition, LogEvent, TimeRange } from '@/lib/logbrook/types';

import { DisplayControls } from '@/components/display-controls';
import { FacetSidebar } from '@/components/facet-sidebar';
import { HistogramChart } from '@/components/histogram-chart';
import { LogStreamTable } from '@/components/log-stream-table';
import { ErrorPanel } from '@/components/query-error-panel';
import { RangeControls } from '@/components/range-controls';
import { StructuredFilterBar } from '@/components/structured-filter-bar';
import { buildPlan, guessChartWidth, planQueries } from '@/lib/explorer/plan';
import {
  rangeLengthMs,
  sumBuckets,
  widerRange,
  zeroFill,
  zoomOutRange,
} from '@/lib/explorer/range';
import { eventAt, pageOffsets, rowOfEvent } from '@/lib/explorer/rows';
import {
  CLEAR_FILTERS_PATCH,
  applyPatch,
  attributesOf,
  describeFilters,
  encodeAttribute,
  facetPatch,
  isAbsoluteRange,
  listOf,
  queryDepsOf,
  relativeRangeOf,
  textOf,
  toggleAttribute,
  togglePinnedColumn,
} from '@/lib/explorer/search';
import { indexesQuery } from '@/lib/logbrook/queries';
import { hasCommandModifier, isDialogTarget, isTypingTarget } from '@/lib/shortcuts';
import { useElementWidth } from '@/lib/use-element-width';

/**
 * The inspector drawer, loaded the first time an event is selected. Its code (tabs, collapsible
 * sections, the `curl` builder) is not needed to read the first page of rows.
 */
const EventInspectorDrawer = lazy(() =>
  import('@/components/event-inspector-drawer').then((module) => ({
    default: module.EventInspectorDrawer,
  })),
);

/**
 * Typed access to the explorer route's search params, hooks and navigation without importing the
 * route module, which imports this one.
 */
const routeApi = getRouteApi('/_app/');

/**
 * Pages used while no data has arrived, as one shared empty list.
 */
const NO_PAGES: ReadonlyArray<never> = [];

/**
 * Relative range offered by "show the last 15 minutes" after a too-large or too-slow query.
 */
const NARROW_RANGE: RelativeRange = '15m';

/**
 * The display settings of a search, with their defaults.
 */
type DisplaySettings = {
  /**
   * Time zone of the timestamps.
   */
  readonly zone: TimeZoneMode;

  /**
   * Row density.
   */
  readonly density: Density;

  /**
   * Whether messages wrap.
   */
  readonly wrap: boolean;

  /**
   * Pinned attribute keys.
   */
  readonly columns: ReadonlyArray<string>;

  /**
   * Auto-refresh period in seconds, if any.
   */
  readonly refresh: RefreshSeconds | undefined;
};

/**
 * Selector for the display settings. A module-level function, so `useSearch` sees the same
 * reference every render, and with structural sharing the component re-renders only when a setting
 * changes.
 */
function displayOf(search: ExplorerSearch): DisplaySettings {
  return {
    zone: search.tz ?? 'local',
    density: search.density ?? 'comfortable',
    wrap: search.wrap === 1,
    columns: listOf(search.cols),
    refresh: search.refresh,
  };
}

/**
 * Selector for the id of the open event.
 */
function selectedIdOf(search: ExplorerSearch): string | undefined {
  return textOf(search.event);
}

/**
 * The explorer: filters, histogram, facets, the virtual event table and the inspector, all driven
 * by the URL and reading Logbrook through the proxy.
 *
 * It subscribes to three slices of the search: what the queries need, the display settings, and the
 * open event. Selecting an event therefore re-renders the table and drawer, not the histogram or
 * facets.
 */
export function ExplorerView() {
  const deps = routeApi.useSearch({ select: queryDepsOf });
  const [fallbackNow] = useState(() => Date.now());
  const [chartRef, chartWidth] = useElementWidth<HTMLDivElement>(guessChartWidth());
  const plan = buildPlan(deps, chartWidth, fallbackNow);
  const indexList = useQuery(indexesQuery());

  if (plan === null && indexList.isError) {
    // The list failed, so "no index" would be a guess: say why, e.g. the server's token was rejected.
    return <ErrorPanel error={indexList.error} onRetry={() => void indexList.refetch()} />;
  }

  if (plan === null) {
    return (
      <div className="flex flex-1 items-center justify-center p-6 text-center text-xs text-muted-foreground">
        This token can read no index yet. Create one in Indexes, or ingest into a new name.
      </div>
    );
  }

  return <ExplorerContent plan={plan} chartRef={chartRef} chartWidth={chartWidth} />;
}

/**
 * The explorer for a resolved plan: runs the four queries and lays the parts out.
 */
function ExplorerContent(props: {
  plan: ExplorerPlan;
  chartRef: RefCallback<HTMLDivElement>;
  chartWidth: number;
}) {
  const { plan } = props;
  const queryClient = useQueryClient();
  const navigate = routeApi.useNavigate();
  const deps = routeApi.useSearch({ select: queryDepsOf });
  const display = routeApi.useSearch({ select: displayOf });
  const selectedId = routeApi.useSearch({ select: selectedIdOf });
  const [showFacets, setShowFacets] = useState(true);
  // Becomes true with the first selection and stays true, so the drawer keeps its state (an
  // `Activity` hides it) after it is closed, while a visit that never opens it never loads it.
  const [inspectorUsed, setInspectorUsed] = useState(selectedId !== undefined);
  if (selectedId !== undefined && !inspectorUsed) {
    setInspectorUsed(true);
  }

  const searchInputRef = useRef<HTMLInputElement | null>(null);

  const queries = planQueries(plan);
  const logs = useInfiniteQuery(queries.logs);
  const facets = useQuery(queries.facets);
  const histogram = useQuery(queries.histogram);
  const errors = useQuery(queries.errors);

  // `useQuery` returns a new result object on every render, but its methods are stable. Taking them
  // out here keeps the callbacks below, and so the components they are passed to, unchanged when
  // only the selected row changes.
  const { refetch: refetchLogs, fetchNextPage } = logs;
  const { refetch: refetchFacets } = facets;

  const pages = logs.data?.pages ?? NO_PAGES;
  const offsets = pageOffsets(pages);
  const selectedRow = selectedId === undefined ? -1 : rowOfEvent(pages, offsets, selectedId);
  const selectedEvent = selectedRow < 0 ? undefined : eventAt(pages, offsets, selectedRow);
  const [shownEvent, setShownEvent] = useState<LogEvent | undefined>(selectedEvent);
  if (selectedEvent !== undefined && selectedEvent !== shownEvent) {
    setShownEvent(selectedEvent);
  }

  /**
   * Zero-fills a histogram response over the plan's range, or `undefined` before it arrived.
   */
  const bucketsOf = (data: typeof histogram.data) =>
    data === undefined ? undefined : zeroFill(data.buckets, plan.range, plan.intervalMs);
  const totalBuckets = bucketsOf(histogram.data);
  const errorBuckets = bucketsOf(errors.data);
  const total = histogram.data === undefined ? null : sumBuckets(histogram.data.buckets);

  const absolute = isAbsoluteRange(deps);
  // When the logs query fails, the table explains it in full. The chart and the facets, which ask
  // the same question and fail the same way, then stay quiet instead of repeating the message.
  // A failed older page also puts the infinite query in the `error` state, but the first page is
  // loaded then: the table keeps its rows and shows that failure under them instead.
  const nextPageFailed = logs.isFetchNextPageError;
  const tableStatus = nextPageFailed ? 'success' : logs.status;
  const queryFailed = tableStatus === 'error';

  /**
   * Applies a change to the search params. Filter changes add a history entry so the back button
   * undoes them; `replace` is for settings and the open event.
   */
  const patch = (change: SearchPatch, replace = false) => {
    void navigate({ search: (previous) => applyPatch(previous, change), replace });
  };

  /**
   * Freezes a new "now" for a relative range and restarts paging.
   */
  const refresh = () => patch({ at: Date.now() }, true);

  /**
   * Selects a relative range and freezes "now".
   */
  const selectRange = (range: RelativeRange) =>
    patch({ range, from: undefined, to: undefined, at: Date.now() });

  /**
   * Zooms to an absolute range, as the histogram drag does.
   */
  const zoomTo = (range: TimeRange) =>
    patch({ from: range.from, to: range.to, range: undefined, at: undefined });

  /**
   * Switches to the next longer preset, if there is one.
   */
  const widen = () => {
    const wider = widerRange(plan.range.to - plan.range.from);
    if (wider !== null) {
      selectRange(wider);
    }
  };

  /**
   * Applies a click on a facet value, or a "filter by" action of the inspector.
   */
  const selectFacet = (field: FacetField, value: string) => {
    patch(facetPatch(field, value, plan.filters));
  };

  /**
   * Toggles an attribute equality condition.
   */
  const filterAttribute = (condition: AttributeCondition) => {
    const next = toggleAttribute(attributesOf(deps), condition);
    patch({ attrs: next.map(encodeAttribute) });
  };

  /**
   * Starts the search over after the server refused a cursor. A relative range gets a new "now",
   * which is a new query; an absolute range has the same key, so its cache entry is reset to load
   * the first page afresh. Refetching would replay every loaded page with its old cursor.
   */
  const restartSearch = () => {
    if (absolute) {
      void queryClient.resetQueries({ queryKey: queries.logs.queryKey, exact: true });
    } else {
      refresh();
    }
  };

  useEffect(() => {
    if (display.refresh === undefined || absolute) {
      return undefined;
    }

    const timer = setInterval(() => {
      if (!document.hidden) {
        void navigate({
          search: (previous) => applyPatch(previous, { at: Date.now() }),
          replace: true,
        });
      }
    }, display.refresh * 1000);
    return () => clearInterval(timer);
  }, [display.refresh, absolute, navigate]);

  /**
   * Closes the inspector and puts the focus back on the event grid. The panel is hidden, not
   * removed, so without this the focus would fall to the page when the element holding it
   * disappears.
   */
  const closeInspector = () => {
    patch({ event: undefined }, true);
    document.querySelector<HTMLElement>('[role="grid"]')?.focus();
  };

  /**
   * `/` focuses the message box; `Escape` leaves a field, or closes the inspector. An effect event,
   * so the listener below is added once and still sees the latest selection.
   */
  const onKeyDown = useEffectEvent((event: KeyboardEvent) => {
    const typing = isTypingTarget(event.target);
    if (event.key === '/' && !typing && !hasCommandModifier(event)) {
      event.preventDefault();
      searchInputRef.current?.focus();
    } else if (event.key === 'Escape' && !isDialogTarget(event.target)) {
      if (typing && event.target instanceof HTMLElement) {
        event.target.blur();
      } else if (selectedId !== undefined) {
        closeInspector();
      }
    }
  });

  useEffect(() => {
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, []);

  return (
    <div className="flex flex-1 flex-col overflow-hidden">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border bg-background pr-3">
        <StructuredFilterBar
          filters={plan.filters}
          facets={facets.data}
          searchInputRef={searchInputRef}
          showFacets={showFacets}
          onToggleFacets={() => setShowFacets((shown) => !shown)}
          onPatch={(change) => patch(change, 'message' in change)}
          onClear={() => patch(CLEAR_FILTERS_PATCH)}
        />
        <RangeControls
          relative={absolute ? undefined : relativeRangeOf(deps)}
          range={plan.range}
          autoRefresh={display.refresh}
          zone={display.zone}
          busy={logs.isFetching && !logs.isFetchingNextPage}
          onRange={selectRange}
          onRefresh={refresh}
          onAutoRefresh={(refreshSeconds) => patch({ refresh: refreshSeconds }, true)}
        />
      </div>

      <HistogramChart
        range={plan.range}
        intervalMs={plan.intervalMs}
        buckets={totalBuckets}
        errorBuckets={errorBuckets}
        total={total}
        error={queryFailed ? null : histogram.error}
        unavailable={queryFailed}
        stale={histogram.isPlaceholderData}
        zone={display.zone}
        width={props.chartWidth}
        containerRef={props.chartRef}
        onZoom={zoomTo}
        onZoomOut={() => zoomTo(zoomOutRange(plan.range, Date.now()))}
      />

      <div className="flex flex-1 overflow-hidden">
        <Activity mode={showFacets ? 'visible' : 'hidden'}>
          <FacetSidebar
            facets={facets.data}
            error={queryFailed ? null : facets.error}
            unavailable={queryFailed}
            stale={facets.isPlaceholderData}
            filters={plan.filters}
            onSelect={selectFacet}
            onRetry={() => void refetchFacets()}
          />
        </Activity>

        <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
          <div className="flex shrink-0 justify-end border-b border-border bg-card/20 px-3 py-1">
            <DisplayControls
              zone={display.zone}
              density={display.density}
              wrap={display.wrap}
              columns={display.columns}
              onZone={(tz) => patch({ tz }, true)}
              onDensity={(density) => patch({ density }, true)}
              onWrap={(wrap) => patch({ wrap: wrap ? 1 : undefined }, true)}
              onUnpin={(key) => patch({ cols: togglePinnedColumn(display.columns, key) }, true)}
            />
          </div>
          <LogStreamTable
            pages={pages}
            status={tableStatus}
            error={logs.error}
            nextPageError={nextPageFailed ? logs.error : null}
            stale={logs.isPlaceholderData}
            fetchingMore={logs.isFetchingNextPage}
            // After a failure the table must not ask again by itself, or it would loop on the same error.
            canFetchMore={logs.hasNextPage && !logs.isFetching && !nextPageFailed}
            fetchNextPage={() => void fetchNextPage()}
            selectedId={selectedId}
            onSelect={(id) => patch({ event: id }, true)}
            highlight={plan.filters.message ?? ''}
            zone={display.zone}
            density={display.density}
            wrap={display.wrap}
            columns={display.columns}
            filterSummary={describeFilters(plan.filters)}
            rangeSummary={describeRange(plan.range, absolute, deps)}
            onClearFilters={
              Object.keys(plan.filters).length > 0 ? () => patch(CLEAR_FILTERS_PATCH) : undefined
            }
            onWiden={absolute || relativeRangeOf(deps) === '30d' ? undefined : widen}
            onRetry={() => void refetchLogs()}
            onRetryNextPage={() => void fetchNextPage()}
            onRestart={restartSearch}
            onNarrow={() => selectRange(NARROW_RANGE)}
          />
        </div>

        {inspectorUsed ? (
          <Suspense fallback={null}>
            <Activity mode={selectedId === undefined ? 'hidden' : 'visible'}>
              <EventInspectorDrawer
                event={selectedEvent ?? (selectedId === undefined ? shownEvent : undefined)}
                index={plan.index}
                zone={display.zone}
                filters={plan.filters}
                columns={display.columns}
                onClose={closeInspector}
                onFilterField={selectFacet}
                onFilterAttribute={filterAttribute}
                onToggleColumn={(key) =>
                  patch({ cols: togglePinnedColumn(display.columns, key) }, true)
                }
              />
            </Activity>
          </Suspense>
        ) : null}
      </div>
    </div>
  );
}

/**
 * Describes the time range for the empty state: "the last 1h", or the absolute bounds.
 */
function describeRange(range: TimeRange, absolute: boolean, deps: ExplorerSearch): string {
  if (absolute) {
    return `the range ${new Date(range.from).toISOString()} to ${new Date(range.to).toISOString()}`;
  }

  const relative = relativeRangeOf(deps);
  return rangeLengthMs(relative) > 0 ? `the last ${relative}` : 'this range';
}
