import type { KeyboardEvent, ReactNode } from 'react';

import { useVirtualizer } from '@tanstack/react-virtual';
import { memo, useDeferredValue, useEffect, useEffectEvent, useRef } from 'react';

import type { TimeZoneMode } from '@/lib/explorer/format';
import type { LogEvent, SearchResult } from '@/lib/logbrook/types';

import { NextPageError } from '@/components/next-page-error';
import { ErrorPanel } from '@/components/query-error-panel';
import { LevelBadge } from '@/components/ui/reui-base';
import { dayKey, formatClock, formatDay, splitHighlight } from '@/lib/explorer/format';
import { eventAt, pageOffsets, rowOfEvent, totalRows } from '@/lib/explorer/rows';
import { MAX_LOADED_EVENTS } from '@/lib/logbrook/queries';
import { hasCommandModifier, isTypingTarget } from '@/lib/shortcuts';
import { cn, getLevelMeta } from '@/lib/utils';

/**
 * How long a requested selection counts as "on its way", in milliseconds. After that the selection
 * in the URL is taken as it is, so a change that never arrived cannot trap the keys.
 */
const PENDING_MOVE_MS = 400;

/**
 * The selection the last `j`, `k` or arrow press asked for.
 */
type PendingMove = {
  /**
   * Id of the event that press selected.
   */
  readonly id: string;

  /**
   * Row of that event.
   */
  readonly row: number;

  /**
   * When the press happened, as a Unix time in milliseconds.
   */
  readonly at: number;
};

/**
 * Row height in pixels per density. Fixed heights keep scrolling constant-time: nothing is
 * measured.
 */
const ROW_HEIGHT_PX = {
  /**
   * Compact rows, for scanning many events.
   */
  compact: 24,

  /**
   * Comfortable rows, with more air.
   */
  comfortable: 30,
} as const;

/**
 * Rows rendered beyond the viewport on each side, so a fast scroll shows no blank band.
 */
const OVERSCAN_ROWS = 15;

/**
 * The next page is requested when the viewport is this many rows from the end of the loaded rows.
 */
const PREFETCH_ROWS = 100;

/**
 * Height of the column header in pixels.
 */
const HEADER_HEIGHT_PX = 28;

/**
 * Skeleton rows shown while the first page loads; the rest of the window is clipped.
 */
const SKELETON_ROWS = 40;

/**
 * Width of a pinned attribute column in pixels.
 */
const PINNED_COLUMN_PX = 140;

/**
 * Row density.
 */
type Density = keyof typeof ROW_HEIGHT_PX;

/**
 * What the table needs to draw and page through the rows.
 */
type LogStreamTableProps = {
  /**
   * Loaded pages, newest first. They stay separate arrays; row `i` is found by offset arithmetic.
   */
  readonly pages: ReadonlyArray<SearchResult>;

  /**
   * State of the first page: `pending` shows skeleton rows and `error` the error panel. A failed
   * later page is not an `error` here; it is `nextPageError`, and the rows stay.
   */
  readonly status: 'pending' | 'error' | 'success';

  /**
   * The error of the first page, when `status` is `error`.
   */
  readonly error: unknown;

  /**
   * Whether the rows belong to the previous query and a new one is loading; they are dimmed.
   */
  readonly stale: boolean;

  /**
   * The error of the last attempt to load an older page, or `null`. The loaded rows stay on screen
   * and the failure is shown under them.
   */
  readonly nextPageError: unknown;

  /**
   * Whether the next page is being fetched.
   */
  readonly fetchingMore: boolean;

  /**
   * Whether a next page exists and no request is running, so asking for it is safe.
   */
  readonly canFetchMore: boolean;

  /**
   * Asks for the next page.
   */
  readonly fetchNextPage: () => void;

  /**
   * Id of the event open in the inspector, or `undefined`.
   */
  readonly selectedId: string | undefined;

  /**
   * Opens the event with this id in the inspector.
   */
  readonly onSelect: (id: string) => void;

  /**
   * The message filter, highlighted inside rows. Deferred, so typing stays responsive.
   */
  readonly highlight: string;

  /**
   * Time zone of the timestamps.
   */
  readonly zone: TimeZoneMode;

  /**
   * Row density.
   */
  readonly density: Density;

  /**
   * Whether messages wrap onto several lines, which makes rows measured instead of fixed.
   */
  readonly wrap: boolean;

  /**
   * Pinned attribute keys, shown as extra columns.
   */
  readonly columns: ReadonlyArray<string>;

  /**
   * Plain-text descriptions of the active filters, repeated in the empty state.
   */
  readonly filterSummary: ReadonlyArray<string>;

  /**
   * Description of the time range, repeated in the empty state.
   */
  readonly rangeSummary: string;

  /**
   * Removes every filter. Omitted when there are none.
   */
  readonly onClearFilters: (() => void) | undefined;

  /**
   * Widens the time range. Omitted when it is already the widest.
   */
  readonly onWiden: (() => void) | undefined;

  /**
   * Runs the failed first request again.
   */
  readonly onRetry: () => void;

  /**
   * Narrows the time range after a too-large or too-slow query.
   */
  readonly onNarrow: () => void;

  /**
   * Asks for the failed older page again, without touching the pages already loaded.
   */
  readonly onRetryNextPage: () => void;

  /**
   * Starts the search over, for a cursor the server refused.
   */
  readonly onRestart: () => void;
};

/**
 * Virtualised table of the loaded log events, newest first.
 *
 * Only the visible rows and a small overscan exist in the DOM. Rows have a fixed height unless
 * wrapping is on, pages are never concatenated, and the next page is requested when the viewport
 * comes within 100 rows of the end. The scroll area is an ARIA grid, with the total row count so
 * assistive technology knows the list is longer than what is rendered.
 */
export function LogStreamTable(props: LogStreamTableProps) {
  // The compiler already skips components that call `useVirtualizer` (it reports "incompatible
  // library": the instance is mutable and memoising its output would show stale rows). The
  // directive makes that explicit and keeps it so if the compiler's policy changes.
  'use no memo';
  const { pages, wrap, density, columns, zone } = props;
  const scrollRef = useRef<HTMLDivElement | null>(null);
  const pendingMove = useRef<PendingMove | null>(null);
  const offsets = pageOffsets(pages);
  const count = totalRows(offsets);
  const rowHeight = ROW_HEIGHT_PX[density];
  const highlight = useDeferredValue(props.highlight);
  const template = columnTemplate(columns.length);

  // oxlint-disable-next-line react/incompatible-library, react-hooks-js/incompatible-library -- this component opts out with 'use no memo' above, so the compiler never memoises the virtualizer's output
  const virtualizer = useVirtualizer({
    count,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => (wrap ? rowHeight * 2 : rowHeight),
    overscan: OVERSCAN_ROWS,
    getItemKey: (row) => eventAt(pages, offsets, row)?.id ?? row,
    useScrollendEvent: true,
  });
  const items = virtualizer.getVirtualItems();
  const lastRow = items[items.length - 1]?.index ?? -1;
  const firstEvent = eventAt(pages, offsets, items[0]?.index ?? 0);

  useEffect(() => {
    virtualizer.measure();
  }, [virtualizer, wrap, rowHeight]);

  const { canFetchMore, fetchNextPage } = props;
  useEffect(() => {
    if (lastRow >= count - PREFETCH_ROWS && canFetchMore) {
      fetchNextPage();
    }
  }, [lastRow, count, canFetchMore, fetchNextPage]);

  /**
   * Moves the selection with `j` and `k` unless the user is typing in a field. An effect event, so
   * the listener below is added once and still calls the latest `moveSelection`.
   */
  const onKeyDown = useEffectEvent((event: globalThis.KeyboardEvent) => {
    const delta = keyDelta(event);
    if (delta !== 0 && !isTypingTarget(event.target)) {
      event.preventDefault();
      moveSelection(delta);
    }
  });

  useEffect(() => {
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, []);

  /**
   * Selects the row `delta` rows from the selected one (or the first, when none is selected) and
   * scrolls it into view.
   */
  const moveSelection = (delta: number) => {
    if (count === 0) {
      return;
    }

    const focusWasInGrid = scrollRef.current?.contains(document.activeElement) ?? false;

    // The selection lives in the URL, so a key press changes it a moment before the new value
    // arrives as a prop. Holding `j` presses faster than that, so a press made while the last one
    // is still on its way counts from the row the last one asked for, not from the stale prop.
    const pending = pendingMove.current;
    const useAsked =
      pending !== null &&
      Date.now() - pending.at < PENDING_MOVE_MS &&
      pending.id !== props.selectedId;
    const shownRow =
      props.selectedId === undefined ? -1 : rowOfEvent(pages, offsets, props.selectedId);
    const current = useAsked ? pending.row : shownRow;
    const next = Math.min(count - 1, Math.max(0, current + delta));
    const target = eventAt(pages, offsets, next);
    if (target !== undefined) {
      pendingMove.current = { id: target.id, row: next, at: Date.now() };
      props.onSelect(target.id);
      virtualizer.scrollToIndex(next);
      if (focusWasInGrid) {
        focusRowWhenRendered(scrollRef.current, next);
      }
    }
  };

  /**
   * Handles arrow keys, Home and End while a row has focus.
   */
  const onGridKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    const delta = arrowDelta(event.key);
    if (delta !== 0) {
      event.preventDefault();
      moveSelection(delta);
    }
  };

  if (props.status === 'error') {
    return (
      <TableShell>
        <ErrorPanel error={props.error} onRetry={props.onRetry} onNarrow={props.onNarrow} />
      </TableShell>
    );
  }

  return (
    <TableShell>
      <TableHeader template={template} columns={columns} zone={zone} />
      {props.status === 'pending' ? (
        <SkeletonRows rowHeight={rowHeight} template={template} />
      ) : null}
      {props.status === 'success' && count === 0 ? <EmptyState {...props} /> : null}
      {count > 0 ? (
        <div
          ref={scrollRef}
          role="grid"
          aria-label="Log events"
          aria-rowcount={count + 1}
          aria-busy={props.stale || props.fetchingMore}
          tabIndex={0}
          onKeyDown={onGridKeyDown}
          className={cn(
            'relative flex-1 overflow-x-hidden overflow-y-auto outline-none focus-visible:ring-1 focus-visible:ring-ring',
            props.stale && 'opacity-60',
          )}>
          <div className="sticky top-0 z-10 h-0">
            {firstEvent === undefined ? null : (
              <div className="pointer-events-none flex justify-end px-3 pt-1">
                <span className="rounded border border-border bg-popover/90 px-1.5 py-0.5 font-mono text-[10px] text-muted-foreground shadow-sm">
                  {formatDay(firstEvent.event_time, zone)}
                </span>
              </div>
            )}
          </div>
          <div style={{ height: virtualizer.getTotalSize() }} className="relative w-full">
            {items.map((item) => {
              const event = eventAt(pages, offsets, item.index);
              if (event === undefined) {
                return null;
              }

              const previous = eventAt(pages, offsets, item.index - 1);
              return (
                <LogRow
                  key={item.key}
                  event={event}
                  row={item.index}
                  start={item.start}
                  size={wrap ? undefined : rowHeight}
                  measure={wrap ? virtualizer.measureElement : undefined}
                  selected={event.id === props.selectedId}
                  dayStart={
                    previous !== undefined &&
                    dayKey(previous.event_time, zone) !== dayKey(event.event_time, zone)
                  }
                  zone={zone}
                  highlight={highlight}
                  columns={columns}
                  template={template}
                  wrap={wrap}
                  onSelect={props.onSelect}
                />
              );
            })}
          </div>
        </div>
      ) : null}
      <TableFooter
        count={count}
        nextPageError={props.nextPageError}
        onRetryNextPage={props.onRetryNextPage}
        onRestart={props.onRestart}
        onNarrow={props.onNarrow}
        fetchingMore={props.fetchingMore}
        stale={props.stale}
        capReached={
          count >= MAX_LOADED_EVENTS && !props.canFetchMore && !props.fetchingMore && hasMore(pages)
        }
      />
    </TableShell>
  );
}

/**
 * Moves the DOM focus to a row once it exists. The row may have to be scrolled into the rendered
 * window first, so this waits for up to a few frames instead of assuming it is already there.
 */
function focusRowWhenRendered(grid: HTMLElement | null, row: number, attempts = 5): void {
  requestAnimationFrame(() => {
    const element = grid?.querySelector<HTMLElement>(`[data-index="${row}"]`);
    if (element !== null && element !== undefined) {
      element.focus({ preventScroll: true });
    } else if (attempts > 0) {
      focusRowWhenRendered(grid, row, attempts - 1);
    }
  });
}

/**
 * Whether the last loaded page has a cursor, that is, older events exist.
 */
function hasMore(pages: ReadonlyArray<SearchResult>): boolean {
  return pages[pages.length - 1]?.next_cursor !== null;
}

/**
 * Maps a key to a selection step: `j` is down, `k` is up, anything else is none.
 */
function keyDelta(event: globalThis.KeyboardEvent): number {
  if (hasCommandModifier(event)) {
    return 0;
  }

  if (event.key === 'j') {
    return 1;
  }

  return event.key === 'k' ? -1 : 0;
}

/**
 * Maps an arrow key to a selection step.
 */
function arrowDelta(key: string): number {
  if (key === 'ArrowDown') {
    return 1;
  }

  return key === 'ArrowUp' ? -1 : 0;
}

/**
 * CSS grid columns shared by the header, the skeleton and the rows: time, level, service, message
 * and one fixed column per pinned attribute.
 */
function columnTemplate(pinned: number): string {
  const fixed = '92px 36px 120px minmax(0,1fr)';
  return pinned === 0 ? fixed : `${fixed} repeat(${pinned}, ${PINNED_COLUMN_PX}px)`;
}

/**
 * Frame of the table area.
 */
function TableShell({ children }: { children: ReactNode }) {
  return (
    <div className="flex min-w-0 flex-1 flex-col overflow-hidden bg-background">{children}</div>
  );
}

/**
 * Column titles, aligned with the rows by the shared grid template.
 */
function TableHeader(props: {
  template: string;
  columns: ReadonlyArray<string>;
  zone: TimeZoneMode;
}) {
  // The table is an ARIA grid on divs, not a `<table>`: rows are absolutely positioned by the
  // virtualizer, which table layout cannot do. The roles are the point, so the "use a native
  // element" rule is off for the header and the row component.
  /* oxlint-disable jsx-a11y/prefer-tag-over-role */
  return (
    <div
      role="row"
      aria-rowindex={1}
      style={{ gridTemplateColumns: props.template, height: HEADER_HEIGHT_PX }}
      className="grid shrink-0 items-center gap-3 border-b border-border bg-card/40 px-3 font-mono text-[10px] text-quiet select-none">
      <div role="columnheader">{props.zone === 'utc' ? 'Time (UTC)' : 'Time (local)'}</div>
      <div role="columnheader">Lvl</div>
      <div role="columnheader">Service</div>
      <div role="columnheader">Message</div>
      {props.columns.map((key) => (
        <div key={key} role="columnheader" className="truncate">
          {key}
        </div>
      ))}
    </div>
  );
  /* oxlint-enable jsx-a11y/prefer-tag-over-role */
}

/**
 * Placeholder rows with the real row height, so the table does not move when data arrives.
 */
function SkeletonRows({ rowHeight, template }: { rowHeight: number; template: string }) {
  const rows = Array.from({ length: SKELETON_ROWS }, (_, index) => index);
  return (
    <div className="flex-1 overflow-hidden" aria-busy="true" aria-label="Loading events">
      {rows.map((row) => (
        <div
          key={row}
          style={{ height: rowHeight, gridTemplateColumns: template }}
          className="grid items-center gap-3 px-3">
          <div className="h-2.5 animate-pulse rounded-sm bg-muted/70 motion-reduce:animate-none" />
          <div className="h-2.5 w-6 animate-pulse rounded-sm bg-muted/70 motion-reduce:animate-none" />
          <div className="h-2.5 animate-pulse rounded-sm bg-muted/70 motion-reduce:animate-none" />
          <div
            style={{ width: `${40 + ((row * 37) % 50)}%` }}
            className="h-2.5 animate-pulse rounded-sm bg-muted/70 motion-reduce:animate-none"
          />
        </div>
      ))}
    </div>
  );
}

/**
 * Shown when the query succeeded with no events. It repeats the range and filters in effect and
 * offers to widen or clear them.
 */
function EmptyState(props: LogStreamTableProps) {
  return (
    <div className="flex flex-1 flex-col items-center justify-center gap-2 px-6 py-16 text-center">
      <p className="font-mono text-xs text-foreground">No matching events</p>
      <p className="max-w-md text-[11px] text-muted-foreground">
        Nothing in {props.rangeSummary}
        {props.filterSummary.length > 0 ? ' with these filters:' : '.'}
      </p>
      {props.filterSummary.length > 0 ? (
        <ul className="flex flex-wrap justify-center gap-1">
          {props.filterSummary.map((text) => (
            <li
              key={text}
              className="rounded border border-border bg-secondary/50 px-1.5 py-0.5 font-mono text-[10px] text-muted-foreground">
              {text}
            </li>
          ))}
        </ul>
      ) : null}
      <div className="mt-2 flex items-center gap-2">
        {props.onWiden === undefined ? null : (
          <button
            type="button"
            onClick={props.onWiden}
            className="cursor-pointer rounded border border-border px-2 py-1 font-mono text-[11px] text-foreground hover:bg-accent">
            Widen the time range
          </button>
        )}
        {props.onClearFilters === undefined ? null : (
          <button
            type="button"
            onClick={props.onClearFilters}
            className="cursor-pointer rounded border border-border px-2 py-1 font-mono text-[11px] text-foreground hover:bg-accent">
            Clear filters
          </button>
        )}
      </div>
    </div>
  );
}

/**
 * Status line under the table: the loaded row count, a loading hint, the failure of an older page,
 * and the banner shown when the 50,000-row cap stops further paging.
 */
function TableFooter(props: {
  count: number;
  nextPageError: unknown;
  onRetryNextPage: () => void;
  onRestart: () => void;
  onNarrow: () => void;
  fetchingMore: boolean;
  stale: boolean;
  capReached: boolean;
}) {
  return (
    <div className="shrink-0 border-t border-border bg-card/40 font-mono text-[10px] text-quiet select-none">
      {props.capReached ? (
        <output className="block border-b border-border bg-amber-500/10 px-3 py-1 text-amber-700 dark:text-[#d4973b]">
          Showing the newest {MAX_LOADED_EVENTS.toLocaleString('en-US')} events. Narrow the time
          range or add a filter to reach older ones.
        </output>
      ) : null}
      {props.nextPageError === null ? null : (
        <NextPageError
          error={props.nextPageError}
          onRetry={props.onRetryNextPage}
          onRestart={props.onRestart}
          onNarrow={props.onNarrow}
        />
      )}
      <div className="flex h-6 items-center justify-between px-3">
        <span>{props.count.toLocaleString('en-US')} rows loaded</span>
        <span aria-live="polite">
          {props.fetchingMore ? 'loading older events' : null}
          {props.stale ? 'updating' : null}
        </span>
      </div>
    </div>
  );
}

/**
 * Props of one row. All primitives or stable references, so a row re-renders only when its own data
 * changes: selecting an event re-renders the two rows involved.
 */
type LogRowProps = {
  /**
   * The event to draw.
   */
  readonly event: LogEvent;

  /**
   * Zero-based row index, for `aria-rowindex`.
   */
  readonly row: number;

  /**
   * Distance from the top of the list in pixels.
   */
  readonly start: number;

  /**
   * Fixed height in pixels, or `undefined` for a measured row.
   */
  readonly size: number | undefined;

  /**
   * Measuring callback for wrapped rows.
   */
  readonly measure: ((element: HTMLElement | null) => void) | undefined;

  /**
   * Whether the event is open in the inspector.
   */
  readonly selected: boolean;

  /**
   * Whether this row is the first of a new calendar day.
   */
  readonly dayStart: boolean;

  /**
   * Time zone of the timestamp.
   */
  readonly zone: TimeZoneMode;

  /**
   * Text to highlight in the message.
   */
  readonly highlight: string;

  /**
   * Pinned attribute keys.
   */
  readonly columns: ReadonlyArray<string>;

  /**
   * Grid template shared with the header.
   */
  readonly template: string;

  /**
   * Whether the message wraps.
   */
  readonly wrap: boolean;

  /**
   * Opens an event in the inspector.
   */
  readonly onSelect: (id: string) => void;
};

/**
 * One event: a coloured rail by severity, the time, the level, the service, the message with the
 * search match marked, and the pinned attribute values. Errors and above tint the whole row.
 *
 * Wrapped in `memo`: the table re-renders with every selection change and makes a new element for
 * each of the roughly 35 visible rows, which the compiler cannot skip because it memoises inside a
 * component, not between a parent and its children. With `memo`, and props that are primitives or
 * stable references, only the rows whose `selected` changed render.
 */
const LogRow = memo(function LogRow(props: LogRowProps) {
  const { event, selected, wrap, measure } = props;
  const meta = getLevelMeta(event.level);
  // ARIA grid on divs, see `TableHeader`.
  /* oxlint-disable jsx-a11y/prefer-tag-over-role */
  return (
    <div
      ref={measure}
      role="row"
      data-index={props.row}
      aria-rowindex={props.row + 2}
      aria-selected={selected}
      tabIndex={selected ? 0 : -1}
      onClick={() => props.onSelect(event.id)}
      onKeyDown={(keyEvent) => {
        if (keyEvent.key === 'Enter') {
          props.onSelect(event.id);
        }
      }}
      style={{
        transform: `translateY(${props.start}px)`,
        gridTemplateColumns: props.template,
        ...(props.size === undefined ? {} : { height: props.size }),
      }}
      className={cn(
        'absolute top-0 left-0 grid w-full cursor-pointer gap-3 px-3 font-mono text-[12px] outline-none focus-visible:ring-1 focus-visible:ring-ring',
        wrap ? 'items-start py-1' : 'items-center',
        meta.rowAccentClass,
        event.level >= 50 && 'bg-rose-500/[0.05]',
        props.dayStart && 'border-t border-t-foreground/25',
        selected ? 'bg-secondary text-foreground' : 'hover:bg-accent/50',
      )}>
      <span role="gridcell" className="truncate text-[11px] text-quiet tabular-nums">
        {formatClock(event.event_time, props.zone)}
      </span>
      <span role="gridcell">
        <LevelBadge level={event.level} compact />
      </span>
      <span role="gridcell" className="truncate text-[11px] text-muted-foreground">
        {event.service ?? '-'}
      </span>
      <span
        role="gridcell"
        className={cn('min-w-0', wrap ? 'break-words whitespace-pre-wrap' : 'truncate')}>
        <Highlighted text={event.message} needle={props.highlight} />
      </span>
      {props.columns.map((key) => (
        <span
          key={key}
          role="gridcell"
          className="truncate text-[11px] text-muted-foreground tabular-nums">
          {scalarText(event.attributes[key])}
        </span>
      ))}
    </div>
  );
  /* oxlint-enable jsx-a11y/prefer-tag-over-role */
});

/**
 * Draws a message with the occurrences of `needle` marked.
 */
function Highlighted({ text, needle }: { text: string; needle: string }) {
  if (needle === '') {
    return text;
  }

  let offset = 0;
  return splitHighlight(text, needle).map((segment) => {
    const key = offset;
    offset += segment.text.length;
    return segment.match ? (
      <mark key={key} className="rounded-xs bg-amber-400/30 text-inherit">
        {segment.text}
      </mark>
    ) : (
      segment.text
    );
  });
}

/**
 * Text for an attribute cell: scalars as they are, objects as a short marker, missing as empty.
 */
function scalarText(value: unknown): string {
  if (value === undefined || value === null) {
    return '';
  }

  if (typeof value === 'object') {
    return Array.isArray(value) ? `[${value.length}]` : '{...}';
  }

  return typeof value === 'string' ? value : JSON.stringify(value);
}
