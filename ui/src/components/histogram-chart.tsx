import type { KeyboardEvent, PointerEvent, RefCallback } from 'react';

import { ZoomOut } from 'lucide-react';
import { useState } from 'react';

import type { TimeZoneMode } from '@/lib/explorer/format';
import type { HistogramKeyState } from '@/lib/explorer/histogram-keys';
import type { HistogramBucket, TimeRange } from '@/lib/logbrook/types';

import { describeError } from '@/lib/explorer/describe-error';
import { formatClock, formatDateTime } from '@/lib/explorer/format';
import { applyHistogramKey, hasKeySelection, selectedBuckets } from '@/lib/explorer/histogram-keys';
import { bucketsToRange, selectionToRange } from '@/lib/explorer/range';

/**
 * Height of the chart in pixels. Fixed, so the page does not move when the data arrives.
 */
const CHART_HEIGHT_PX = 56;

/**
 * Height of the thin baseline drawn for an empty bucket, in pixels.
 */
const EMPTY_BAR_PX = 1;

/**
 * Where the chart sits in the page: the data to draw, how it was requested, and what the user can
 * do with it.
 */
type HistogramChartProps = {
  /**
   * The frozen range the chart covers.
   */
  readonly range: TimeRange;

  /**
   * Bucket duration in milliseconds.
   */
  readonly intervalMs: number;

  /**
   * Zero-filled buckets of all matching events, one per interval slot. `undefined` while the first
   * response is on its way.
   */
  readonly buckets: ReadonlyArray<HistogramBucket> | undefined;

  /**
   * Zero-filled buckets of events at level 50 and above, drawn over the total.
   */
  readonly errorBuckets: ReadonlyArray<HistogramBucket> | undefined;

  /**
   * Number of matching events: the sum of `buckets`.
   */
  readonly total: number | null;

  /**
   * The histogram request's error, or `null`.
   */
  readonly error: unknown;

  /**
   * Whether the bars belong to the previous query and a new one is loading.
   */
  readonly stale: boolean;

  /**
   * Whether the whole query is failing and the table already shows the error. The chart then shows
   * a quiet "no data" instead of a loading animation or a second copy of the message.
   */
  readonly unavailable: boolean;

  /**
   * Time zone of the labels.
   */
  readonly zone: TimeZoneMode;

  /**
   * Called with the range the user dragged over.
   */
  readonly onZoom: (range: TimeRange) => void;

  /**
   * Called when the user asks to see twice the span.
   */
  readonly onZoomOut: () => void;

  /**
   * Width of the chart area in pixels. The parent measures it, because it also needs the width to
   * pick the bucket interval before the data exists.
   */
  readonly width: number;

  /**
   * Ref the parent's width measurement observes; it is attached to the chart area.
   */
  readonly containerRef: RefCallback<HTMLDivElement>;
};

/**
 * A drag in progress, in pixels from the chart's left edge.
 */
type Drag = {
  /**
   * Where the pointer went down.
   */
  readonly fromPx: number;

  /**
   * Where the pointer is now.
   */
  readonly toPx: number;
};

/**
 * Small SVG bar chart of the matching events over the range, with the errors drawn over the total.
 * The bar count follows the chart width. Dragging across the bars zooms the explorer to that span;
 * the header shows the total, or the bucket under the pointer.
 *
 * The chart is also operable from the keyboard. Tab focuses it; Left and Right move a cursor from
 * bucket to bucket (Home and End jump to the ends), Shift with those keys extends a selection,
 * Enter zooms to the selection (or to the cursor's bucket), and Escape clears it. The header, which
 * is a polite live region, reads out the bucket or the selection, so a screen reader announces
 * every step, and the chart draws the cursor and the selection the way it draws a drag.
 */
export function HistogramChart(props: HistogramChartProps) {
  const {
    range,
    intervalMs,
    buckets,
    errorBuckets,
    total,
    error,
    stale,
    unavailable,
    zone,
    width,
    containerRef,
  } = props;
  const [drag, setDrag] = useState<Drag | null>(null);
  const [hovered, setHovered] = useState<number | null>(null);
  const [keys, setKeys] = useState<HistogramKeyState | null>(null);

  const slots = buckets?.length ?? 0;
  const slotPx = slots > 0 ? width / slots : 0;
  const peak = Math.max(1, ...(buckets ?? []).map((bucket) => bucket.count));
  const keyed = keys === null || slots === 0 ? null : clampKeys(keys, slots);
  const keySpan = keyed === null ? null : selectedBuckets(keyed);
  const shown = keyed === null ? hovered : keyed.cursor;
  const hoveredBucket = shown === null ? undefined : buckets?.[shown];
  const hoveredErrors = shown === null ? 0 : (errorBuckets?.[shown]?.count ?? 0);
  const keyedDescription = describeKeyed(keyed, keySpan, {
    buckets,
    errorBuckets,
    range,
    intervalMs,
    zone,
  });

  /**
   * Converts a pointer event to a position inside the chart.
   */
  const positionOf = (event: PointerEvent<SVGSVGElement>): number => {
    const box = event.currentTarget.getBoundingClientRect();
    return Math.min(width, Math.max(0, event.clientX - box.left));
  };

  /**
   * Starts a drag selection and captures the pointer so it ends even outside the chart.
   */
  const onPointerDown = (event: PointerEvent<SVGSVGElement>) => {
    event.currentTarget.setPointerCapture(event.pointerId);
    const x = positionOf(event);
    setDrag({ fromPx: x, toPx: x });
  };

  /**
   * Extends the drag, or updates the hovered bucket when none is in progress.
   */
  const onPointerMove = (event: PointerEvent<SVGSVGElement>) => {
    const x = positionOf(event);
    if (drag !== null) {
      setDrag({ fromPx: drag.fromPx, toPx: x });
    } else if (slotPx > 0) {
      setHovered(Math.min(slots - 1, Math.floor(x / slotPx)));
    }
  };

  /**
   * Abandons a drag the browser took away (a touch turned into a scroll, the pointer was captured
   * elsewhere) without zooming.
   */
  const onPointerCancel = () => setDrag(null);

  /**
   * Ends the drag and zooms when it covered more than a click.
   */
  const onPointerUp = (event: PointerEvent<SVGSVGElement>) => {
    if (drag === null) {
      return;
    }

    const selected = selectionToRange(
      { fromPx: drag.fromPx, toPx: positionOf(event) },
      { widthPx: width, range, intervalMs },
    );
    setDrag(null);
    if (selected !== null) {
      props.onZoom(selected);
    }
  };

  /**
   * Moves the keyboard cursor, extends or clears the selection, or zooms, as the key asks (see
   * `applyHistogramKey`). Keys it does not use keep their default and bubble, so Tab and the page
   * shortcuts work. Escape bubbles too unless there was something here to clear.
   */
  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.altKey || event.ctrlKey || event.metaKey) {
      return;
    }

    const action = applyHistogramKey(keyed, event.key, event.shiftKey, slots);
    if (action.kind === 'ignore') {
      return;
    }

    event.preventDefault();
    if (action.kind === 'state') {
      setKeys(action.state);
    } else if (action.kind === 'clear') {
      // The page also closes the event panel on Escape; this one belongs to the chart.
      event.nativeEvent.stopPropagation();
      setKeys(null);
    } else {
      setKeys(null);
      props.onZoom(bucketsToRange(action.first, action.last, { range, intervalMs }));
    }
  };

  return (
    <div className="border-b border-border bg-background px-4 py-2 select-none">
      <div className="mb-1.5 flex items-center justify-between font-mono text-[11px] text-muted-foreground">
        <div className="flex items-center gap-2 tabular-nums">
          <span>{formatClock(range.from, zone)}</span>
          <span className="text-quiet">to</span>
          <span>{formatClock(range.to, zone)}</span>
          <span className="text-quiet">{zone === 'utc' ? 'UTC' : 'local'}</span>
          <button
            type="button"
            onClick={props.onZoomOut}
            aria-label="Zoom out to twice the time span"
            title="Zoom out"
            className="inline-flex h-5 w-5 cursor-pointer items-center justify-center rounded text-muted-foreground hover:bg-accent hover:text-foreground">
            <ZoomOut className="h-3 w-3" />
          </button>
          <span className="text-quiet">
            drag across the bars to zoom, or Tab to them and use ← →
          </span>
        </div>

        <div className="tabular-nums" aria-live="polite">
          {keySpan !== null && hasKeySelection(keyed) ? (
            <SelectionReadout
              buckets={buckets}
              errorBuckets={errorBuckets}
              first={keySpan.first}
              last={keySpan.last}
              range={range}
              intervalMs={intervalMs}
              zone={zone}
            />
          ) : hoveredBucket === undefined ? (
            <TotalReadout total={total} stale={stale} unavailable={unavailable} />
          ) : (
            <span>
              <span className="text-quiet">{formatDateTime(hoveredBucket.time, zone)}</span>{' '}
              <span className="font-medium text-foreground">{hoveredBucket.count}</span>
              {hoveredErrors > 0 ? (
                <span className="ml-1.5 text-rose-500 dark:text-[#e05263]">
                  {hoveredErrors} err
                </span>
              ) : null}
            </span>
          )}
        </div>
      </div>

      <div
        ref={containerRef}
        className="relative"
        style={{ height: CHART_HEIGHT_PX }}
        aria-busy={buckets === undefined && !unavailable}>
        {buckets === undefined ? (
          <div
            className={`h-full rounded-sm bg-muted/50 ${
              unavailable ? '' : 'animate-pulse motion-reduce:animate-none'
            }`}
          />
        ) : (
          <div
            // oxlint-disable-next-line jsx-a11y/prefer-tag-over-role -- an `<input type="range">` has no way to select a span of buckets or to read out their counts; this is a slider over buckets with its own keys
            role="slider"
            tabIndex={0}
            aria-roledescription="time range selector"
            aria-label="Events over time"
            aria-orientation="horizontal"
            aria-valuemin={0}
            aria-valuemax={slots - 1}
            aria-valuenow={keyed?.cursor ?? slots - 1}
            aria-valuetext={keyedDescription}
            aria-keyshortcuts="ArrowLeft ArrowRight Shift+ArrowLeft Shift+ArrowRight Home End Enter Escape"
            onKeyDown={onKeyDown}
            onBlur={() => setKeys(null)}
            className="rounded-sm outline-none focus-visible:ring-1 focus-visible:ring-ring">
            <svg
              width={width}
              height={CHART_HEIGHT_PX}
              // oxlint-disable-next-line jsx-a11y/prefer-tag-over-role -- an `<svg>` cannot be replaced by `<img>`: the bars inside it are drawn and dragged here
              role="img"
              aria-label={`Events over time, ${total ?? 0} in total`}
              onPointerDown={onPointerDown}
              onPointerMove={onPointerMove}
              onPointerUp={onPointerUp}
              onPointerCancel={onPointerCancel}
              onPointerLeave={() => setHovered(null)}
              className={`cursor-crosshair touch-none ${stale ? 'opacity-50' : ''}`}>
              {buckets.map((bucket, slot) => (
                <Bar
                  key={bucket.time}
                  x={slot * slotPx}
                  width={slotPx}
                  total={bucket.count}
                  errors={errorBuckets?.[slot]?.count ?? 0}
                  peak={peak}
                  active={hovered === slot}
                />
              ))}
              {drag === null ? null : (
                <rect
                  x={Math.min(drag.fromPx, drag.toPx)}
                  y={0}
                  width={Math.abs(drag.toPx - drag.fromPx)}
                  height={CHART_HEIGHT_PX}
                  className="fill-foreground/10 stroke-foreground/40"
                />
              )}
              {keySpan === null || !hasKeySelection(keyed) ? null : (
                <rect
                  data-testid="histogram-key-selection"
                  x={keySpan.first * slotPx}
                  y={0}
                  width={(keySpan.last - keySpan.first + 1) * slotPx}
                  height={CHART_HEIGHT_PX}
                  className="fill-foreground/10 stroke-foreground/40"
                />
              )}
              {keyed === null ? null : (
                <rect
                  data-testid="histogram-key-cursor"
                  x={keyed.cursor * slotPx}
                  y={0}
                  width={slotPx}
                  height={CHART_HEIGHT_PX}
                  className="fill-none stroke-foreground"
                />
              )}
            </svg>
          </div>
        )}
        {error === null || error === undefined ? null : <ChartError error={error} />}
      </div>
    </div>
  );
}

/**
 * The keyboard state with its cursor and anchor held inside the `slots` buckets there are now, in
 * case the chart got fewer buckets (a narrower window, a new range) since the last key press.
 */
function clampKeys(state: HistogramKeyState, slots: number): HistogramKeyState {
  const last = slots - 1;
  return {
    cursor: Math.min(state.cursor, last),
    anchor: state.anchor === null ? null : Math.min(state.anchor, last),
  };
}

/**
 * The sentence a screen reader gets for the slider: the bucket under the cursor, or the selection,
 * with its time span and counts, and what Enter does. Before any key press it only says how to
 * start.
 */
function describeKeyed(
  keyed: HistogramKeyState | null,
  span: { first: number; last: number } | null,
  chart: {
    buckets: ReadonlyArray<HistogramBucket> | undefined;
    errorBuckets: ReadonlyArray<HistogramBucket> | undefined;
    range: TimeRange;
    intervalMs: number;
    zone: TimeZoneMode;
  },
): string {
  const { buckets, errorBuckets, range, intervalMs, zone } = chart;
  if (keyed === null || span === null) {
    return 'Use the arrow keys to move between time buckets';
  }

  const times = bucketsToRange(span.first, span.last, { range, intervalMs });
  const events = sumCounts(buckets, span.first, span.last);
  const errors = sumCounts(errorBuckets, span.first, span.last);
  const counts = `${events} events${errors > 0 ? `, ${errors} errors` : ''}`;
  if (!hasKeySelection(keyed)) {
    return `${formatDateTime(times.from, zone)}, ${counts}. Shift with the arrows selects, Enter zooms in`;
  }

  return `Selected ${formatDateTime(times.from, zone)} to ${formatDateTime(times.to, zone)}, ${counts}. Enter zooms in, Escape clears`;
}

/**
 * The chart header while the keyboard has a selection: its time span, the events and errors in it,
 * and what the keys do next.
 */
function SelectionReadout(props: {
  buckets: ReadonlyArray<HistogramBucket> | undefined;
  errorBuckets: ReadonlyArray<HistogramBucket> | undefined;
  first: number;
  last: number;
  range: TimeRange;
  intervalMs: number;
  zone: TimeZoneMode;
}) {
  const { buckets, errorBuckets, first, last, range, intervalMs, zone } = props;
  const span = bucketsToRange(first, last, { range, intervalMs });
  const count = sumCounts(buckets, first, last);
  const errors = sumCounts(errorBuckets, first, last);
  return (
    <span>
      <span className="text-quiet">
        selected {formatDateTime(span.from, zone)} to {formatDateTime(span.to, zone)}
      </span>{' '}
      <span className="font-medium text-foreground">{count}</span> events
      {errors > 0 ? (
        <span className="ml-1.5 text-rose-500 dark:text-[#e05263]">{errors} err</span>
      ) : null}
      <span className="ml-1.5 text-quiet">Enter to zoom, Esc to clear</span>
    </span>
  );
}

/**
 * Sum of the counts of buckets `first` to `last`, inclusive.
 */
function sumCounts(
  buckets: ReadonlyArray<HistogramBucket> | undefined,
  first: number,
  last: number,
): number {
  return (buckets ?? []).slice(first, last + 1).reduce((sum, bucket) => sum + bucket.count, 0);
}

/**
 * The total in the chart header: the number of events, or a placeholder while it loads.
 */
function TotalReadout(props: { total: number | null; stale: boolean; unavailable: boolean }) {
  const { total, stale } = props;
  if (total === null) {
    return <span className="text-quiet">{props.unavailable ? 'no data' : 'counting events'}</span>;
  }

  return (
    <span className={stale ? 'text-quiet' : ''}>
      <span className="font-medium text-foreground">{total.toLocaleString('en-US')}</span> events
    </span>
  );
}

/**
 * Message laid over the chart when its request failed.
 */
function ChartError({ error }: { error: unknown }) {
  const description = describeError(error);
  return (
    <div
      role="alert"
      className="absolute inset-0 flex items-center justify-center bg-background/80 px-3 text-center font-mono text-[11px] text-muted-foreground">
      Histogram unavailable: {description.title.toLowerCase()}
      {description.serverMessage === null ? '' : ` (${description.serverMessage})`}
    </div>
  );
}

/**
 * One bucket: a muted bar for the total with a red bar for its errors over it. An empty bucket is a
 * one-pixel baseline so the time axis stays readable.
 */
function Bar(props: {
  x: number;
  width: number;
  total: number;
  errors: number;
  peak: number;
  active: boolean;
}) {
  const { x, width, total, errors, peak, active } = props;
  const gap = width > 3 ? 1 : 0;
  const barWidth = Math.max(1, width - gap);
  const totalHeight = total === 0 ? EMPTY_BAR_PX : Math.max(2, (total / peak) * CHART_HEIGHT_PX);
  const errorHeight = errors === 0 ? 0 : Math.max(2, (errors / peak) * CHART_HEIGHT_PX);
  return (
    <g>
      <rect
        x={x}
        y={CHART_HEIGHT_PX - totalHeight}
        width={barWidth}
        height={totalHeight}
        className={active ? 'fill-foreground/60' : 'fill-muted-foreground/35'}
      />
      {errorHeight === 0 ? null : (
        <rect
          x={x}
          y={CHART_HEIGHT_PX - errorHeight}
          width={barWidth}
          height={errorHeight}
          className="fill-[#e05263]"
        />
      )}
    </g>
  );
}
