import { RefreshCw } from 'lucide-react';

import type { TimeZoneMode } from '@/lib/explorer/format';
import type { RelativeRange } from '@/lib/explorer/range';
import type { RefreshSeconds } from '@/lib/explorer/search';
import type { TimeRange } from '@/lib/logbrook/types';

import { formatDateTime } from '@/lib/explorer/format';
import { RELATIVE_RANGES } from '@/lib/explorer/range';
import { REFRESH_SECONDS } from '@/lib/explorer/search';

/**
 * The range and refresh state, and the callbacks that change it.
 */
type RangeControlsProps = {
  /**
   * The selected relative range, or `undefined` while an absolute range (a zoom) is active.
   */
  readonly relative: RelativeRange | undefined;

  /**
   * The frozen range the data was requested for.
   */
  readonly range: TimeRange;

  /**
   * Seconds between automatic refreshes, or `undefined` for none.
   */
  readonly autoRefresh: RefreshSeconds | undefined;

  /**
   * Time zone of the "as of" label.
   */
  readonly zone: TimeZoneMode;

  /**
   * Whether a request is running.
   */
  readonly busy: boolean;

  /**
   * Selects a relative range, which also freezes a new "now".
   */
  readonly onRange: (range: RelativeRange) => void;

  /**
   * Freezes a new "now" and reloads.
   */
  readonly onRefresh: () => void;

  /**
   * Sets the automatic refresh period; `undefined` turns it off.
   */
  readonly onAutoRefresh: (seconds: RefreshSeconds | undefined) => void;
};

/**
 * Time range presets, the frozen window, a refresh button and an auto-refresh period.
 *
 * A relative range is resolved to absolute bounds once per refresh, so paging cursors stay valid.
 * The label says when that moment was. After a zoom the range is absolute and refreshing it would
 * ask for the same data, so the refresh controls are disabled until a preset is chosen again.
 */
export function RangeControls(props: RangeControlsProps) {
  const { relative, range, zone } = props;
  const fixed = relative === undefined;
  return (
    <div className="flex flex-wrap items-center gap-2">
      <fieldset
        aria-label="Time range"
        className="inline-flex min-w-0 items-center rounded border border-border bg-secondary/40 p-0.5">
        {RELATIVE_RANGES.map((preset) => (
          <button
            key={preset}
            type="button"
            aria-pressed={preset === relative}
            onClick={() => props.onRange(preset)}
            className={`cursor-pointer rounded-xs px-1.5 py-0.5 font-mono text-[11px] transition-colors ${
              preset === relative
                ? 'bg-background font-medium text-foreground shadow-2xs'
                : 'text-muted-foreground hover:text-foreground'
            }`}>
            {preset}
          </button>
        ))}
      </fieldset>

      <span
        className="hidden font-mono text-[10px] text-quiet tabular-nums xl:inline"
        title={`${formatDateTime(range.from, zone)} to ${formatDateTime(range.to, zone)}`}>
        {fixed ? 'fixed range' : `as of ${formatDateTime(range.to, zone)}`}
      </span>

      <button
        type="button"
        onClick={props.onRefresh}
        disabled={fixed}
        aria-label="Refresh"
        title={fixed ? 'Pick a preset range to refresh' : 'Refresh: freeze a new "now"'}
        className="inline-flex h-6 w-6 cursor-pointer items-center justify-center rounded text-muted-foreground hover:bg-accent hover:text-foreground disabled:cursor-not-allowed disabled:opacity-40">
        <RefreshCw
          className={`h-3 w-3 ${props.busy ? 'animate-spin motion-reduce:animate-none' : ''}`}
        />
      </button>

      <select
        aria-label="Auto refresh"
        value={props.autoRefresh ?? 0}
        disabled={fixed}
        onChange={(event) => {
          const seconds = Number(event.target.value);
          props.onAutoRefresh(REFRESH_SECONDS.find((option) => option === seconds));
        }}
        className="h-6 rounded border border-border bg-background px-1 font-mono text-[11px] text-muted-foreground disabled:opacity-40">
        <option value={0}>Off</option>
        {REFRESH_SECONDS.map((seconds) => (
          <option key={seconds} value={seconds}>
            {seconds} s
          </option>
        ))}
      </select>
    </div>
  );
}
