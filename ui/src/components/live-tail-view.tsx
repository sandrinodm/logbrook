import type { TailStatus } from '@/lib/logbrook/tail';
import type { TailSearch } from '@/lib/logbrook/tail-search';

import { TailList } from '@/components/tail-list';
import { TailNoticeBanner } from '@/components/tail-notice';
import { useTailSnapshot } from '@/lib/logbrook/use-tail';

/**
 * Props of `LiveTailView`.
 */
type LiveTailViewProps = {
  /**
   * The tail's search params.
   */
  search: TailSearch;

  /**
   * The index being followed, or `null` while the index list loads.
   */
  index: string | null;
};

/**
 * Wording and dot colour for a connection status.
 */
function statusLook(status: TailStatus): { label: string; dot: string } {
  switch (status) {
    case 'idle':
      return { label: 'Not connected', dot: 'bg-quiet' };
    case 'connecting':
      return { label: 'Connecting', dot: 'animate-pulse bg-quiet' };
    case 'live':
      return { label: 'Live', dot: 'bg-emerald-500' };
    case 'reconnecting':
      return { label: 'Reconnecting', dot: 'animate-pulse bg-amber-500' };
    case 'suspended':
      return { label: 'Paused while the tab was hidden', dot: 'bg-quiet' };
    case 'stopped':
      return { label: 'Stopped', dot: 'bg-[#e05263]' };
  }
}

/**
 * The live tail below the filter bar: connection status, any notice, and the event list. This is
 * the only component that subscribes to the tail store, so filter edits do not re-render with every
 * frame.
 */
export function LiveTailView({ search, index }: LiveTailViewProps) {
  const snapshot = useTailSnapshot();
  const { label, dot } = statusLook(snapshot.status);

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border px-4 py-1.5 font-mono text-xs">
        <span className="inline-flex items-center gap-2 text-foreground">
          <span className={`h-2 w-2 rounded-full ${dot}`} />
          <span>{label}</span>
          {index === null ? null : <span className="text-quiet">/indexes/{index}/logs/tail</span>}
        </span>
        <span className="text-[11px] text-quiet tabular-nums">
          {snapshot.events.length.toLocaleString()} buffered
          {snapshot.dropped > 0 ? `, ${snapshot.dropped.toLocaleString()} older dropped` : ''}
        </span>
      </div>
      {snapshot.notice !== null && index !== null ? (
        <TailNoticeBanner notice={snapshot.notice} search={search} index={index} />
      ) : null}
      <TailList
        snapshot={snapshot}
        emptyText={
          snapshot.status === 'live'
            ? 'Waiting for new events. The tail shows events ingested from now on.'
            : 'No events yet.'
        }
      />
    </div>
  );
}
