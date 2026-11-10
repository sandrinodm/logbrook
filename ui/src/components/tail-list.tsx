import { useVirtualizer } from '@tanstack/react-virtual';
import { ArrowDown } from 'lucide-react';
import { useLayoutEffect, useRef, useState } from 'react';

import type { TailSnapshot } from '@/lib/logbrook/tail';
import type { LogEvent } from '@/lib/logbrook/types';

import { LevelBadge } from '@/components/ui/reui-base';
import { formatShortTimeUtc } from '@/lib/utils';

/**
 * Height of one row in pixels. Rows are one line and never measured, so scrolling costs the same at
 * 10,000 events as at ten. It equals the skeleton's row height.
 */
const ROW_HEIGHT_PX = 24;

/**
 * Rows rendered beyond the viewport on each side, so fast scrolling shows no blank band.
 */
const OVERSCAN_ROWS = 15;

/**
 * How close to the bottom, in pixels, still counts as "at the newest event".
 */
const EDGE_THRESHOLD_PX = 8;

/**
 * Props of `TailList`.
 */
type TailListProps = {
  /**
   * The live snapshot from the tail store.
   */
  snapshot: TailSnapshot;

  /**
   * Text for the empty state, which repeats what is being followed.
   */
  emptyText: string;
};

/**
 * The tail's event list: virtualized, one fixed-height line per event, newest at the bottom.
 *
 * While the reader is at the bottom the list follows the newest event. Scrolling up freezes the
 * view on the snapshot that was showing (snapshots are immutable, so keeping one is free) while the
 * store keeps buffering; a pill counts what arrived since and jumps back to the bottom. Rows are
 * keyed by event id, so they keep their identity as events arrive and old ones drop out.
 */
export function TailList({ snapshot, emptyText }: TailListProps) {
  // TanStack Virtual's `useVirtualizer` returns a mutable instance that React Compiler cannot
  // memoize safely; the compiler would skip this component anyway, so say so.
  'use no memo';

  const scrollRef = useRef<HTMLDivElement | null>(null);
  const [heldSnapshot, setFrozen] = useState<TailSnapshot | null>(null);
  // A snapshot kept while scrolled up belongs to one stream. When the tail is opened again for
  // another index or filter, or restarted, it shows the previous stream and `total` has started
  // over, so it is let go.
  const frozen = heldSnapshot?.epoch === snapshot.epoch ? heldSnapshot : null;
  if (heldSnapshot !== null && frozen === null) {
    setFrozen(null);
  }

  const shown = frozen ?? snapshot;
  const events = shown.events;
  // oxlint-disable-next-line react/incompatible-library, react-hooks-js/incompatible-library -- opted out above with 'use no memo'
  const virtualizer = useVirtualizer({
    count: events.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_HEIGHT_PX,
    overscan: OVERSCAN_ROWS,
    getItemKey: (index) => events[index]?.id ?? index,
    useScrollendEvent: true,
  });

  useLayoutEffect(() => {
    const element = scrollRef.current;
    if (frozen === null && element !== null) {
      element.scrollTop = element.scrollHeight;
    }
  }, [events, frozen]);

  /**
   * Freezes the view when the reader scrolls away from the bottom, and follows again when they come
   * back.
   */
  const handleScroll = () => {
    const element = scrollRef.current;
    if (element === null) {
      return;
    }

    const distance = element.scrollHeight - element.scrollTop - element.clientHeight;
    const atEdge = distance <= EDGE_THRESHOLD_PX;
    if (atEdge && frozen !== null) {
      setFrozen(null);
    } else if (!atEdge && frozen === null) {
      setFrozen(snapshot);
    }
  };

  const pending = Math.max(0, snapshot.total - shown.total);

  return (
    <div className="relative flex min-h-0 flex-1 flex-col">
      <div
        ref={scrollRef}
        role="log"
        aria-label="Live events"
        aria-live="off"
        // Focusable so the keyboard can scroll it (arrow keys, Page Up and Down, End).
        // oxlint-disable-next-line jsx-a11y/no-noninteractive-tabindex -- a scrollable region must be reachable without a mouse
        tabIndex={0}
        onScroll={handleScroll}
        className="min-h-0 flex-1 overflow-y-auto font-mono focus-visible:ring-1 focus-visible:ring-ring focus-visible:outline-none">
        {events.length === 0 ? (
          <p className="px-4 py-6 text-xs text-quiet">{emptyText}</p>
        ) : (
          <div style={{ height: virtualizer.getTotalSize() }} className="relative w-full">
            {virtualizer.getVirtualItems().map((item) => (
              <div
                key={item.key}
                style={{ height: ROW_HEIGHT_PX, transform: `translateY(${item.start}px)` }}
                className="absolute top-0 left-0 w-full">
                <TailRow event={events[item.index]} />
              </div>
            ))}
          </div>
        )}
      </div>
      {pending > 0 ? (
        <button
          type="button"
          onClick={() => setFrozen(null)}
          className="absolute bottom-4 left-1/2 inline-flex -translate-x-1/2 cursor-pointer items-center gap-1.5 rounded-full border border-border bg-foreground px-3 py-1 text-xs font-medium text-background shadow-md hover:opacity-90">
          <ArrowDown className="h-3 w-3" />
          {pending.toLocaleString()} new {pending === 1 ? 'event' : 'events'}
        </button>
      ) : null}
    </div>
  );
}

/**
 * One event on one line: time, level, service, message and source. Long messages are truncated.
 */
function TailRow({ event }: { event: LogEvent | undefined }) {
  if (event === undefined) {
    return null;
  }

  return (
    <div className="grid h-full grid-cols-[96px_40px_132px_1fr_auto] items-center gap-3 px-4 text-xs hover:bg-accent/50">
      <span className="text-[11px] text-quiet tabular-nums">
        {formatShortTimeUtc(event.event_time)}
      </span>
      <LevelBadge level={event.level} compact />
      <span className="truncate text-[11px] text-muted-foreground">{event.service ?? ''}</span>
      <span className="truncate text-foreground">{event.message}</span>
      <span className="text-[11px] text-quiet">{event.source}</span>
    </div>
  );
}
