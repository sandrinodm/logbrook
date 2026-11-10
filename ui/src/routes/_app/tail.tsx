import { useQuery } from '@tanstack/react-query';
import { createFileRoute, useNavigate } from '@tanstack/react-router';
import { useEffect, useEffectEvent, useRef } from 'react';

import type { SearchPatch } from '@/lib/explorer/search';

import { DataViewSkeleton } from '@/components/data-view-skeleton';
import { LiveTailView } from '@/components/live-tail-view';
import { TailFilterBar } from '@/components/tail-filter-bar';
import { applyPatch, textOf } from '@/lib/explorer/search';
import { indexesQuery } from '@/lib/logbrook/queries';
import { resolveTailIndex, tailTargetOf, validateTailSearch } from '@/lib/logbrook/tail-search';
import { useTailConnection } from '@/lib/logbrook/use-tail';
import { hasCommandModifier, isEditableTarget, isTypingTarget } from '@/lib/shortcuts';

/**
 * The live tail. Client-only, like the explorer, so the stream never starts on the server. Filters
 * live in the URL and use the explorer's param names; changing one opens a new stream.
 */
export const Route = createFileRoute('/_app/tail')({
  validateSearch: validateTailSearch,
  ssr: false,
  pendingComponent: DataViewSkeleton,
  component: TailPage,
});

/**
 * Filter bar and live view. The page itself never subscribes to the tail store; only `LiveTailView`
 * does.
 */
function TailPage() {
  const navigate = useNavigate({ from: Route.fullPath });
  const search = Route.useSearch();
  const indexes = useQuery(indexesQuery());
  const names = indexes.data?.map((item) => item.name);
  const index = resolveTailIndex(textOf(search.index), names);
  useTailConnection(index === null ? null : tailTargetOf(search, index));
  const messageInputRef = useRef<HTMLInputElement | null>(null);

  /**
   * `/` focuses the message box and `Escape` leaves a field, as in the explorer. An effect event,
   * so the listener below is added once.
   */
  const onKeyDown = useEffectEvent((event: KeyboardEvent) => {
    if (event.key === '/' && !isTypingTarget(event.target) && !hasCommandModifier(event)) {
      event.preventDefault();
      messageInputRef.current?.focus();
    } else if (event.key === 'Escape' && isEditableTarget(event.target)) {
      (event.target as HTMLElement).blur();
    }
  });

  useEffect(() => {
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, []);

  /**
   * Writes a filter change to the URL, replacing the history entry so typing does not fill it.
   */
  const patch = (change: SearchPatch) => {
    void navigate({ search: (previous) => applyPatch(previous, change), replace: true });
  };

  return (
    <div className="flex min-h-0 flex-1 flex-col bg-background">
      <TailFilterBar
        search={search}
        indexNames={names}
        index={index}
        onPatch={patch}
        messageInputRef={messageInputRef}
      />
      {indexes.isError ? (
        <p role="alert" className="px-4 py-3 text-xs text-muted-foreground">
          Could not load the indexes: {indexes.error.message}
        </p>
      ) : null}
      <LiveTailView search={search} index={index} />
    </div>
  );
}
