import { useQuery } from '@tanstack/react-query';
import { useNavigate, useSearch } from '@tanstack/react-router';
import { Check, ChevronDown } from 'lucide-react';
import { useState } from 'react';

import type { IndexInfo } from '@/lib/logbrook/types';

import { BasePopover } from '@/components/ui/reui-base';
import { applyPatch, switchIndexPatch, textOf } from '@/lib/explorer/search';
import { hasIndexDetail } from '@/lib/logbrook/capabilities';
import { formatBytes, formatRetention } from '@/lib/logbrook/format';
import { indexesQuery } from '@/lib/logbrook/queries';
import { asRouteSearch } from '@/lib/views';

/**
 * Index shown when the URL has no `index` search param, which is the one every server has.
 */
const DEFAULT_INDEX = 'default';

/**
 * The index picker of the top bar. It lists the indexes the server's read token can reach, which
 * are the ones the explorer and tail can open, never the larger list an admin token sees. Picking
 * one writes the `index` search param of the current page, and the layout keeps that param when the
 * user moves between views.
 */
export function IndexSwitcher() {
  const navigate = useNavigate();
  const current =
    useSearch({ from: '/_app', select: (search) => textOf(search.index) }) ?? DEFAULT_INDEX;
  const list = useQuery(indexesQuery());
  const [open, setOpen] = useState(false);

  /**
   * Writes the chosen index into the search params and closes the popover.
   */
  const choose = (name: string) => {
    setOpen(false);
    void navigate({
      to: '.',
      search: asRouteSearch((previous) => applyPatch(previous, switchIndexPatch(name))),
    });
  };

  return (
    <BasePopover
      open={open}
      onOpenChange={setOpen}
      trigger={
        <button
          type="button"
          aria-label={`Index: ${current}`}
          className="inline-flex cursor-pointer items-center gap-1 rounded px-1.5 py-0.5 font-mono text-xs text-foreground hover:bg-accent">
          <span>{current}</span>
          <ChevronDown className="h-3 w-3 text-quiet" />
        </button>
      }
      className="w-64">
      <div className="space-y-0.5">
        <div className="px-2 py-1 font-mono text-[10px] text-quiet uppercase">
          Indexes{list.data === undefined ? '' : ` (${list.data.length})`}
        </div>
        {list.data === undefined ? (
          <SwitcherStatus failed={list.isError} onRetry={() => void list.refetch()} />
        ) : (
          list.data.map((index) => (
            <IndexOption
              key={index.name}
              index={index}
              selected={index.name === current}
              onChoose={choose}
            />
          ))
        )}
      </div>
    </BasePopover>
  );
}

/**
 * The popover body while there is no list: loading, or a failure with a retry.
 */
function SwitcherStatus({
  failed,
  onRetry,
}: {
  readonly failed: boolean;
  readonly onRetry: () => void;
}) {
  if (!failed) {
    return <p className="px-2 py-1.5 text-xs text-muted-foreground">Loading indexes…</p>;
  }

  return (
    <div className="space-y-1 px-2 py-1.5 text-xs">
      <p className="text-[#e05263]">Could not load the index list.</p>
      <button type="button" onClick={onRetry} className="cursor-pointer underline">
        Try again
      </button>
    </div>
  );
}

/**
 * One selectable index. Size and retention show when the read token receives them; a
 * source-restricted token gets the name alone.
 */
function IndexOption({
  index,
  selected,
  onChoose,
}: {
  readonly index: IndexInfo;
  readonly selected: boolean;
  readonly onChoose: (name: string) => void;
}) {
  return (
    <button
      type="button"
      onClick={() => onChoose(index.name)}
      className="flex w-full cursor-pointer items-center justify-between rounded px-2 py-1.5 text-left font-mono text-xs hover:bg-accent">
      <div>
        <div className="text-foreground">{index.name}</div>
        {hasIndexDetail(index) ? (
          <div className="text-[10px] text-quiet">
            {formatBytes(index.size_bytes)} • {formatRetention(index.retention_ms)}
          </div>
        ) : null}
      </div>
      {selected ? <Check className="h-3 w-3 text-foreground" /> : null}
    </button>
  );
}
