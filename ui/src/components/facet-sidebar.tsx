import { ChevronDown } from 'lucide-react';
import { useState } from 'react';

import type { FacetField } from '@/lib/explorer/search';
import type { Facets, FacetValue, LogFilters } from '@/lib/logbrook/types';

import { ErrorPanel } from '@/components/query-error-panel';
import { BaseCollapsible } from '@/components/ui/reui-base';
import { getLevelMeta } from '@/lib/utils';

/**
 * Values shown per facet before "show more".
 */
const COLLAPSED_VALUES = 8;

/**
 * What the sidebar shows and how a click changes the query.
 */
type FacetSidebarProps = {
  /**
   * The server's facets for the current range and filters, or `undefined` before the first
   * response.
   */
  readonly facets: Facets | undefined;

  /**
   * The facets request's error, or `null`.
   */
  readonly error: unknown;

  /**
   * Whether the whole query is failing and the table already shows the error. The sidebar then says
   * only that it has nothing to show, instead of a second copy of the message.
   */
  readonly unavailable: boolean;

  /**
   * Whether the facets belong to the previous query and a new one is loading.
   */
  readonly stale: boolean;

  /**
   * The active filters, to mark the selected values.
   */
  readonly filters: LogFilters;

  /**
   * Called when a value is clicked; the parent turns it into a URL change.
   */
  readonly onSelect: (field: FacetField, value: string) => void;

  /**
   * Runs the failed request again.
   */
  readonly onRetry: () => void;
};

/**
 * Left sidebar with the five facets the server computes for the filtered events: level, service,
 * source, logger and host. Bars are proportional to the largest count of their facet. Clicking a
 * value filters by it; clicking it again clears the filter.
 */
export function FacetSidebar({
  facets,
  error,
  unavailable,
  stale,
  filters,
  onSelect,
  onRetry,
}: FacetSidebarProps) {
  if (unavailable && facets === undefined) {
    return (
      <aside
        aria-label="Facets"
        className="w-56 shrink-0 border-r border-border bg-card/40 px-3 py-3 text-[11px] text-quiet">
        No facets while the query fails.
      </aside>
    );
  }

  return (
    <aside
      aria-label="Facets"
      aria-busy={facets === undefined || stale}
      className="w-56 shrink-0 overflow-y-auto border-r border-border bg-card/40 select-none">
      {error === null ? null : (
        <div className="px-2 py-3">
          <ErrorPanel error={error} onRetry={onRetry} />
        </div>
      )}
      <div className={stale ? 'opacity-60' : ''}>
        <FacetSection
          title="Level (min)"
          field="level"
          values={facets?.levels}
          active={filters.minLevel === undefined ? [] : [String(filters.minLevel)]}
          onSelect={onSelect}
        />
        <FacetSection
          title="Service"
          field="service"
          values={facets?.services}
          active={filters.service === undefined ? [] : [filters.service]}
          onSelect={onSelect}
        />
        <FacetSection
          title="Source"
          field="source"
          values={facets?.sources}
          active={filters.source ?? []}
          onSelect={onSelect}
        />
        <FacetSection
          title="Logger"
          field="logger"
          values={facets?.loggers}
          active={filters.logger === undefined ? [] : [filters.logger]}
          onSelect={onSelect}
        />
        <FacetSection
          title="Host"
          field="host"
          values={facets?.hosts}
          active={filters.host === undefined ? [] : [filters.host]}
          onSelect={onSelect}
        />
      </div>
    </aside>
  );
}

/**
 * One collapsible facet: its values with count bars, the active ones highlighted.
 */
function FacetSection(props: {
  title: string;
  field: FacetField;
  values: ReadonlyArray<FacetValue> | undefined;
  active: ReadonlyArray<string>;
  onSelect: (field: FacetField, value: string) => void;
}) {
  const [showAll, setShowAll] = useState(false);
  const { values } = props;
  const activeValues = new Set(props.active);
  const largest = Math.max(1, ...(values ?? []).map((item) => item.count));
  const shown = showAll ? values : values?.slice(0, COLLAPSED_VALUES);
  const hidden = (values?.length ?? 0) - (shown?.length ?? 0);

  return (
    <BaseCollapsible.Root defaultOpen className="border-b border-border/70 py-2">
      <BaseCollapsible.Trigger className="group flex w-full cursor-pointer items-center justify-between px-3 py-1 text-left text-[11px] font-medium text-muted-foreground hover:text-foreground">
        <span>{props.title}</span>
        <ChevronDown className="h-3 w-3 opacity-50 transition-transform duration-150 group-data-[panel-open]:rotate-180" />
      </BaseCollapsible.Trigger>

      <BaseCollapsible.Panel className="mt-1 space-y-0.5 px-1.5">
        {values === undefined ? <FacetPlaceholder /> : null}
        {values?.length === 0 ? <div className="px-2 py-1 text-[11px] text-quiet">None</div> : null}
        {shown?.map((item) => (
          <FacetRow
            key={item.value}
            item={item}
            largest={largest}
            field={props.field}
            selected={activeValues.has(item.value)}
            onSelect={props.onSelect}
          />
        ))}
        {hidden > 0 ? (
          <button
            type="button"
            onClick={() => setShowAll(true)}
            className="w-full cursor-pointer px-2 py-1 text-left font-mono text-[10px] text-quiet hover:text-foreground">
            Show {hidden} more
          </button>
        ) : null}
      </BaseCollapsible.Panel>
    </BaseCollapsible.Root>
  );
}

/**
 * Grey bars shown while the first facets response is on its way.
 */
function FacetPlaceholder() {
  return (
    <div className="space-y-1 px-2 py-1" aria-hidden="true">
      <div className="h-3 animate-pulse rounded-sm bg-muted/70 motion-reduce:animate-none" />
      <div className="h-3 w-2/3 animate-pulse rounded-sm bg-muted/70 motion-reduce:animate-none" />
    </div>
  );
}

/**
 * One facet value: its label, a bar behind it sized by count, and the count.
 */
function FacetRow(props: {
  item: FacetValue;
  largest: number;
  field: FacetField;
  selected: boolean;
  onSelect: (field: FacetField, value: string) => void;
}) {
  const { item, selected } = props;
  return (
    <button
      type="button"
      aria-pressed={selected}
      onClick={() => props.onSelect(props.field, item.value)}
      className={`relative flex w-full cursor-pointer items-center justify-between overflow-hidden rounded px-2 py-1 text-left font-mono text-[11px] transition-colors ${
        selected
          ? 'bg-secondary font-medium text-foreground'
          : 'text-muted-foreground hover:bg-accent/60 hover:text-foreground'
      }`}>
      <span
        aria-hidden="true"
        style={{ width: `${(item.count / props.largest) * 100}%` }}
        className="absolute inset-y-0 left-0 bg-foreground/[0.06]"
      />
      <span className="relative truncate">
        <FacetLabel field={props.field} value={item.value} />
      </span>
      <span className="relative ml-2 text-[10px] text-quiet tabular-nums">
        {item.count.toLocaleString('en-US')}
      </span>
    </button>
  );
}

/**
 * The label of a facet value. Levels show a coloured dot, the short name and the number.
 */
function FacetLabel({ field, value }: { field: FacetField; value: string }) {
  if (field !== 'level') {
    return value;
  }

  const meta = getLevelMeta(Number(value));
  return (
    <span className="inline-flex items-center gap-2">
      <span className={`h-1.5 w-1.5 rounded-full ${meta.dotClass}`} />
      <span className={meta.textClass}>{meta.shortLabel}</span>
      <span className="text-quiet">{value}</span>
    </span>
  );
}
