import { Check, ChevronRight, Columns3, Copy, X } from 'lucide-react';
import * as React from 'react';

import type { TimeZoneMode } from '@/lib/explorer/format';
import type { AttributeCondition, LogEvent, LogFilters } from '@/lib/logbrook/types';

import { BaseCollapsible, BaseTabs, Button, LevelBadge } from '@/components/ui/reui-base';
import { buildSearchCurl, eventWindowParams } from '@/lib/explorer/curl';
import { formatDateTime } from '@/lib/explorer/format';

/**
 * Envelope fields that can become a filter. `pid` cannot: the API has no filter for it.
 */
type FilterableField = 'source' | 'service' | 'logger' | 'host';

/**
 * Which button was pressed last, to show "Copied" for a moment.
 */
type CopyTarget = 'json' | 'curl';

/**
 * What the drawer shows and how its actions change the explorer's URL.
 */
type EventInspectorDrawerProps = {
  /**
   * The event to show, or `undefined` when the id in the URL is not among the loaded rows.
   */
  readonly event: LogEvent | undefined;

  /**
   * Index the event belongs to, for the copied `curl` command.
   */
  readonly index: string;

  /**
   * Time zone of the timestamps.
   */
  readonly zone: TimeZoneMode;

  /**
   * Active filters, to mark values that are already filtered by.
   */
  readonly filters: LogFilters;

  /**
   * Pinned attribute columns, to mark attributes that already are one.
   */
  readonly columns: ReadonlyArray<string>;

  /**
   * Closes the drawer.
   */
  readonly onClose: () => void;

  /**
   * Filters by an envelope field's value.
   */
  readonly onFilterField: (field: FilterableField, value: string) => void;

  /**
   * Filters by a top-level attribute's value.
   */
  readonly onFilterAttribute: (condition: AttributeCondition) => void;

  /**
   * Pins an attribute as a table column, or unpins it.
   */
  readonly onToggleColumn: (key: string) => void;
};

/**
 * Side drawer for one event: the message and time, the envelope fields and attributes with "filter
 * by this value" and "add as column" actions, nested values as a collapsible tree, and the event as
 * JSON or as a `curl` command that fetches it.
 */
export function EventInspectorDrawer(props: EventInspectorDrawerProps) {
  const { event } = props;
  const [copied, setCopied] = React.useState<CopyTarget | null>(null);

  /**
   * Copies text to the clipboard and shows "Copied" on its button for a moment. A browser that
   * refuses clipboard access leaves the button unchanged.
   */
  const copy = (text: string, target: CopyTarget) => {
    navigator.clipboard.writeText(text).then(
      () => {
        setCopied(target);
        setTimeout(() => setCopied(null), 1600);
      },
      () => undefined,
    );
  };

  return (
    <aside
      aria-label="Event details"
      className="flex h-full w-[400px] shrink-0 flex-col overflow-hidden border-l border-border bg-card/30">
      <div className="flex items-center justify-between border-b border-border px-3.5 py-2">
        <span className="font-mono text-[11px] text-muted-foreground">Event</span>
        <button
          type="button"
          onClick={props.onClose}
          aria-label="Close inspector"
          className="cursor-pointer rounded p-1 text-muted-foreground hover:bg-accent hover:text-foreground">
          <X className="h-3.5 w-3.5" />
        </button>
      </div>

      {event === undefined ? (
        <p className="p-4 text-xs text-muted-foreground">
          This event is not among the loaded rows. It may be older than what has been loaded, or
          outside the current time range or filters.
        </p>
      ) : (
        <EventDetails {...props} event={event} copied={copied} onCopy={copy} />
      )}
    </aside>
  );
}

/**
 * The drawer body for a found event.
 */
function EventDetails(
  props: EventInspectorDrawerProps & {
    event: LogEvent;
    copied: CopyTarget | null;
    onCopy: (text: string, target: CopyTarget) => void;
  },
) {
  const { event, zone } = props;
  const lagMs = Math.max(0, event.received_at - event.event_time);
  const eventJson = JSON.stringify(event, null, 2);
  const curl = buildSearchCurl(props.index, eventWindowParams(event.event_time));
  const attributeEntries = Object.entries(event.attributes);
  const pinnedColumns = new Set(props.columns);

  return (
    <>
      <div className="border-b border-border px-3.5 py-2.5">
        <div className="flex items-center gap-2 font-mono text-xs">
          <LevelBadge level={event.level} showNumeric />
          <span className="truncate text-muted-foreground">{event.id}</span>
        </div>
        <p className="mt-2 font-mono text-xs leading-relaxed break-words whitespace-pre-wrap text-foreground">
          {event.message}
        </p>
        <div className="mt-2 flex items-center justify-between font-mono text-[10px] text-quiet tabular-nums">
          <span>{formatDateTime(event.event_time, zone)}</span>
          <span>received +{lagMs} ms</span>
        </div>
      </div>

      <BaseTabs.Root defaultValue="fields" className="flex flex-1 flex-col overflow-hidden">
        <BaseTabs.List className="flex gap-4 border-b border-border px-3">
          <TabButton value="fields">Fields ({attributeEntries.length})</TabButton>
          <TabButton value="json">JSON and curl</TabButton>
        </BaseTabs.List>

        <BaseTabs.Panel value="fields" className="flex-1 space-y-4 overflow-y-auto p-3.5">
          <Section title="Envelope">
            {(['source', 'service', 'logger', 'host'] as const).map((field) => (
              <EnvelopeRow
                key={field}
                field={field}
                value={event[field]}
                active={isFieldActive(field, event[field], props.filters)}
                onFilter={props.onFilterField}
              />
            ))}
            <div className="flex items-center justify-between py-1.5">
              <span className="text-muted-foreground">pid</span>
              <span className="text-foreground">{event.pid ?? 'null'}</span>
            </div>
          </Section>

          <Section title="Attributes" note="filters use op: eq">
            {attributeEntries.length === 0 ? (
              <div className="py-1.5 text-quiet">No attributes</div>
            ) : null}
            {attributeEntries.map(([key, value]) => (
              <AttributeRow
                key={key}
                name={key}
                value={value}
                filtered={isAttributeFiltered(key, value, props.filters)}
                pinned={pinnedColumns.has(key)}
                onFilter={props.onFilterAttribute}
                onToggleColumn={props.onToggleColumn}
              />
            ))}
          </Section>
        </BaseTabs.Panel>

        <BaseTabs.Panel value="json" className="flex-1 space-y-3 overflow-y-auto p-3.5">
          <CopyBlock
            title="Event as JSON"
            text={eventJson}
            copied={props.copied === 'json'}
            onCopy={() => props.onCopy(eventJson, 'json')}
          />
          <CopyBlock
            title="curl for this millisecond"
            text={curl}
            copied={props.copied === 'curl'}
            onCopy={() => props.onCopy(curl, 'curl')}
          />
        </BaseTabs.Panel>
      </BaseTabs.Root>
    </>
  );
}

/**
 * Whether an envelope value is already one of the active filters.
 */
function isFieldActive(field: FilterableField, value: string | null, filters: LogFilters): boolean {
  if (value === null) {
    return false;
  }

  return field === 'source' ? (filters.source ?? []).includes(value) : filters[field] === value;
}

/**
 * Whether a top-level attribute value is already filtered by, with the same type.
 */
function isAttributeFiltered(key: string, value: unknown, filters: LogFilters): boolean {
  return (filters.attributes ?? []).some((item) => item.key === key && item.value === value);
}

/**
 * Turns an attribute value into an equality condition, or `null` for values the API cannot filter
 * on: null, objects, arrays and non-finite numbers.
 */
function scalarCondition(key: string, value: unknown): AttributeCondition | null {
  const finite = typeof value === 'number' && Number.isFinite(value);
  if (typeof value === 'string' || typeof value === 'boolean' || finite) {
    return { key, op: 'eq', value };
  }

  return null;
}

/**
 * A tab label.
 */
function TabButton({ value, children }: { value: string; children: React.ReactNode }) {
  return (
    <BaseTabs.Tab
      value={value}
      className="cursor-pointer border-b-2 border-transparent py-2 text-[11px] font-medium text-muted-foreground data-[selected]:border-foreground data-[selected]:text-foreground">
      {children}
    </BaseTabs.Tab>
  );
}

/**
 * A titled group of rows.
 */
function Section(props: { title: string; note?: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="mb-1 flex items-center justify-between font-mono text-[10px] tracking-wider text-quiet uppercase">
        <span>{props.title}</span>
        {props.note === undefined ? null : <span>{props.note}</span>}
      </div>
      <div className="divide-y divide-border/50 border-t border-b border-border/60 font-mono text-[11px]">
        {props.children}
      </div>
    </div>
  );
}

/**
 * One envelope field with a filter action.
 */
function EnvelopeRow(props: {
  field: FilterableField;
  value: string | null;
  active: boolean;
  onFilter: (field: FilterableField, value: string) => void;
}) {
  const { field, value } = props;
  return (
    <div className="group flex items-center justify-between gap-2 py-1.5">
      <span className="text-muted-foreground">{field}</span>
      <div className="flex min-w-0 items-center gap-2">
        <span className="truncate text-foreground">{value ?? 'null'}</span>
        {value === null ? null : (
          <button
            type="button"
            onClick={() => props.onFilter(field, value)}
            className="shrink-0 cursor-pointer text-[10px] text-muted-foreground underline hover:text-foreground">
            {props.active ? 'remove filter' : 'filter by this value'}
          </button>
        )}
      </div>
    </div>
  );
}

/**
 * One top-level attribute. A scalar gets "filter by this value" and "add as column"; an object or
 * array shows as a collapsible tree.
 */
function AttributeRow(props: {
  name: string;
  value: unknown;
  filtered: boolean;
  pinned: boolean;
  onFilter: (condition: AttributeCondition) => void;
  onToggleColumn: (key: string) => void;
}) {
  const condition = scalarCondition(props.name, props.value);
  return (
    <div className="py-1.5">
      <div className="flex items-start justify-between gap-2">
        <span className="shrink-0 text-muted-foreground">{props.name}</span>
        <div className="flex min-w-0 flex-col items-end gap-0.5">
          {condition === null ? null : (
            <span className="max-w-full break-all text-foreground">{scalarLabel(props.value)}</span>
          )}
          <div className="flex items-center gap-2 text-[10px]">
            {condition === null ? null : (
              <button
                type="button"
                onClick={() => props.onFilter(condition)}
                className="cursor-pointer text-muted-foreground underline hover:text-foreground">
                {props.filtered ? 'remove filter' : 'filter by this value'}
              </button>
            )}
            <button
              type="button"
              onClick={() => props.onToggleColumn(props.name)}
              className="inline-flex cursor-pointer items-center gap-0.5 text-muted-foreground underline hover:text-foreground">
              <Columns3 className="h-2.5 w-2.5" />
              {props.pinned ? 'remove column' : 'add as column'}
            </button>
          </div>
        </div>
      </div>
      {condition === null && typeof props.value === 'object' && props.value !== null ? (
        <ValueTree value={props.value} />
      ) : null}
      {condition === null && (props.value === null || typeof props.value !== 'object') ? (
        <span className="text-quiet">{String(props.value)}</span>
      ) : null}
    </div>
  );
}

/**
 * Prints a scalar: strings quoted, numbers and booleans bare.
 */
function scalarLabel(value: unknown): string {
  return typeof value === 'string' ? `"${value}"` : String(value);
}

/**
 * A nested object or array as a tree of collapsible nodes. Nested values cannot be filtered on,
 * because the API only matches top-level attributes.
 */
function ValueTree({ value }: { value: object }) {
  const entries = Object.entries(value);
  const isArray = Array.isArray(value);
  return (
    <BaseCollapsible.Root className="mt-1">
      <BaseCollapsible.Trigger className="group flex cursor-pointer items-center gap-1 text-[10px] text-muted-foreground hover:text-foreground">
        <ChevronRight className="h-3 w-3 transition-transform group-data-[panel-open]:rotate-90" />
        {isArray ? `array, ${entries.length} items` : `object, ${entries.length} keys`}
      </BaseCollapsible.Trigger>
      <BaseCollapsible.Panel className="mt-1 ml-2 space-y-0.5 border-l border-border/60 pl-2">
        {entries.map(([key, child]) => (
          <div key={key} className="text-[11px]">
            <span className="text-muted-foreground">{key}: </span>
            {typeof child === 'object' && child !== null ? (
              <ValueTree value={child} />
            ) : (
              <span className="break-all text-foreground">{scalarLabel(child)}</span>
            )}
          </div>
        ))}
      </BaseCollapsible.Panel>
    </BaseCollapsible.Root>
  );
}

/**
 * A titled block of preformatted text with a copy button.
 */
function CopyBlock(props: { title: string; text: string; copied: boolean; onCopy: () => void }) {
  return (
    <div>
      <div className="mb-1 flex items-center justify-between">
        <span className="font-mono text-[10px] text-quiet uppercase">{props.title}</span>
        <Button variant="outline" size="xs" onClick={props.onCopy}>
          {props.copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
          <span>{props.copied ? 'Copied' : 'Copy'}</span>
        </Button>
      </div>
      <pre className="overflow-x-auto rounded border border-border bg-secondary/40 p-2.5 font-mono text-[11px] leading-relaxed text-foreground">
        {props.text}
      </pre>
    </div>
  );
}
