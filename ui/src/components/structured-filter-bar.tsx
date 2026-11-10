import { Check, ChevronDown, PanelLeft, Plus, Search, X } from 'lucide-react';
import * as React from 'react';

import type { SearchPatch } from '@/lib/explorer/search';
import type { AttributeCondition, AttributeValue, Facets, LogFilters } from '@/lib/logbrook/types';

import { BasePopover, Button, Kbd } from '@/components/ui/reui-base';
import {
  MAX_ATTRIBUTE_CONDITIONS,
  encodeAttribute,
  toggleAttribute,
  toggleSource,
} from '@/lib/explorer/search';
import { useDebouncedDraft } from '@/lib/use-debounced-draft';

/**
 * Minimum-level choices. The API only filters by `min_level`: there is no exact or maximum level.
 */
const LEVEL_OPTIONS = [
  { value: undefined, label: 'All levels' },
  { value: 20, label: '>= 20 debug' },
  { value: 30, label: '>= 30 info' },
  { value: 40, label: '>= 40 warn' },
  { value: 50, label: '>= 50 error' },
  { value: 60, label: '>= 60 fatal' },
] as const;

/**
 * Fields the "add filter" form can set. `source` is the only list; the others hold one exact value.
 */
type FilterField = 'service' | 'logger' | 'host' | 'source' | 'attribute';

/**
 * Type of an attribute value the form can build. The API compares types, so `123` and `"123"` are
 * different values.
 */
type AttributeKind = 'string' | 'number' | 'boolean';

/**
 * What the filter bar shows and how its controls change the URL.
 */
type StructuredFilterBarProps = {
  /**
   * The active filters.
   */
  readonly filters: LogFilters;

  /**
   * Facets of the current query, used to suggest values in the "add filter" form.
   */
  readonly facets: Facets | undefined;

  /**
   * Ref to the message box, so the `/` shortcut can focus it.
   */
  readonly searchInputRef: React.RefObject<HTMLInputElement | null>;

  /**
   * Whether the facet sidebar is open.
   */
  readonly showFacets: boolean;

  /**
   * Opens or closes the facet sidebar.
   */
  readonly onToggleFacets: () => void;

  /**
   * Applies a change to the search params.
   */
  readonly onPatch: (patch: SearchPatch) => void;

  /**
   * Removes every filter.
   */
  readonly onClear: () => void;
};

/**
 * Filter bar: facet toggle, the message search, the minimum level, a chip for every active filter
 * and a form to add a service, logger, host, source or attribute condition.
 *
 * Only what the API can answer is offered: one exact value for service, logger and host, an OR list
 * of sources, a minimum level, a literal case-sensitive message substring and up to 16 top-level
 * attribute equalities.
 */
export function StructuredFilterBar(props: StructuredFilterBarProps) {
  const { filters, onPatch } = props;
  const hasFilters = Object.keys(filters).length > 0;
  const attributes = filters.attributes ?? [];

  return (
    <div className="flex flex-wrap items-center gap-1.5 border-b border-border bg-background px-3 py-1.5">
      <button
        type="button"
        onClick={props.onToggleFacets}
        aria-label="Toggle facets sidebar"
        aria-pressed={props.showFacets}
        title="Toggle facets sidebar"
        className={`inline-flex h-6 w-6 cursor-pointer items-center justify-center rounded text-muted-foreground hover:bg-accent hover:text-foreground ${
          props.showFacets ? 'text-foreground' : ''
        }`}>
        <PanelLeft className="h-3.5 w-3.5" />
      </button>

      <MessageSearch
        message={filters.message ?? ''}
        searchInputRef={props.searchInputRef}
        onChange={(message) => onPatch({ message })}
      />

      <LevelSelect minLevel={filters.minLevel} onChange={(minLevel) => onPatch({ minLevel })} />

      {filters.service === undefined ? null : (
        <Chip
          label="service"
          value={filters.service}
          onRemove={() => onPatch({ service: undefined })}
        />
      )}
      {filters.logger === undefined ? null : (
        <Chip
          label="logger"
          value={filters.logger}
          onRemove={() => onPatch({ logger: undefined })}
        />
      )}
      {filters.host === undefined ? null : (
        <Chip label="host" value={filters.host} onRemove={() => onPatch({ host: undefined })} />
      )}
      {(filters.source ?? []).map((source) => (
        <Chip
          key={source}
          label="source"
          value={source}
          onRemove={() => onPatch({ source: toggleSource(filters.source ?? [], source) })}
        />
      ))}
      {attributes.map((condition) => (
        <Chip
          key={condition.key}
          label={`${condition.key} =`}
          value={formatValue(condition.value)}
          onRemove={() => onPatch({ attrs: encodeList(toggleAttribute(attributes, condition)) })}
        />
      ))}

      <AddFilterPopover filters={filters} facets={props.facets} onPatch={onPatch} />

      {hasFilters ? (
        <button
          type="button"
          onClick={props.onClear}
          className="cursor-pointer px-1.5 font-mono text-[11px] text-quiet hover:text-foreground">
          Clear
        </button>
      ) : null}
    </div>
  );
}

/**
 * Prints an attribute value the way it was typed: strings in quotes, numbers and booleans bare.
 */
function formatValue(value: AttributeValue): string {
  return typeof value === 'string' ? `"${value}"` : String(value);
}

/**
 * Encodes conditions for the `attrs` URL param.
 */
function encodeList(conditions: ReadonlyArray<AttributeCondition>): string[] {
  return conditions.map(encodeAttribute);
}

/**
 * Message search box. The text is typed into local state and written to the URL 250 ms after the
 * last keystroke, replacing the history entry, so typing neither floods the server nor the back
 * button. It says "case-sensitive" because the API does a literal, case-sensitive substring match.
 */
function MessageSearch(props: {
  message: string;
  searchInputRef: React.RefObject<HTMLInputElement | null>;
  onChange: (message: string | undefined) => void;
}) {
  const { message, onChange, searchInputRef } = props;
  const [draft, setDraft] = useDebouncedDraft({
    value: message,
    onCommit: (text) => onChange(text === '' ? undefined : text),
  });

  return (
    <div className="relative w-64 sm:w-80">
      <Search className="pointer-events-none absolute top-1/2 left-2 h-3 w-3 -translate-y-1/2 text-quiet" />
      <input
        ref={searchInputRef}
        aria-label="Message contains (case-sensitive)"
        type="text"
        value={draft}
        onChange={(event) => setDraft(event.target.value)}
        placeholder="Message contains (case-sensitive)"
        title="Literal substring of the message. Case-sensitive: 'Error' does not match 'error'."
        className="h-6.5 w-full rounded border border-border bg-secondary/35 pr-10 pl-6.5 font-mono text-[11px] text-foreground placeholder:text-quiet focus:border-foreground/40 focus:bg-background"
      />
      <div className="absolute top-1/2 right-1.5 flex -translate-y-1/2 items-center gap-1">
        {draft === '' ? (
          <Kbd>/</Kbd>
        ) : (
          <button
            type="button"
            aria-label="Clear message filter"
            onClick={() => setDraft('')}
            className="cursor-pointer text-muted-foreground hover:text-foreground">
            <X className="h-3 w-3" />
          </button>
        )}
      </div>
    </div>
  );
}

/**
 * Popover to pick the minimum level.
 */
function LevelSelect(props: {
  minLevel: number | undefined;
  onChange: (minLevel: number | undefined) => void;
}) {
  return (
    <BasePopover
      trigger={
        <button
          type="button"
          className={`inline-flex h-6 cursor-pointer items-center gap-1 rounded border px-2 font-mono text-[11px] ${
            props.minLevel === undefined
              ? 'border-border bg-transparent text-muted-foreground hover:text-foreground'
              : 'border-foreground/30 bg-secondary text-foreground'
          }`}>
          <span>{props.minLevel === undefined ? 'level' : `level >= ${props.minLevel}`}</span>
          <ChevronDown className="h-2.5 w-2.5 opacity-50" />
        </button>
      }
      className="w-40">
      <div className="space-y-0.5">
        {LEVEL_OPTIONS.map((option) => (
          <button
            key={option.label}
            type="button"
            onClick={() => props.onChange(option.value)}
            className="flex w-full cursor-pointer items-center justify-between rounded px-2 py-1 font-mono text-[11px] hover:bg-accent">
            <span>{option.label}</span>
            {props.minLevel === option.value ? <Check className="h-3 w-3" /> : null}
          </button>
        ))}
      </div>
    </BasePopover>
  );
}

/**
 * A removable chip for one active filter.
 */
function Chip(props: { label: string; value: string; onRemove: () => void }) {
  return (
    <span className="inline-flex h-6 max-w-64 items-center gap-1 rounded border border-border bg-secondary px-2 font-mono text-[11px] text-foreground">
      <span className="text-muted-foreground">{props.label}</span>
      <span className="truncate">{props.value}</span>
      <button
        type="button"
        aria-label={`Remove filter ${props.label} ${props.value}`}
        onClick={props.onRemove}
        className="cursor-pointer text-muted-foreground hover:text-foreground">
        <X className="h-2.5 w-2.5" />
      </button>
    </span>
  );
}

/**
 * Result of reading the form: a patch to apply, or the reason it was refused.
 */
type FormOutcome =
  | { readonly ok: true; readonly patch: SearchPatch }
  | { readonly ok: false; readonly reason: string };

/**
 * What the "add filter" form holds.
 */
type FormValues = {
  /**
   * Which filter is being set.
   */
  readonly field: FilterField;

  /**
   * Attribute key, for the `attribute` field.
   */
  readonly key: string;

  /**
   * Attribute value type.
   */
  readonly kind: AttributeKind;

  /**
   * Text typed as the value.
   */
  readonly value: string;
};

/**
 * Turns the form values into a patch, or explains why it cannot. Attribute keys cannot contain `=`,
 * numbers must be finite, and at most 16 conditions fit in one filter.
 */
function readFilterForm(values: FormValues, filters: LogFilters): FormOutcome {
  const value = values.value.trim();
  if (values.field === 'attribute') {
    return readAttributeForm(values, value, filters.attributes ?? []);
  }

  if (value === '') {
    return { ok: false, reason: 'Enter a value' };
  }

  if (values.field === 'source') {
    return { ok: true, patch: { source: [...new Set([...(filters.source ?? []), value])] } };
  }

  return { ok: true, patch: { [values.field]: value } };
}

/**
 * Reads the attribute part of the form.
 */
function readAttributeForm(
  values: FormValues,
  value: string,
  current: ReadonlyArray<AttributeCondition>,
): FormOutcome {
  const key = values.key.trim();
  if (key === '' || key.includes('=')) {
    return { ok: false, reason: 'The key is required and cannot contain "="' };
  }

  if (current.length >= MAX_ATTRIBUTE_CONDITIONS && !current.some((item) => item.key === key)) {
    return { ok: false, reason: `At most ${MAX_ATTRIBUTE_CONDITIONS} attribute conditions` };
  }

  const parsed = parseValue(values.kind, value);
  if (parsed === null) {
    return { ok: false, reason: 'Enter a finite number' };
  }

  const others = current.filter((item) => item.key !== key);
  const next = [...others, { key, op: 'eq' as const, value: parsed }];
  return { ok: true, patch: { attrs: encodeList(next) } };
}

/**
 * Parses text as the chosen type. Returns `null` for a number that is empty or not finite.
 */
function parseValue(kind: AttributeKind, text: string): AttributeValue | null {
  if (kind === 'boolean') {
    return text === 'true';
  }

  if (kind === 'string') {
    return text;
  }

  const number = Number(text);
  return text !== '' && Number.isFinite(number) ? number : null;
}

/**
 * Popover form for adding a service, logger, host, source or attribute condition.
 */
function AddFilterPopover(props: {
  filters: LogFilters;
  facets: Facets | undefined;
  onPatch: (patch: SearchPatch) => void;
}) {
  const [open, setOpen] = React.useState(false);
  const [values, setValues] = React.useState<FormValues>({
    field: 'service',
    key: '',
    kind: 'string',
    value: '',
  });
  const [error, setError] = React.useState<string | null>(null);
  const suggestions = suggestionsFor(values.field, props.facets);

  /**
   * Applies the form, or shows why it was refused.
   */
  const submit = (event: React.SyntheticEvent<HTMLFormElement>) => {
    event.preventDefault();
    const outcome = readFilterForm(values, props.filters);
    if (!outcome.ok) {
      setError(outcome.reason);
      return;
    }

    props.onPatch(outcome.patch);
    setError(null);
    setValues({ ...values, value: '' });
    setOpen(false);
  };

  return (
    <BasePopover
      open={open}
      onOpenChange={setOpen}
      trigger={
        <button
          type="button"
          className="inline-flex h-6 cursor-pointer items-center gap-1 rounded border border-border bg-secondary/40 px-2 font-mono text-[11px] text-muted-foreground hover:bg-accent hover:text-foreground">
          <Plus className="h-3 w-3" />
          <span>Filter</span>
        </button>
      }
      className="w-72 p-3">
      <form onSubmit={submit} className="space-y-3">
        <div className="text-[11px] font-medium text-foreground">Add a filter</div>

        <div>
          <label htmlFor="filter-field" className="block text-[10px] text-muted-foreground">
            Field
          </label>
          <select
            id="filter-field"
            value={values.field}
            onChange={(event) => setValues({ ...values, field: event.target.value as FilterField })}
            className="mt-0.5 h-6.5 w-full rounded border border-border bg-background px-1 font-mono text-[11px]">
            <option value="service">service (exact)</option>
            <option value="logger">logger (exact)</option>
            <option value="host">host (exact)</option>
            <option value="source">source (adds to the OR list)</option>
            <option value="attribute">attribute (equals)</option>
          </select>
        </div>

        {values.field === 'attribute' ? (
          <div className="grid grid-cols-3 gap-1.5">
            <div className="col-span-2">
              <label htmlFor="filter-key" className="block text-[10px] text-muted-foreground">
                Key (top level only)
              </label>
              <input
                id="filter-key"
                value={values.key}
                onChange={(event) => setValues({ ...values, key: event.target.value })}
                className="mt-0.5 h-6.5 w-full rounded border border-border bg-background px-2 font-mono text-xs"
              />
            </div>
            <div>
              <label htmlFor="filter-kind" className="block text-[10px] text-muted-foreground">
                Type
              </label>
              <select
                id="filter-kind"
                value={values.kind}
                onChange={(event) =>
                  setValues({ ...values, kind: event.target.value as AttributeKind })
                }
                className="mt-0.5 h-6.5 w-full rounded border border-border bg-background px-1 font-mono text-[11px]">
                <option value="string">text</option>
                <option value="number">number</option>
                <option value="boolean">bool</option>
              </select>
            </div>
          </div>
        ) : null}

        <div>
          <label htmlFor="filter-value" className="block text-[10px] text-muted-foreground">
            Value
          </label>
          {values.kind === 'boolean' && values.field === 'attribute' ? (
            <select
              id="filter-value"
              value={values.value === 'true' ? 'true' : 'false'}
              onChange={(event) => setValues({ ...values, value: event.target.value })}
              className="mt-0.5 h-6.5 w-full rounded border border-border bg-background px-2 font-mono text-xs">
              <option value="true">true</option>
              <option value="false">false</option>
            </select>
          ) : (
            <input
              id="filter-value"
              list="filter-suggestions"
              value={values.value}
              onChange={(event) => setValues({ ...values, value: event.target.value })}
              className="mt-0.5 h-6.5 w-full rounded border border-border bg-background px-2 font-mono text-xs"
            />
          )}
          <datalist id="filter-suggestions">
            {suggestions.map((suggestion) => (
              <option key={suggestion} value={suggestion}>
                {suggestion}
              </option>
            ))}
          </datalist>
        </div>

        {error === null ? null : (
          <div role="alert" className="text-[10px] text-rose-500">
            {error}
          </div>
        )}

        <div className="flex justify-end border-t border-border pt-2">
          <Button type="submit" size="xs">
            Apply
          </Button>
        </div>
      </form>
    </BasePopover>
  );
}

/**
 * Values to suggest for a field, taken from the facets of the current query.
 */
function suggestionsFor(field: FilterField, facets: Facets | undefined): string[] {
  if (facets === undefined) {
    return [];
  }

  const lists = {
    service: facets.services,
    logger: facets.loggers,
    host: facets.hosts,
    source: facets.sources,
    attribute: [],
  };
  return lists[field].map((item) => item.value);
}
