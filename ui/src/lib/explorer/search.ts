import { Schema } from 'effect';

import type {
  TimeRange,
  AttributeCondition,
  AttributeValue,
  LogFilters,
} from '@/lib/logbrook/types';

import { lenient } from '../lenient-schema.ts';
import { DEFAULT_RANGE, RELATIVE_RANGES, freezeRange, type RelativeRange } from './range.ts';

/**
 * Most attribute conditions the API accepts in one `filter`.
 */
export const MAX_ATTRIBUTE_CONDITIONS = 16;

/**
 * Most pinned attribute columns the table shows.
 */
export const MAX_PINNED_COLUMNS = 6;

/**
 * Most sources one request may name.
 */
const MAX_SOURCES = 128;

/**
 * Auto-refresh periods the toolbar offers, in seconds.
 */
export const REFRESH_SECONDS = [5, 10, 30, 60] as const;

/**
 * One of the auto-refresh periods.
 */
export type RefreshSeconds = (typeof REFRESH_SECONDS)[number];

/**
 * Text as the router hands it over. A value such as `404` arrives as a number, because the router
 * parses URL values as JSON, so both are accepted and read with `textOf`. That includes `index`:
 * Logbrook allows index names made only of digits, which arrive as numbers.
 */
const Text = Schema.Union([Schema.String, Schema.Finite]);

/**
 * A single value or a list of them; the router yields a bare value for `?source=a`.
 */
const TextList = Schema.Union([Text, Schema.Array(Text)]);

/**
 * Search params of the explorer route. Every field is optional and a missing one means its default,
 * which keeps URLs short. Values are strings and numbers only, so structural sharing in
 * `useSearch({ select })` works.
 *
 * `range` is relative and resolved against `at`, the moment of the last refresh; `from` and `to`
 * are an absolute range and take precedence.
 */
export const ExplorerSearchSchema = Schema.Struct({
  index: lenient(Text),
  range: lenient(Schema.Literals(RELATIVE_RANGES)),
  from: lenient(Schema.Finite.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0))),
  to: lenient(Schema.Finite.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0))),
  at: lenient(Schema.Finite.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0))),
  source: lenient(TextList),
  service: lenient(Text),
  logger: lenient(Text),
  host: lenient(Text),
  message: lenient(Text),
  minLevel: lenient(
    Schema.Finite.check(Schema.isInt(), Schema.isBetween({ minimum: 0, maximum: 255 })),
  ),
  attrs: lenient(TextList),
  event: lenient(Text),
  cols: lenient(TextList),
  density: lenient(Schema.Literals(['compact', 'comfortable'])),
  tz: lenient(Schema.Literals(['utc', 'local'])),
  wrap: lenient(Schema.Literal(1)),
  refresh: lenient(Schema.Literals(REFRESH_SECONDS)),
});

/**
 * Decoded search params of the explorer route.
 */
export type ExplorerSearch = typeof ExplorerSearchSchema.Type;

/**
 * The route's `validateSearch`: the schema as a Standard Schema.
 */
export const validateExplorerSearch = Schema.toStandardSchemaV1(ExplorerSearchSchema);

/**
 * Reads a text param as a string, trimming nothing.
 */
export function textOf(value: string | number | undefined): string | undefined {
  return value === undefined ? undefined : String(value);
}

/**
 * Reads a list param as an array of strings: absent is empty, a bare value is a one-item list.
 */
export function listOf(
  value: string | number | ReadonlyArray<string | number> | undefined,
): string[] {
  if (value === undefined) {
    return [];
  }

  const items = typeof value === 'object' ? value : [value];
  return items.map(String);
}

/**
 * Encodes an attribute condition as `t:key=value`, where `t` is `s`, `n` or `b` for a string,
 * number or boolean. The type letter keeps `123` and `"123"` apart, which the API compares as
 * different values.
 */
export function encodeAttribute(condition: AttributeCondition): string {
  const letter = attributeTypeLetter(condition.value);
  return `${letter}:${condition.key}=${String(condition.value)}`;
}

/**
 * Decodes an attribute condition written by `encodeAttribute`. Returns `null` for anything else,
 * including keys that are empty and numbers that are not finite.
 */
export function decodeAttribute(text: string): AttributeCondition | null {
  const match = /^([snb]):([^=]+)=([\s\S]*)$/.exec(text);
  if (match === null) {
    return null;
  }

  const [, letter, key, raw] = match;
  if (letter === undefined || key === undefined || raw === undefined) {
    return null;
  }

  const value = parseAttributeValue(letter, raw);
  return value === null ? null : { key, op: 'eq', value };
}

/**
 * Parses the value part of an encoded attribute by its type letter.
 */
function parseAttributeValue(letter: string, raw: string): AttributeValue | null {
  if (letter === 's') {
    return raw;
  }

  if (letter === 'b') {
    return raw === 'true' || raw === 'false' ? raw === 'true' : null;
  }

  const number = Number(raw);
  return raw.trim() !== '' && Number.isFinite(number) ? number : null;
}

/**
 * Returns `s`, `n` or `b` for the type of an attribute value.
 */
function attributeTypeLetter(value: AttributeValue): string {
  if (typeof value === 'number') {
    return 'n';
  }

  return typeof value === 'boolean' ? 'b' : 's';
}

/**
 * Reads the attribute conditions of a search, dropping malformed entries and keeping the last
 * condition for a repeated key, at most 16 of them.
 */
export function attributesOf(search: Pick<ExplorerSearch, 'attrs'>): AttributeCondition[] {
  const byKey = new Map<string, AttributeCondition>();
  for (const text of listOf(search.attrs)) {
    const condition = decodeAttribute(text);
    if (condition !== null) {
      byKey.set(condition.key, condition);
    }
  }

  return [...byKey.values()].slice(0, MAX_ATTRIBUTE_CONDITIONS);
}

/**
 * Builds the API filters from the search params. Empty text is left out.
 */
export function filtersOf(search: ExplorerSearch): LogFilters {
  const filters: { -readonly [K in keyof LogFilters]: LogFilters[K] } = {};
  const sources = listOf(search.source).slice(0, MAX_SOURCES);
  if (sources.length > 0) {
    filters.source = sources;
  }

  assignText(filters, 'service', textOf(search.service));
  assignText(filters, 'logger', textOf(search.logger));
  assignText(filters, 'host', textOf(search.host));
  assignText(filters, 'message', textOf(search.message));
  if (search.minLevel !== undefined) {
    filters.minLevel = search.minLevel;
  }

  const attributes = attributesOf(search);
  if (attributes.length > 0) {
    filters.attributes = attributes;
  }

  return filters;
}

/**
 * Sets a text filter unless the value is missing or empty.
 */
function assignText(
  filters: { -readonly [K in keyof LogFilters]: LogFilters[K] },
  key: 'service' | 'logger' | 'host' | 'message',
  value: string | undefined,
): void {
  if (value !== undefined && value !== '') {
    filters[key] = value;
  }
}

/**
 * Whether the search narrows the result with any filter.
 */
export function hasFilters(search: ExplorerSearch): boolean {
  return Object.keys(filtersOf(search)).length > 0;
}

/**
 * Resolves the search to the time range a request uses.
 *
 * An absolute range (`from` and `to`) wins. Otherwise the relative range ends at `at`, the moment
 * of the last refresh, so the same URL always asks for the same window and cursors stay valid. When
 * `at` is missing, `now` stands in; the route normally redirects first so that does not happen.
 */
export function resolveTimeRange(search: ExplorerSearch, now: number): TimeRange {
  if (search.from !== undefined && search.to !== undefined && search.from < search.to) {
    return { from: search.from, to: search.to };
  }

  return freezeRange(relativeRangeOf(search), search.at ?? now);
}

/**
 * Whether the search uses an absolute range.
 */
export function isAbsoluteRange(search: ExplorerSearch): boolean {
  return search.from !== undefined && search.to !== undefined && search.from < search.to;
}

/**
 * Returns the relative range of a search, or the default.
 */
export function relativeRangeOf(search: ExplorerSearch): RelativeRange {
  return search.range ?? DEFAULT_RANGE;
}

/**
 * The part of the search that decides what the queries ask for. `loaderDeps` returns exactly this,
 * so selecting an event, pinning a column or switching density never reloads the data.
 */
export type ExplorerQueryDeps = Pick<
  ExplorerSearch,
  | 'index'
  | 'range'
  | 'from'
  | 'to'
  | 'at'
  | 'source'
  | 'service'
  | 'logger'
  | 'host'
  | 'message'
  | 'minLevel'
  | 'attrs'
>;

/**
 * Keys of `ExplorerQueryDeps`.
 */
const QUERY_DEP_KEYS = [
  'index',
  'range',
  'from',
  'to',
  'at',
  'source',
  'service',
  'logger',
  'host',
  'message',
  'minLevel',
  'attrs',
] as const satisfies ReadonlyArray<keyof ExplorerQueryDeps>;

/**
 * Picks the query-relevant fields of a search, leaving out the ones that are not set.
 */
export function queryDepsOf(search: ExplorerSearch): ExplorerQueryDeps {
  const source: Record<string, unknown> = search;
  const deps: Record<string, unknown> = {};
  for (const key of QUERY_DEP_KEYS) {
    if (source[key] !== undefined) {
      deps[key] = source[key];
    }
  }

  return deps as ExplorerQueryDeps;
}

/**
 * A change to the search: a value sets a param, `undefined` removes it.
 */
export type SearchPatch = { readonly [K in keyof ExplorerSearch]?: ExplorerSearch[K] | undefined };

/**
 * Applies a patch to the search and removes params that are `undefined` or empty lists, so the URL
 * never carries `?source=[]`.
 */
export function applyPatch<T extends Readonly<Record<string, unknown>>>(
  search: T,
  patch: SearchPatch,
): T {
  const merged: Record<string, unknown> = { ...search, ...patch };
  for (const [key, value] of Object.entries(merged)) {
    const emptyList = Array.isArray(value) && value.length === 0;
    if (value === undefined || value === '' || emptyList) {
      delete merged[key];
    }
  }

  return merged as T;
}

/**
 * Search params the log explorer and the live tail share: the index and the filters. A link from
 * one view to the other carries exactly these. Range, selected event, columns and display settings
 * belong to the explorer alone and stay out of the tail's URL.
 */
export const SHARED_SEARCH_KEYS = [
  'index',
  'source',
  'service',
  'logger',
  'host',
  'message',
  'minLevel',
  'attrs',
] as const satisfies ReadonlyArray<keyof ExplorerSearch>;

/**
 * Picks the shared params out of the current search, for a link to the other view. Anything else in
 * `search` is dropped.
 */
export function sharedSearchOf(search: Readonly<Record<string, unknown>>): SearchPatch {
  const shared: Record<string, unknown> = {};
  for (const key of SHARED_SEARCH_KEYS) {
    if (search[key] !== undefined) {
      shared[key] = search[key];
    }
  }

  return shared;
}

/**
 * Patch that switches the view to another index. The open event belongs to the old index, so it is
 * dropped with it.
 */
export function switchIndexPatch(name: string): SearchPatch {
  return { index: name, event: undefined };
}

/**
 * Patch that removes every filter and leaves range and display settings alone.
 */
export const CLEAR_FILTERS_PATCH: SearchPatch = {
  /**
   * Removes the source list.
   */
  source: undefined,

  /**
   * Removes the service filter.
   */
  service: undefined,

  /**
   * Removes the logger filter.
   */
  logger: undefined,

  /**
   * Removes the host filter.
   */
  host: undefined,

  /**
   * Removes the message filter.
   */
  message: undefined,

  /**
   * Removes the minimum level.
   */
  minLevel: undefined,

  /**
   * Removes the attribute conditions.
   */
  attrs: undefined,
};

/**
 * Adds an attribute condition, or removes it when the identical one is already set. A different
 * value for the same key replaces the old one. At most 16 conditions are kept.
 */
export function toggleAttribute(
  current: ReadonlyArray<AttributeCondition>,
  condition: AttributeCondition,
): AttributeCondition[] {
  const existing = current.find((item) => item.key === condition.key);
  const others = current.filter((item) => item.key !== condition.key);
  if (existing !== undefined && existing.value === condition.value) {
    return others;
  }

  return [...others, condition].slice(-MAX_ATTRIBUTE_CONDITIONS);
}

/**
 * Adds a key to the pinned columns, or removes it when it is already pinned. The oldest pin drops
 * out beyond `MAX_PINNED_COLUMNS`.
 */
export function togglePinnedColumn(current: ReadonlyArray<string>, key: string): string[] {
  if (current.includes(key)) {
    return current.filter((item) => item !== key);
  }

  return [...current, key].slice(-MAX_PINNED_COLUMNS);
}

/**
 * Adds a source to the list, or removes it when it is already there.
 */
export function toggleSource(current: ReadonlyArray<string>, source: string): string[] {
  return current.includes(source)
    ? current.filter((item) => item !== source)
    : [...current, source];
}

/**
 * Which facet a click targets. `level` sets the minimum level; the rest are the exact-value
 * filters, with `source` as the only list.
 */
export type FacetField = 'level' | 'service' | 'logger' | 'host' | 'source';

/**
 * Builds the patch for clicking a facet value: it selects the value, or clears it when it is
 * already the active one. Level values are the numeric strings the server returns.
 */
export function facetPatch(field: FacetField, value: string, filters: LogFilters): SearchPatch {
  switch (field) {
    case 'level': {
      const level = Number(value);
      return { minLevel: filters.minLevel === level ? undefined : level };
    }
    case 'source':
      return { source: toggleSource(filters.source ?? [], value) };
    default:
      return { [field]: filters[field] === value ? undefined : value };
  }
}

/**
 * Fills in what a URL may leave out so that it names one definite query: the first index when none
 * is given, and the moment of the last refresh (`at`) for a relative range.
 *
 * Returns the completed search, or `null` when it was already complete. The route redirects to the
 * result, which makes the URL itself the frozen "now" and keeps reloads and shared links
 * reproducible.
 */
export function withDefaults(
  search: ExplorerSearch,
  firstIndex: string | undefined,
  now: number,
): ExplorerSearch | null {
  const needsIndex = textOf(search.index) === undefined && firstIndex !== undefined;
  const needsAt = !isAbsoluteRange(search) && search.at === undefined;
  if (!needsIndex && !needsAt) {
    return null;
  }

  return applyPatch(search, {
    ...(needsIndex ? { index: firstIndex } : {}),
    ...(needsAt ? { at: now } : {}),
  });
}

/**
 * Describes the active filters in words, for the empty state.
 */
export function describeFilters(filters: LogFilters): string[] {
  const parts: string[] = [];
  for (const source of filters.source ?? []) {
    parts.push(`source = ${source}`);
  }

  for (const key of ['service', 'logger', 'host'] as const) {
    const value = filters[key];
    if (value !== undefined) {
      parts.push(`${key} = ${value}`);
    }
  }

  if (filters.minLevel !== undefined) {
    parts.push(`level >= ${filters.minLevel}`);
  }

  if (filters.message !== undefined) {
    parts.push(`message contains "${filters.message}" (case-sensitive)`);
  }

  for (const condition of filters.attributes ?? []) {
    parts.push(`${condition.key} = ${JSON.stringify(condition.value)}`);
  }

  return parts;
}
