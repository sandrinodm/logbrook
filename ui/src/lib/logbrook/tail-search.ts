import { Schema } from 'effect';

import { ExplorerSearchSchema, filtersOf, type ExplorerSearch } from '@/lib/explorer/search';

import type { TailTarget } from './tail.ts';
import type { TimeRange } from './types.ts';

/**
 * Search params of the tail route: the explorer's filter params under the same names and encoding,
 * so a filter set carries over between the two views. The tail has no time range.
 */
export const TailSearchSchema = Schema.Struct({
  index: ExplorerSearchSchema.fields.index,
  source: ExplorerSearchSchema.fields.source,
  service: ExplorerSearchSchema.fields.service,
  logger: ExplorerSearchSchema.fields.logger,
  host: ExplorerSearchSchema.fields.host,
  message: ExplorerSearchSchema.fields.message,
  minLevel: ExplorerSearchSchema.fields.minLevel,
  attrs: ExplorerSearchSchema.fields.attrs,
});

/**
 * Decoded search params of the tail route.
 */
export type TailSearch = typeof TailSearchSchema.Type;

/**
 * The route's `validateSearch`: the schema as a Standard Schema.
 */
export const validateTailSearch = Schema.toStandardSchemaV1(TailSearchSchema);

/**
 * Picks the index to follow: the one in the URL, else `default` when the credential can see it,
 * else the first index. Returns `null` while the list is unknown or empty.
 */
export function resolveTailIndex(
  requested: string | undefined,
  names: ReadonlyArray<string> | undefined,
): string | null {
  if (requested !== undefined && requested !== '') {
    return requested;
  }

  if (names === undefined || names.length === 0) {
    return null;
  }

  return names.includes('default') ? 'default' : (names[0] ?? null);
}

/**
 * What to follow for a search and a resolved index.
 */
export function tailTargetOf(search: TailSearch, indexName: string): TailTarget {
  return { index: indexName, filters: filtersOf(search) };
}

/**
 * The explorer search that shows `window` with the tail's index and filters, so a reader can look
 * up what a broken tail missed. The explorer reads the same param names.
 */
export function explorerSearchFor(
  search: TailSearch,
  indexName: string,
  window: TimeRange,
): ExplorerSearch {
  return { ...search, index: indexName, from: window.from, to: window.to };
}
