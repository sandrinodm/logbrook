import type { AttributeCondition, LogFilters, SearchParams, TimeRange } from './types.ts';

/**
 * Serialises attribute conditions into the JSON document of the `filter` query parameter.
 *
 * Conditions are sorted by key so equal filters always produce equal URLs, which keeps query keys
 * and cursors stable. Returns `null` for an empty list, because the server treats an omitted
 * `filter` the same as an empty one.
 */
export function attributeFilterJson(conditions: ReadonlyArray<AttributeCondition>): string | null {
  if (conditions.length === 0) {
    return null;
  }

  const sorted = [...conditions].sort((a, b) => a.key.localeCompare(b.key));
  const attributes = sorted.map(({ key, op, value }) => ({ key, op, value }));
  return JSON.stringify({ attributes });
}

/**
 * Appends the filters every endpoint shares to `params`.
 *
 * Blank text filters are skipped instead of sent as empty strings, which the server would treat as
 * a real (and empty) value. `source` is joined with commas, the server's OR syntax.
 */
export function appendFilters(params: URLSearchParams, filters: LogFilters): void {
  if (filters.source !== undefined && filters.source.length > 0) {
    params.set('source', filters.source.join(','));
  }

  appendText(params, 'service', filters.service);
  appendText(params, 'logger', filters.logger);
  appendText(params, 'host', filters.host);
  appendText(params, 'message', filters.message);

  if (filters.minLevel !== undefined) {
    params.set('min_level', String(filters.minLevel));
  }

  const filter = attributeFilterJson(filters.attributes ?? []);
  if (filter !== null) {
    params.set('filter', filter);
  }
}

/**
 * Appends the required `from` and `to` bounds to `params`.
 */
export function appendRange(params: URLSearchParams, range: TimeRange): void {
  params.set('from', String(range.from));
  params.set('to', String(range.to));
}

/**
 * Sets `name` to `value` unless `value` is missing or empty.
 */
function appendText(params: URLSearchParams, name: string, value: string | undefined): void {
  if (value !== undefined && value !== '') {
    params.set(name, value);
  }
}

/**
 * Builds the query string of a search page: range, filters, `limit` and, after the first page, the
 * cursor.
 */
export function searchQueryString(params: SearchParams, cursor: string | null): string {
  const query = new URLSearchParams();
  appendRange(query, params);
  appendFilters(query, params);
  if (params.limit !== undefined) {
    query.set('limit', String(params.limit));
  }

  if (cursor !== null) {
    query.set('cursor', cursor);
  }

  return query.toString();
}
