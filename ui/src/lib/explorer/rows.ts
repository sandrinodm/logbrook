import type { LogEvent, SearchResult } from '@/lib/logbrook/types';

/**
 * Index of the first row of every page, plus the total row count as the last entry. For pages of 3,
 * 3 and 2 rows it is `[0, 3, 6, 8]`.
 *
 * The explorer keeps pages as separate arrays and finds row `i` with these offsets, so a new page
 * never means copying 50,000 events into one array.
 */
export function pageOffsets(pages: ReadonlyArray<SearchResult>): ReadonlyArray<number> {
  const offsets = [0];
  let total = 0;
  for (const page of pages) {
    total += page.events.length;
    offsets.push(total);
  }

  return offsets;
}

/**
 * Number of rows in the pages, given their offsets.
 */
export function totalRows(offsets: ReadonlyArray<number>): number {
  return offsets[offsets.length - 1] ?? 0;
}

/**
 * Finds the page that holds row `row` with a binary search, and the row's position inside it.
 * Returns `null` when `row` is outside `0 <= row < totalRows`.
 */
export function locateRow(
  offsets: ReadonlyArray<number>,
  row: number,
): { readonly page: number; readonly position: number } | null {
  if (!Number.isInteger(row) || row < 0 || row >= totalRows(offsets)) {
    return null;
  }

  let low = 0;
  let high = offsets.length - 2;
  while (low < high) {
    const middle = (low + high + 1) >> 1;
    if ((offsets[middle] ?? 0) <= row) {
      low = middle;
    } else {
      high = middle - 1;
    }
  }

  return { page: low, position: row - (offsets[low] ?? 0) };
}

/**
 * Returns the event at row `row`, or `undefined` when there is none.
 */
export function eventAt(
  pages: ReadonlyArray<SearchResult>,
  offsets: ReadonlyArray<number>,
  row: number,
): LogEvent | undefined {
  const location = locateRow(offsets, row);
  if (location === null) {
    return undefined;
  }

  return pages[location.page]?.events[location.position];
}

/**
 * Returns the row of the event with the given id, or `-1` when it is not loaded. It scans every
 * page, so call it for a key press or a click, not per rendered row.
 */
export function rowOfEvent(
  pages: ReadonlyArray<SearchResult>,
  offsets: ReadonlyArray<number>,
  id: string,
): number {
  for (const [pageIndex, page] of pages.entries()) {
    const position = page.events.findIndex((event) => event.id === id);
    if (position >= 0) {
      return (offsets[pageIndex] ?? 0) + position;
    }
  }

  return -1;
}
