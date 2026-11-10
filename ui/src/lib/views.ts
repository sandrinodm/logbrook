import type { ExplorerSearch } from '@/lib/explorer/search';

import { SHARED_SEARCH_KEYS, sharedSearchOf } from '@/lib/explorer/search';

/**
 * The main views, in nav order.
 */
export const VIEWS = [
  { to: '/', label: 'Logs' },
  { to: '/tail', label: 'Live Tail' },
  { to: '/indexes', label: 'Indexes' },
  { to: '/ingest', label: 'Ingest' },
] as const;

/**
 * Path of one of the main views.
 */
export type ViewPath = (typeof VIEWS)[number]['to'];

/**
 * Whether a view reads the filters. Logs and Live Tail do, so a link between them carries the
 * filters; Indexes and Ingest only need to know which index is selected.
 */
function usesFilters(to: ViewPath): boolean {
  return to === '/' || to === '/tail';
}

/**
 * Search params that belong to the explorer alone and are worth bringing back when the user returns
 * to it: the time range (`range`, or `from` and `to`), pinned columns, density, time zone and wrap.
 * The open event is left out (it may not exist any more), and so is `at`: a returning relative
 * range takes a fresh "now" from the explorer's URL completion.
 */
const EXPLORER_ONLY_KEYS = [
  'range',
  'from',
  'to',
  'cols',
  'density',
  'tz',
  'wrap',
] as const satisfies ReadonlyArray<keyof ExplorerSearch>;

/**
 * The explorer-only params the explorer had when it was last left.
 */
export type ExplorerViewState = Readonly<Record<string, unknown>>;

/**
 * What the explorer had when the user last left it, in this tab. It lives in memory, so a reload
 * starts clean and a link opened directly is never changed by it. (Session storage would persist
 * across a reload, where the server-rendered links could no longer match the browser's.)
 */
let remembered: ExplorerViewState = {};

/**
 * Remembers the explorer-only params of `search`, replacing what was remembered before. Called when
 * the explorer is left, with the search it had.
 */
export function rememberExplorerView(search: Readonly<Record<string, unknown>>): void {
  const state: Record<string, unknown> = {};
  for (const key of EXPLORER_ONLY_KEYS) {
    if (search[key] !== undefined) {
      state[key] = search[key];
    }
  }

  remembered = state;
}

/**
 * The explorer-only params remembered by `rememberExplorerView`, or none.
 */
export function recallExplorerView(): ExplorerViewState {
  return remembered;
}

/**
 * Forgets what the explorer had.
 */
export function forgetExplorerView(): void {
  remembered = {};
}

/**
 * The search params for a link to `to`, built from the current search.
 *
 * Between Logs and Live Tail the index and the filters carry over, and nothing else: the time
 * range, the open event, pinned columns and display settings are the explorer's alone and must not
 * end up in the tail's URL. Indexes and Ingest keep only the selected index, so it survives a visit
 * and a return.
 *
 * A link to Logs can also carry `restore`, the explorer-only params remembered from the last visit
 * (see `viewSearch`). They are applied under the current filters, which always win, and never
 * include `at`, so a relative range is resolved against a new "now".
 */
export function searchForView(
  to: ViewPath,
  current: Readonly<Record<string, unknown>>,
  restore: ExplorerViewState = {},
): Record<string, unknown> {
  const shared = sharedSearchOf(current);
  if (to === '/') {
    return { ...restore, ...shared };
  }

  if (usesFilters(to)) {
    return shared;
  }

  const [indexKey] = SHARED_SEARCH_KEYS;
  return shared[indexKey] === undefined ? {} : { [indexKey]: shared[indexKey] };
}

/**
 * The search params for a navigation link, button or shortcut. Coming from any view but the
 * explorer (`fromExplorer` false), a link to Logs brings back the range, pinned columns, density,
 * time zone and wrap the explorer had when it was left. From the explorer itself nothing is
 * restored, so a click on Logs while on Logs still starts from the defaults.
 */
export function viewSearch(
  to: ViewPath,
  current: Readonly<Record<string, unknown>>,
  fromExplorer: boolean,
): Record<string, unknown> {
  return searchForView(to, current, fromExplorer ? {} : recallExplorerView());
}

/**
 * Wraps a search builder for TanStack Router's `search` option.
 *
 * The option is typed per target route, and the builders here return plain records that the target
 * route validates when it loads (its `validateSearch` is lenient and drops what it does not know).
 * Proving that to the compiler for a runtime-chosen `to` is not possible, so the one cast lives
 * here.
 */
export function asRouteSearch(
  build: (current: Record<string, unknown>) => Record<string, unknown>,
): never {
  return build as never;
}
