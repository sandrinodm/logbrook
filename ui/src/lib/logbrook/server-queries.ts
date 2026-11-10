import { listManagedIndexesFn, listReadableIndexesFn } from '../../functions/indexes.functions.ts';
import { unwrapIndexList } from './index-list-outcome.ts';
import { indexesQuery, managedIndexesQuery } from './queries.ts';

/**
 * The switcher's index list fetched through a server function. The layout's loader awaits it on the
 * server, so the first HTML already holds the list. It shares its key with `indexesQuery`, which
 * the browser uses afterwards through the proxy; both use the read token.
 */
export function serverIndexesQuery() {
  return { ...indexesQuery(), queryFn: async () => unwrapIndexList(await listReadableIndexesFn()) };
}

/**
 * The management view's index list. With an admin token configured it lists every index with sizes
 * and policy; without one it falls back to what the read token can see.
 */
export function adminIndexesQuery() {
  return managedIndexesQuery(async () => unwrapIndexList(await listManagedIndexesFn()));
}
