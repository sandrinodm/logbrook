import type { QueryClient } from '@tanstack/react-query';

import { logbrookKeys } from './queries.ts';

/**
 * The part of the query client that `refreshIndexLists` uses.
 */
export type IndexListInvalidator = Pick<QueryClient, 'invalidateQueries'>;

/**
 * Refreshes the management list, then the switcher list, after an index was created or deleted.
 *
 * Logbrook serves both lists and `/metrics` from one permit and answers `429` while it is taken, so
 * the two requests must not be sent at the same instant: the second refresh starts only after the
 * first one has finished.
 */
export async function refreshIndexLists(client: IndexListInvalidator): Promise<void> {
  await client.invalidateQueries({ queryKey: logbrookKeys.managedIndexes() });
  await client.invalidateQueries({ queryKey: logbrookKeys.indexes(), exact: true });
}
