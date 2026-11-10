import { describe, expect, it, vi } from 'vite-plus/test';

import { listManagedIndexesFn, listReadableIndexesFn } from '../../functions/indexes.functions.ts';
import { LogbrookError } from './errors.ts';
import { retryDelayMs, shouldRetry } from './queries.ts';
import { adminIndexesQuery, serverIndexesQuery } from './server-queries.ts';

vi.mock('../../functions/indexes.functions.ts', () => ({
  listReadableIndexesFn: vi.fn(),
  listManagedIndexesFn: vi.fn(),
}));

/**
 * An index list outcome that failed with `429` and asked to wait three seconds.
 */
const BUSY = { ok: false, status: 429, message: 'busy', retryAfterSeconds: 3 } as const;

/**
 * Runs the query function of a query's options the way the cache would, with no context.
 */
function runQuery(query: { readonly queryFn?: unknown }): Promise<unknown> {
  const run = query.queryFn as () => Promise<unknown>;
  return run();
}

describe('the index lists fetched through server functions', () => {
  it('give the switcher list a LogbrookError with the status and Retry-After of the failure', async () => {
    vi.mocked(listReadableIndexesFn).mockResolvedValue(BUSY);

    const error = await runQuery(serverIndexesQuery()).catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(LogbrookError);
    expect(error).toMatchObject({ status: 429, retryAfterSeconds: 3 });
    expect(shouldRetry(0, error)).toBe(true);
    expect(retryDelayMs(0, error)).toBe(3_000);
  });

  it('give the management list the same, so a 429 is retried instead of shown', async () => {
    vi.mocked(listManagedIndexesFn).mockResolvedValue(BUSY);

    const error = await runQuery(adminIndexesQuery()).catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(LogbrookError);
    expect(error).toMatchObject({ status: 429, retryAfterSeconds: 3 });
  });

  it('return the indexes of a successful outcome', async () => {
    const indexes = [{ name: 'default' }] as never;
    vi.mocked(listReadableIndexesFn).mockResolvedValue({ ok: true, indexes });

    expect(await runQuery(serverIndexesQuery())).toBe(indexes);
  });
});
