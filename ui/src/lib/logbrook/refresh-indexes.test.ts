import { describe, expect, it } from 'vite-plus/test';

import { logbrookKeys } from './queries.ts';
import { refreshIndexLists } from './refresh-indexes.ts';

/**
 * A fake query client whose invalidations stay pending until the test settles them, and which
 * records what was requested and when.
 */
function fakeClient() {
  const started: Array<{ queryKey: unknown; exact: boolean | undefined }> = [];
  const settlers: Array<() => void> = [];
  return {
    started,
    settlers,
    client: {
      invalidateQueries: (filters?: { queryKey?: unknown; exact?: boolean }) => {
        started.push({ queryKey: filters?.queryKey, exact: filters?.exact });
        return new Promise<void>((resolve) => settlers.push(resolve));
      },
    },
  };
}

/**
 * Lets pending promise callbacks run.
 */
async function flush(): Promise<void> {
  await new Promise((done) => setTimeout(done, 0));
}

describe('refreshIndexLists', () => {
  it('starts the management list first and the switcher list only after it has finished', async () => {
    const { client, started, settlers } = fakeClient();

    const done = refreshIndexLists(client);
    await flush();

    expect(started).toEqual([{ queryKey: logbrookKeys.managedIndexes(), exact: undefined }]);

    settlers[0]?.();
    await flush();

    expect(started).toEqual([
      { queryKey: logbrookKeys.managedIndexes(), exact: undefined },
      { queryKey: logbrookKeys.indexes(), exact: true },
    ]);
    settlers[1]?.();
    await done;
  });

  it('does not refresh the switcher list when the first refresh fails', async () => {
    const started: unknown[] = [];
    const client = {
      invalidateQueries: (filters?: { queryKey?: unknown }) => {
        started.push(filters?.queryKey);
        return Promise.reject(new Error('refetch failed'));
      },
    };

    await expect(refreshIndexLists(client)).rejects.toThrow('refetch failed');

    expect(started).toEqual([logbrookKeys.managedIndexes()]);
  });
});
