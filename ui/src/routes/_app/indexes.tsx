import { useQuery, useQueryClient } from '@tanstack/react-query';
import { createFileRoute, useNavigate, useSearch } from '@tanstack/react-router';

import type { IndexMutationOutcome } from '@/functions/indexes.functions';

import { IndexesStorageView } from '@/components/indexes-storage-view';
import { PanelSkeleton } from '@/components/panel-skeleton';
import { Button } from '@/components/ui/reui-base';
import { createIndexFn, deleteIndexFn } from '@/functions/indexes.functions';
import { accessQuery } from '@/lib/access-query';
import { applyPatch, textOf } from '@/lib/explorer/search';
import { refreshIndexLists } from '@/lib/logbrook/refresh-indexes';
import { adminIndexesQuery } from '@/lib/logbrook/server-queries';
import { noop } from '@/lib/noop';
import { asRouteSearch } from '@/lib/views';

/**
 * Index list with sizes, retention and create/delete controls. Client-only: the list comes from a
 * server function that uses the admin token, and the server renders the skeleton meanwhile.
 */
export const Route = createFileRoute('/_app/indexes')({
  ssr: false,
  pendingComponent: PanelSkeleton,
  loader: ({ context }) => {
    void context.queryClient.query(adminIndexesQuery()).catch(noop);
  },
  component: IndexesPage,
});

/**
 * Indexes view over the server's index list. It lists with the admin token when the server has one,
 * which includes indexes the read token may not read, and refreshes the list, and the index
 * switcher with it, after every create or delete.
 */
function IndexesPage() {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const current = useSearch({ from: '/_app', select: (search) => textOf(search.index) });
  const list = useQuery(adminIndexesQuery());
  const access = useQuery(accessQuery());

  /**
   * Creates an index, then refreshes the lists so it appears in both places.
   */
  const createIndex = async (name: string): Promise<IndexMutationOutcome> => {
    const outcome = await createIndexFn({ data: { name } });
    if (outcome.ok) {
      await refreshIndexLists(queryClient);
    }

    return outcome;
  };

  /**
   * Deletes an index and refreshes the lists. When the deleted index was the selected one, the
   * selection is cleared so the views fall back to `default`.
   */
  const deleteIndex = async (name: string): Promise<IndexMutationOutcome> => {
    const outcome = await deleteIndexFn({ data: { name } });
    if (outcome.ok) {
      await refreshIndexLists(queryClient);
      if (current === name) {
        await navigate({
          to: '.',
          search: asRouteSearch((previous) => applyPatch(previous, { index: undefined })),
        });
      }
    }

    return outcome;
  };

  if (list.data === undefined) {
    return list.isError ? (
      <IndexesFailed message={list.error.message} onRetry={() => void list.refetch()} />
    ) : (
      <PanelSkeleton />
    );
  }

  return (
    <IndexesStorageView
      indexes={list.data}
      canManage={access.data?.canManageIndexes === true}
      onCreateIndex={createIndex}
      onDeleteIndex={deleteIndex}
      onExploreIndex={(name) => void navigate({ to: '/', search: { index: name } })}
    />
  );
}

/**
 * Shown when the index list cannot be loaded.
 */
function IndexesFailed({
  message,
  onRetry,
}: {
  readonly message: string;
  readonly onRetry: () => void;
}) {
  return (
    <div className="flex flex-1 flex-col items-center justify-center gap-3 p-6 text-center">
      <p className="text-sm font-medium text-foreground">Could not load the index list</p>
      <p className="max-w-md font-mono text-xs break-words text-muted-foreground">{message}</p>
      <Button variant="outline" size="sm" onClick={onRetry}>
        Try again
      </Button>
    </div>
  );
}
