import { createFileRoute } from '@tanstack/react-router';

import { DataViewSkeleton } from '@/components/data-view-skeleton';
import { ExplorerView } from '@/components/explorer-view';
import { buildPlan, guessChartWidth, startPlanQueries } from '@/lib/explorer/plan';
import { queryDepsOf, validateExplorerSearch } from '@/lib/explorer/search';
import { rememberExplorerView } from '@/lib/views';

/**
 * The log explorer. Client-only: the virtualised table needs the viewport, and rendering rows on
 * the server would mean a large HTML document and a hydration pass. The server renders the skeleton
 * instead.
 *
 * The `/_app` layout completes the URL (default index, frozen "now") by redirect, so every explorer
 * URL names one definite query. `loaderDeps` is the query-relevant part of the search only; the
 * open event, pinned columns and display settings never reload data. The loader starts the four
 * requests and does not wait for them, so they run while the component chunk downloads. Leaving the
 * explorer remembers its range and display settings (`rememberExplorerView`) for the way back.
 */
export const Route = createFileRoute('/_app/')({
  validateSearch: validateExplorerSearch,
  loaderDeps: ({ search }) => queryDepsOf(search),
  ssr: false,
  loader: ({ context, deps }) => {
    const plan = buildPlan(deps, guessChartWidth(), Date.now());
    if (plan !== null) {
      startPlanQueries(context.queryClient, plan);
    }
  },
  pendingComponent: DataViewSkeleton,
  component: ExplorerView,
  // Leaving for the tail, indexes or ingest: keep the range and display settings for the way back.
  onLeave: (match) => rememberExplorerView(match.search),
});
