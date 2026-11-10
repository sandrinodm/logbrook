import { createRouter } from '@tanstack/react-router';
import { setupRouterSsrQueryIntegration } from '@tanstack/react-router-ssr-query';

import { RouteError, RouteNotFound } from './components/route-fallbacks.tsx';
import { createQueryClient } from './lib/query-client.ts';
import { routeTree } from './routeTree.gen.ts';

/**
 * Builds the router, with a new `QueryClient` in its context.
 *
 * TanStack Start calls this once per server request and once in the browser. The settings follow
 * from how the data views work:
 *
 * - `defaultPendingMinMs: 0`: a client-only route keeps its pending fallback for at least this long,
 *   and the router's default of 500 ms would show half a second of skeleton on every hard load.
 *   `defaultPendingMs` stays at its default of one second, so fast navigations show none.
 * - `defaultPreload: 'intent'` loads a route's chunk and data when the pointer rests on its link.
 * - `defaultPreloadStaleTime: 0` leaves freshness to TanStack Query instead of the router's 30-second
 *   preload cache.
 * - `defaultStructuralSharing: true` lets `useSearch({ select })` return objects and re-render only
 *   when the selected part changed. It requires JSON-compatible search params, so they hold numbers
 *   and strings, never `Date`.
 */
export function getRouter() {
  const queryClient = createQueryClient();
  const router = createRouter({
    routeTree,
    context: { queryClient },
    scrollRestoration: true,
    defaultPendingMinMs: 0,
    defaultPreload: 'intent',
    defaultPreloadStaleTime: 0,
    defaultStructuralSharing: true,
    defaultErrorComponent: RouteError,
    defaultNotFoundComponent: RouteNotFound,
  });
  setupRouterSsrQueryIntegration({ router, queryClient });

  return router;
}

declare module '@tanstack/react-router' {
  /**
   * Registers this app's router type with TanStack Router, so `Link`, `useNavigate` and route hooks
   * are typed against the generated route tree.
   */
  interface Register {
    /**
     * The router `getRouter` builds; only its type is used.
     */
    router: ReturnType<typeof getRouter>;
  }
}
