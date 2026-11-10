import type { QueryClient } from '@tanstack/react-query';
import type { ReactNode } from 'react';

import { TanStackDevtools } from '@tanstack/react-devtools';
import { ReactQueryDevtoolsPanel } from '@tanstack/react-query-devtools';
import { HeadContent, Outlet, Scripts, createRootRouteWithContext } from '@tanstack/react-router';
import { TanStackRouterDevtoolsPanel } from '@tanstack/react-router-devtools';

import { THEME_INIT_SCRIPT } from '../lib/theme.ts';
import appCss from '../styles.css?url';

/**
 * Context every route's loaders and `beforeLoad` receive, built per request by `getRouter`.
 */
type RouterContext = {
  /**
   * The request's own query cache, which the SSR Query integration hands from the server render to
   * the browser.
   */
  queryClient: QueryClient;
};

/**
 * Root route: the HTML document, its head tags and the stylesheet every page shares.
 */
export const Route = createRootRouteWithContext<RouterContext>()({
  head: () => ({
    meta: [
      { charSet: 'utf-8' },
      { name: 'viewport', content: 'width=device-width, initial-scale=1' },
      { title: 'Logbrook' },
    ],
    links: [{ rel: 'stylesheet', href: appCss }],
    scripts: [{ children: THEME_INIT_SCRIPT }],
  }),
  shellComponent: RootDocument,
  component: Outlet,
});

/**
 * Renders the `<html>` document around every page, with the TanStack Router and Query devtools.
 *
 * The theme class is not in the markup: the server cannot know the visitor's choice, and a script
 * in the head sets it before the first paint (see `THEME_INIT_SCRIPT`). `suppressHydrationWarning`
 * covers that one element, because the script changes its `class` and `style` before React
 * hydrates.
 *
 * The devtools only reach development: the `@tanstack/devtools-vite` plugin strips them from
 * production builds.
 */
function RootDocument({ children }: { children: ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <HeadContent />
      </head>
      <body className="bg-background text-foreground antialiased selection:bg-foreground/15 selection:text-foreground">
        {children}
        <TanStackDevtools
          config={{ position: 'bottom-right' }}
          plugins={[
            { name: 'TanStack Router', render: <TanStackRouterDevtoolsPanel /> },
            { name: 'TanStack Query', render: <ReactQueryDevtoolsPanel /> },
          ]}
        />
        <Scripts />
      </body>
    </html>
  );
}
