import type { QueryClient } from '@tanstack/react-query';

import { Outlet, createFileRoute, redirect } from '@tanstack/react-router';
import { Schema } from 'effect';
import { Suspense, lazy, useRef, useState } from 'react';

import { AppShortcuts } from '@/components/app-shortcuts';
import { TopNavBar } from '@/components/top-nav-bar';
import { accessQuery } from '@/lib/access-query';
import { ExplorerSearchSchema, textOf, withDefaults } from '@/lib/explorer/search';
import { indexesQuery } from '@/lib/logbrook/queries';
import { serverIndexesQuery } from '@/lib/logbrook/server-queries';
import { noop } from '@/lib/noop';

/**
 * The command palette, loaded the first time it opens: it and its dependency (`cmdk`) stay out of
 * the initial JavaScript of every page.
 */
const CommandPalette = lazy(() =>
  import('@/components/command-palette').then((module) => ({ default: module.CommandPalette })),
);

/**
 * The shortcut help, loaded the first time it opens, for the same reason as the palette.
 */
const ShortcutHelp = lazy(() =>
  import('@/components/shortcut-help').then((module) => ({ default: module.ShortcutHelp })),
);

/**
 * Search params every page understands: the index the views read. Absent means `default`. An index
 * named only with digits arrives as a number, because the router parses URL values as JSON, so the
 * schema is the explorer's, which accepts both; read it with `textOf`.
 */
const AppSearch = Schema.Struct({ index: ExplorerSearchSchema.fields.index });

/**
 * Layout of every page. Rendered on the server: the index list and the access info are read there,
 * so the first HTML already has the nav bar and the index switcher filled in. The app does no
 * authentication, so navigating needs no check. The views below it may opt out of server
 * rendering.
 *
 * The explorer's URL is completed here rather than in the explorer route, because this runs on the
 * server as well: the server renders the final URL, and the browser hydrates the same HTML. (A
 * redirect that only happened in the browser would change the nav links after the server had
 * rendered them, which React reports as a hydration mismatch.)
 */
export const Route = createFileRoute('/_app')({
  validateSearch: Schema.toStandardSchemaV1(AppSearch),
  ssr: true,
  beforeLoad: async ({ location, context }) => {
    if (location.pathname === '/') {
      const search = Schema.decodeUnknownSync(ExplorerSearchSchema)(location.search);
      const firstIndex =
        textOf(search.index) === undefined
          ? await defaultIndexName(context.queryClient)
          : undefined;
      const complete = withDefaults(search, firstIndex, Date.now());
      if (complete !== null) {
        throw redirect({ to: '/', search: complete, replace: true });
      }
    }
  },
  loader: async ({ context }) => {
    // On the server the data is awaited, so it is in the HTML and in the data handed to the
    // browser. In the browser the router calls this on every navigation, and the queries' own
    // freshness decides whether anything is fetched (the access info never goes stale, so it is
    // fetched once, on the server), so they are started but not awaited.
    if (import.meta.env.SSR) {
      await Promise.all([
        context.queryClient.query(accessQuery()).catch(noop),
        context.queryClient.query(serverIndexesQuery()).catch(noop),
      ]);
    } else {
      void context.queryClient.query(accessQuery()).catch(noop);
      void context.queryClient.query(indexesQuery()).catch(noop);
    }
  },
  component: AppLayout,
});

/**
 * Index to open when the URL names none: `default` when the token can read it, else the first one,
 * or `undefined` when there is none or the list cannot be loaded (the explorer then explains).
 */
async function defaultIndexName(queryClient: QueryClient): Promise<string | undefined> {
  try {
    const query = import.meta.env.SSR ? serverIndexesQuery() : indexesQuery();
    const names = (await queryClient.query(query)).map((index) => index.name);
    return names.includes('default') ? 'default' : names[0];
  } catch {
    return undefined;
  }
}

/**
 * Frames the views with the top bar, the palette and the shortcut help. A "skip to content" link is
 * the first thing a keyboard user can reach.
 */
function AppLayout() {
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [helpOpen, setHelpOpen] = useState(false);
  const returnFocusTo = useRef<HTMLElement | null>(null);

  /**
   * Remembers what had the focus when an overlay opened, so it can be given back when the overlay
   * closes. The palette is mounted only while open, and the dialog library cannot restore focus to
   * an element it never saw.
   */
  const rememberFocus = () => {
    if (!paletteOpen && !helpOpen && document.activeElement instanceof HTMLElement) {
      returnFocusTo.current = document.activeElement;
    }
  };

  /**
   * Gives the focus back after an overlay closed.
   */
  const restoreFocus = () => {
    const target = returnFocusTo.current;
    returnFocusTo.current = null;
    setTimeout(() => target?.focus(), 0);
  };

  /**
   * Opens or closes the palette, remembering and restoring the focus.
   */
  const setPalette = (open: boolean) => {
    if (open) {
      rememberFocus();
    } else {
      restoreFocus();
    }

    setPaletteOpen(open);
  };

  /**
   * Opens or closes the help, remembering and restoring the focus.
   */
  const setHelp = (open: boolean) => {
    if (open) {
      rememberFocus();
    } else {
      restoreFocus();
    }

    setHelpOpen(open);
  };

  /**
   * Opens the help. When the palette is the one asking, it closes first and the help gives the
   * focus back to what the palette had taken it from.
   */
  const openHelp = () => {
    setPaletteOpen(false);
    setHelp(true);
  };

  return (
    <div className="flex h-screen w-screen flex-col overflow-hidden bg-background text-foreground">
      <a
        href="#main"
        className="sr-only z-50 rounded bg-foreground px-3 py-1.5 text-xs text-background focus:not-sr-only focus:absolute focus:top-1 focus:left-1">
        Skip to content
      </a>
      <TopNavBar onPalette={() => setPalette(true)} onHelp={openHelp} />
      <main id="main" tabIndex={-1} className="flex min-h-0 flex-1 flex-col outline-none">
        <Outlet />
      </main>
      <AppShortcuts
        overlayOpen={paletteOpen || helpOpen}
        onPalette={() => setPalette(!paletteOpen)}
        onHelp={openHelp}
      />
      {paletteOpen ? (
        <Suspense fallback={null}>
          <CommandPalette open onOpenChange={setPalette} onHelp={openHelp} />
        </Suspense>
      ) : null}
      {helpOpen ? (
        <Suspense fallback={null}>
          <ShortcutHelp open onOpenChange={setHelp} />
        </Suspense>
      ) : null}
    </div>
  );
}
