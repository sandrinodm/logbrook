import { Link, useLocation } from '@tanstack/react-router';
import { Command, HelpCircle, Monitor, Moon, Sun } from 'lucide-react';

import type { ThemePreference } from '@/lib/theme';

import { IndexSwitcher } from '@/components/index-switcher';
import { Button } from '@/components/ui/reui-base';
import { useThemePreference } from '@/lib/theme';
import { VIEWS, asRouteSearch, viewSearch } from '@/lib/views';

/**
 * Icon and wording of the theme button for each preference.
 */
const THEME_LOOK = {
  /**
   * Following the operating system.
   */
  system: { Icon: Monitor, label: 'system' },

  /**
   * Always light.
   */
  light: { Icon: Sun, label: 'light' },

  /**
   * Always dark.
   */
  dark: { Icon: Moon, label: 'dark' },
} as const satisfies Record<ThemePreference, unknown>;

/**
 * Props of `TopNavBar`.
 */
type TopNavBarProps = {
  /**
   * Opens the command palette.
   */
  readonly onPalette: () => void;

  /**
   * Opens the keyboard shortcut help.
   */
  readonly onHelp: () => void;
};

/**
 * Header of the layout: brand, index switcher, view links, and the palette, help and theme buttons.
 *
 * The links between Logs and Live Tail carry the index and the filters (see `searchForView`), and a
 * link to Logs also brings back the range and display settings the explorer had (`viewSearch`).
 * Every button that shows only an icon has an accessible name.
 */
export function TopNavBar({ onPalette, onHelp }: TopNavBarProps) {
  const [theme, cycleTheme] = useThemePreference();
  const { Icon: ThemeIcon, label: themeLabel } = THEME_LOOK[theme];
  const onExplorer = useLocation({ select: (location) => location.pathname === '/' });

  return (
    <header className="flex h-10 shrink-0 items-center justify-between border-b border-border bg-card/60 px-3 select-none">
      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2">
          <svg
            viewBox="0 0 24 24"
            fill="none"
            aria-hidden="true"
            className="h-4 w-4 text-foreground"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round">
            <path d="M3 8c2.5-2 5-2 7.5 0s5 2 7.5 0 2.5-1 3-1" />
            <path d="M3 12.5c2.5-2 5-2 7.5 0s5 2 7.5 0 2.5-1 3-1" />
            <path d="M3 17c2.5-2 5-2 7.5 0s5 2 7.5 0 2.5-1 3-1" />
          </svg>
          <span className="text-xs font-medium tracking-tight text-foreground">logbrook</span>
          <span className="text-quiet" aria-hidden="true">
            /
          </span>

          <IndexSwitcher />
        </div>

        <nav aria-label="Views" className="flex items-center gap-1">
          {VIEWS.map((view) => (
            <Link
              key={view.to}
              to={view.to}
              search={asRouteSearch((current) => viewSearch(view.to, current, onExplorer))}
              activeOptions={{ exact: true, includeSearch: false }}
              className="cursor-pointer rounded px-2.5 py-1 text-xs text-muted-foreground transition-colors hover:text-foreground"
              activeProps={{ className: 'bg-secondary font-medium text-foreground' }}>
              {view.label}
            </Link>
          ))}
        </nav>
      </div>

      <div className="flex items-center gap-1">
        <Button
          variant="ghost"
          size="sm"
          onClick={onPalette}
          aria-label="Open the command palette"
          aria-keyshortcuts="Control+K Meta+K"
          className="gap-1 font-mono text-[11px] text-muted-foreground">
          <Command className="h-3 w-3" />K
        </Button>

        <Button
          variant="ghost"
          size="icon"
          onClick={onHelp}
          aria-label="Keyboard shortcuts"
          aria-keyshortcuts="?">
          <HelpCircle className="h-3.5 w-3.5" />
        </Button>

        <Button
          variant="ghost"
          size="icon"
          onClick={cycleTheme}
          aria-label={`Theme: ${themeLabel}. Switch theme`}>
          <ThemeIcon className="h-3.5 w-3.5" />
        </Button>
      </div>
    </header>
  );
}
