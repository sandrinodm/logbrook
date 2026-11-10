import type { ReactNode } from 'react';

import { useQuery } from '@tanstack/react-query';
import { useLocation, useNavigate } from '@tanstack/react-router';
import { Command } from 'cmdk';

import type { ThemePreference } from '@/lib/theme';
import type { ViewPath } from '@/lib/views';

import { RELATIVE_RANGES } from '@/lib/explorer/range';
import { applyPatch, switchIndexPatch } from '@/lib/explorer/search';
import { indexesQuery } from '@/lib/logbrook/queries';
import { setThemePreference } from '@/lib/theme';
import { VIEWS, asRouteSearch, viewSearch } from '@/lib/views';

/**
 * Themes the palette can set.
 */
const THEMES: ReadonlyArray<{ readonly value: ThemePreference; readonly label: string }> = [
  { value: 'system', label: 'Theme: follow the system' },
  { value: 'light', label: 'Theme: light' },
  { value: 'dark', label: 'Theme: dark' },
];

/**
 * Class of every palette row. `data-[selected=true]` is how cmdk marks the highlighted one.
 */
const ITEM_CLASS =
  'flex cursor-pointer items-center justify-between gap-3 rounded px-2 py-1.5 text-xs text-foreground data-[selected=true]:bg-accent';

/**
 * Props of `CommandPalette`.
 */
type CommandPaletteProps = {
  /**
   * Whether the palette is showing.
   */
  readonly open: boolean;

  /**
   * Called when the palette asks to open or close (Escape, a click outside, after a command ran).
   */
  readonly onOpenChange: (open: boolean) => void;

  /**
   * Opens the shortcut help.
   */
  readonly onHelp: () => void;
};

/**
 * The command palette: type to filter, arrow keys to move, Enter to run. It switches view, index
 * and time range, sets the theme, and opens the shortcut help. Radix's dialog under cmdk traps
 * focus while it is open and gives it back to the element that had it when it closes.
 *
 * It is loaded on first use, so it is not part of the initial JavaScript.
 */
export function CommandPalette({ open, onOpenChange, onHelp }: CommandPaletteProps) {
  const navigate = useNavigate();
  const onExplorer = useLocation({ select: (location) => location.pathname === '/' });
  const indexes = useQuery(indexesQuery());

  /**
   * Closes the palette, then runs a command.
   */
  const run = (command: () => void | Promise<void>) => () => {
    onOpenChange(false);
    void command();
  };

  /**
   * Goes to a view with the filters it shares carried along.
   */
  const goTo = (to: ViewPath) =>
    navigate({
      to,
      search: asRouteSearch((previous) => viewSearch(to, previous, onExplorer)),
    });

  /**
   * Selects a relative range on the explorer, freezing a new "now". From another view it opens the
   * explorer with the shared filters.
   */
  const selectRange = (range: (typeof RELATIVE_RANGES)[number]) =>
    navigate({
      to: '/',
      search: asRouteSearch((previous) =>
        applyPatch(onExplorer ? previous : viewSearch('/', previous, false), {
          range,
          from: undefined,
          to: undefined,
          at: Date.now(),
        }),
      ),
    });

  /**
   * Switches index on the current view.
   */
  const selectIndex = (name: string) =>
    navigate({
      to: '.',
      search: asRouteSearch((previous) => applyPatch(previous, switchIndexPatch(name))),
    });

  return (
    <Command.Dialog
      open={open}
      onOpenChange={onOpenChange}
      label="Command palette"
      overlayClassName="fixed inset-0 z-50 bg-black/50"
      contentClassName="fixed top-[18%] left-1/2 z-50 w-[92vw] max-w-lg -translate-x-1/2 overflow-hidden rounded-lg border border-border bg-popover text-popover-foreground shadow-xl">
      <Command.Input
        placeholder="Type a command, view, index or range"
        className="w-full border-b border-border bg-transparent px-3 py-2.5 text-sm text-foreground outline-none placeholder:text-quiet focus:border-ring"
      />
      <Command.List className="max-h-80 overflow-y-auto p-1.5">
        <Command.Empty className="px-2 py-6 text-center text-xs text-muted-foreground">
          Nothing matches.
        </Command.Empty>

        <PaletteGroup heading="Go to">
          {VIEWS.map((view) => (
            <Command.Item
              key={view.to}
              value={`Go to ${view.label}`}
              className={ITEM_CLASS}
              onSelect={run(() => goTo(view.to))}>
              {view.label}
            </Command.Item>
          ))}
        </PaletteGroup>

        <PaletteGroup heading="Index">
          {(indexes.data ?? []).map((index) => (
            <Command.Item
              key={index.name}
              value={`Use index ${index.name}`}
              className={ITEM_CLASS}
              onSelect={run(() => selectIndex(index.name))}>
              Use index <span className="font-mono">{index.name}</span>
            </Command.Item>
          ))}
        </PaletteGroup>

        <PaletteGroup heading="Time range">
          {RELATIVE_RANGES.map((range) => (
            <Command.Item
              key={range}
              value={`Show the last ${range}`}
              className={ITEM_CLASS}
              onSelect={run(() => selectRange(range))}>
              Show the last <span className="font-mono">{range}</span>
            </Command.Item>
          ))}
        </PaletteGroup>

        <PaletteGroup heading="Appearance">
          {THEMES.map((theme) => (
            <Command.Item
              key={theme.value}
              value={theme.label}
              className={ITEM_CLASS}
              onSelect={run(() => setThemePreference(theme.value))}>
              {theme.label}
            </Command.Item>
          ))}
        </PaletteGroup>

        <PaletteGroup heading="Help">
          <Command.Item value="Keyboard shortcuts" className={ITEM_CLASS} onSelect={onHelp}>
            Keyboard shortcuts
            <kbd className="font-mono text-[10px] text-quiet">?</kbd>
          </Command.Item>
        </PaletteGroup>
      </Command.List>
    </Command.Dialog>
  );
}

/**
 * A titled group of palette rows. cmdk hides the group when none of its rows match.
 */
function PaletteGroup({ heading, children }: { heading: string; children: ReactNode }) {
  return (
    <Command.Group
      heading={heading}
      className="[&_[cmdk-group-heading]]:px-2 [&_[cmdk-group-heading]]:py-1 [&_[cmdk-group-heading]]:font-mono [&_[cmdk-group-heading]]:text-[10px] [&_[cmdk-group-heading]]:text-quiet [&_[cmdk-group-heading]]:uppercase">
      {children}
    </Command.Group>
  );
}
