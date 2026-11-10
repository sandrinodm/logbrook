import { useLocation, useNavigate } from '@tanstack/react-router';
import { useEffect, useEffectEvent } from 'react';

import { hasCommandModifier, isTypingTarget } from '@/lib/shortcuts';
import { asRouteSearch, viewSearch } from '@/lib/views';

/**
 * Props of `AppShortcuts`.
 */
type AppShortcutsProps = {
  /**
   * Whether an overlay (the palette or the help) is open. Single keys do nothing then, so typing in
   * the palette or pressing `t` over the help cannot also move the page behind it.
   */
  readonly overlayOpen: boolean;

  /**
   * Toggles the command palette.
   */
  readonly onPalette: () => void;

  /**
   * Opens the shortcut help.
   */
  readonly onHelp: () => void;
};

/**
 * Registers the shortcuts that work on every signed-in page: `Ctrl K` or `⌘ K` for the palette, `t`
 * to switch between Logs and Live Tail, and `?` for the help. The palette key works inside text
 * fields; the single keys do not. Renders nothing.
 *
 * The page-specific keys (`j`, `k`, `/`, `Esc`) live with the views they act on.
 */
export function AppShortcuts({ overlayOpen, onPalette, onHelp }: AppShortcutsProps) {
  const navigate = useNavigate();
  const onTail = useLocation({ select: (location) => location.pathname === '/tail' });
  const onExplorer = useLocation({ select: (location) => location.pathname === '/' });

  /**
   * Handles one key press. An effect event, so the listener below is added once and still sees the
   * latest overlay state and location.
   */
  const onKeyDown = useEffectEvent((event: KeyboardEvent) => {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'k') {
      event.preventDefault();
      onPalette();
      return;
    }

    if (overlayOpen || hasCommandModifier(event) || isTypingTarget(event.target)) {
      return;
    }

    if (event.key === '?') {
      event.preventDefault();
      onHelp();
    } else if (event.key === 't') {
      event.preventDefault();
      const to = onTail ? '/' : '/tail';
      void navigate({
        to,
        search: asRouteSearch((previous) => viewSearch(to, previous, onExplorer)),
      });
    }
  });

  useEffect(() => {
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, []);

  return null;
}
