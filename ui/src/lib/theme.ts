import { useSyncExternalStore } from 'react';

/**
 * What the visitor chose: a fixed theme, or `system` to follow the operating system.
 */
export type ThemePreference = 'system' | 'light' | 'dark';

/**
 * `localStorage` key of the stored preference. Absent means `system`.
 */
export const THEME_STORAGE_KEY = 'logbrook-theme';

/**
 * Media query that is true when the operating system prefers a dark theme.
 */
const DARK_QUERY = '(prefers-color-scheme: dark)';

/**
 * Order the toggle cycles through.
 */
const THEME_CYCLE: ReadonlyArray<ThemePreference> = ['system', 'light', 'dark'];

/**
 * Script that runs in `<head>`, before the first paint, and puts the `dark` class on `<html>` when
 * the stored preference or the system asks for it. Without it the page would paint light and then
 * flip, or the server would have to guess. It must stay free of imports and in step with
 * `applyTheme`.
 */
export const THEME_INIT_SCRIPT = `(function(){try{var s=localStorage.getItem('${THEME_STORAGE_KEY}');var d=s==='dark'||(s!=='light'&&matchMedia('${DARK_QUERY}').matches);var e=document.documentElement;e.classList.toggle('dark',d);e.style.colorScheme=d?'dark':'light'}catch(_){}})()`;

/**
 * Callbacks to run when the preference changes in this tab.
 */
const listeners = new Set<() => void>();

/**
 * Reads the stored preference, or `system` when none is stored or storage is unavailable.
 */
function readPreference(): ThemePreference {
  try {
    const stored = localStorage.getItem(THEME_STORAGE_KEY);
    return stored === 'light' || stored === 'dark' ? stored : 'system';
  } catch {
    return 'system';
  }
}

/**
 * Puts the theme on `<html>`: the `dark` class Tailwind's variant reads, and `color-scheme` so
 * scrollbars and form controls match.
 */
function applyTheme(preference: ThemePreference): void {
  const dark = preference === 'dark' || (preference === 'system' && matchMedia(DARK_QUERY).matches);
  document.documentElement.classList.toggle('dark', dark);
  document.documentElement.style.colorScheme = dark ? 'dark' : 'light';
}

/**
 * Stores a preference, applies it and tells subscribers. A storage failure (private window, blocked
 * site data) still applies the theme for this page view.
 */
export function setThemePreference(preference: ThemePreference): void {
  try {
    if (preference === 'system') {
      localStorage.removeItem(THEME_STORAGE_KEY);
    } else {
      localStorage.setItem(THEME_STORAGE_KEY, preference);
    }
  } catch {
    // Not persisted; the choice lasts until the page is closed.
  }

  applyTheme(preference);
  for (const listener of listeners) {
    listener();
  }
}

/**
 * The preference after `current` in the toggle's cycle: system, light, dark.
 */
export function nextThemePreference(current: ThemePreference): ThemePreference {
  const index = THEME_CYCLE.indexOf(current);
  return THEME_CYCLE[(index + 1) % THEME_CYCLE.length] ?? 'system';
}

/**
 * Subscribes to preference changes from this tab, other tabs, and the operating system's own switch
 * (which matters while the preference is `system`).
 */
function subscribe(onChange: () => void): () => void {
  listeners.add(onChange);
  const media = matchMedia(DARK_QUERY);
  /**
   * Re-applies the theme when the system switches, if the preference is to follow it.
   */
  const onSystem = () => {
    if (readPreference() === 'system') {
      applyTheme('system');
    }
  };
  /**
   * Follows a preference changed in another tab.
   */
  const onStorage = (event: StorageEvent) => {
    if (event.key === THEME_STORAGE_KEY) {
      applyTheme(readPreference());
      onChange();
    }
  };
  media.addEventListener('change', onSystem);
  window.addEventListener('storage', onStorage);
  return () => {
    listeners.delete(onChange);
    media.removeEventListener('change', onSystem);
    window.removeEventListener('storage', onStorage);
  };
}

/**
 * The server renders before it can know the visitor's choice, so it reports `system`.
 */
function serverPreference(): ThemePreference {
  return 'system';
}

/**
 * The current preference and a function that moves to the next one. Server-rendered output shows
 * `system`; the browser corrects it right after hydration without a mismatch.
 */
export function useThemePreference(): readonly [ThemePreference, () => void] {
  const preference = useSyncExternalStore(subscribe, readPreference, serverPreference);
  return [preference, () => setThemePreference(nextThemePreference(preference))];
}
