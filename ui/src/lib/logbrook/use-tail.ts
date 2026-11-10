import { useEffect, useSyncExternalStore } from 'react';

import type { TailSnapshot, TailTarget } from './tail.ts';

import { watchTabVisibility } from './tail-visibility.ts';
import { tailStore } from './tail.ts';

/**
 * Reads the tail store. The component re-renders when a snapshot is published, which is at most
 * once per animation frame.
 */
export function useTailSnapshot(): TailSnapshot {
  return useSyncExternalStore(
    tailStore.subscribe,
    tailStore.getSnapshot,
    tailStore.getServerSnapshot,
  );
}

/**
 * Keeps the tail open on `target` for as long as the calling component is mounted.
 *
 * The stream closes when the component unmounts (the user left the route), and also when the tab
 * stays hidden for a minute; it reopens, resuming where it stopped, when the tab is shown again. A
 * `null` target closes the tail. The effect depends on the serialised target, so equal filters
 * never reconnect.
 */
export function useTailConnection(target: TailTarget | null): void {
  const key = target === null ? null : JSON.stringify(target);
  useEffect(() => {
    if (key === null) {
      tailStore.close();
      return;
    }

    tailStore.open(JSON.parse(key) as TailTarget);
    const stopWatching = watchTabVisibility(tailStore, document);
    return () => {
      stopWatching();
      tailStore.close();
    };
  }, [key]);
}
