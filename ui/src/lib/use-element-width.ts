import type { RefCallback } from 'react';

import { useEffect, useState } from 'react';

/**
 * Tracks the content width of an element with a `ResizeObserver`.
 *
 * Returns a callback ref to attach and the latest width in pixels. `initialWidth` is used until the
 * first measurement, so a chart can pick its bucket count on the first render instead of flashing.
 * The element is kept in state rather than read from a ref object in an effect, so an element that
 * appears later (the component first rendered something else) or is replaced is still observed.
 */
export function useElementWidth<T extends HTMLElement>(
  initialWidth: number,
): readonly [RefCallback<T>, number] {
  const [element, setElement] = useState<T | null>(null);
  const [width, setWidth] = useState(initialWidth);

  useEffect(() => {
    if (element === null) {
      return undefined;
    }

    const observer = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (entry !== undefined) {
        setWidth(Math.floor(entry.contentRect.width));
      }
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, [element]);

  return [setElement, width];
}
