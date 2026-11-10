/**
 * From tablet width up, the page itself doesn't scroll: the area under the
 * header does (`.nb-scroll-root`, see globals.css), so the scrollbar starts
 * below the header. On phones the document scrolls as usual. Code that reads
 * or sets the page's scroll position asks here for the element that scrolls.
 */
export function scrollRoot(): HTMLElement {
  const root = document.querySelector<HTMLElement>(".nb-scroll-root");
  if (root && getComputedStyle(root).overflowY === "auto") return root;
  return (document.scrollingElement ?? document.documentElement) as HTMLElement;
}

/**
 * Keep the scroll position of each history entry, and put it back on Back,
 * Forward and reload. The browser and Astro's router do this for the document
 * only, not for an element that scrolls.
 */
export function restoreScrollRoot(): void {
  const key = () => `nb-scroll:${history.state?.index ?? 0}:${location.pathname}`;
  let pending = false;
  // Between leaving a page and settling on the next one, the new page's first
  // scroll events must not overwrite the position saved for its entry.
  let navigating = false;

  // Scroll events don't bubble, so listen in the capture phase.
  window.addEventListener(
    "scroll",
    (event) => {
      const root = scrollRoot();
      if (event.target !== root || root === document.scrollingElement || pending || navigating) return;
      pending = true;
      requestAnimationFrame(() => {
        pending = false;
        try {
          sessionStorage.setItem(key(), String(root.scrollTop));
        } catch {}
      });
    },
    { capture: true, passive: true },
  );

  const restore = () => {
    const root = scrollRoot();
    if (root === document.scrollingElement || location.hash) return;
    try {
      const saved = Number(sessionStorage.getItem(key()));
      if (saved > 0) root.scrollTop = saved;
    } catch {}
  };

  const [entry] = performance.getEntriesByType("navigation") as PerformanceNavigationTiming[];
  let returning = entry?.type === "back_forward" || entry?.type === "reload";
  document.addEventListener("astro:before-preparation", (event) => {
    returning = (event as Event & { navigationType?: string }).navigationType === "traverse";
    navigating = true;
  });
  document.addEventListener("astro:page-load", () => {
    if (returning) restore();
    returning = false;
    requestAnimationFrame(() => {
      navigating = false;
    });
  });
}
