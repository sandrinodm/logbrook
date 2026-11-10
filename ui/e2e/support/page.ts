import type { Browser } from '@e2e-dev/web';
import type { Locator } from 'e2e';

import { setTimeout as sleep } from 'node:timers/promises';

/**
 * Pauses for `millis`. The runner has no fixed-time wait of its own; this is the plain sleep that
 * Playwright's `waitForTimeout` was.
 */
export const pause = (millis: number): Promise<void> => sleep(millis);

/**
 * CSS selector of the chart in the explorer's histogram: the `<svg>` that `getByRole('img', { name:
 * /Events over time/ })` finds. `locator.locator()` and `locator.evaluate()` do not exist in the
 * runner, so the tests that look inside it query by CSS.
 */
export const HISTOGRAM_SELECTOR = 'svg[role=img][aria-label*="Events over time"]';

/**
 * Resolves once the timers the page has already queued with a zero delay have run. Playwright's
 * waits were slow enough for them to run on their own; the runner's steps take about a millisecond,
 * so a step that follows a dialog closing can come before the page has finished giving the focus
 * back. Timers fire in the order they were set, so one more timer set from here runs after them.
 */
export async function settle(browser: Browser): Promise<void> {
  await browser.evaluate(async () => {
    await new Promise<void>((done) => setTimeout(done, 0));
    return null;
  });
}

/**
 * The text of the single element `locator` finds, as `textContent()` reads it (whitespace
 * collapsed, which is the nearest the runner has to `innerText()`). Throws when the element has no
 * text node to read, so a missing value fails the test instead of matching an empty string.
 */
export async function readText(locator: Locator): Promise<string> {
  const text = await locator.textContent();
  if (text === null) {
    throw new Error('The element has no text content');
  }

  return text;
}

/**
 * The search params of the page's current URL.
 */
export async function searchParams(browser: Browser): Promise<URLSearchParams> {
  return new URL(await browser.url()).searchParams;
}

/**
 * The path of the page's current URL.
 */
export async function pathname(browser: Browser): Promise<string> {
  return new URL(await browser.url()).pathname;
}

/**
 * Moves the mouse in a straight line from `from` to `to` in `steps` equal moves, the last one on
 * `to`, as Playwright's `mouse.move(x, y, { steps })` does. The runner's `mouse.move` has no
 * `steps` option.
 */
export async function moveMouseInSteps(
  browser: Browser,
  from: { readonly x: number; readonly y: number },
  to: { readonly x: number; readonly y: number },
  steps: number,
): Promise<void> {
  for (let step = 1; step <= steps; step += 1) {
    const progress = step / steps;
    await browser.mouse.move(
      from.x + (to.x - from.x) * progress,
      from.y + (to.y - from.y) * progress,
    );
  }
}

/**
 * Scrolls the rows grid to `top` pixels, or to its end with `'bottom'`. Throws unless exactly one
 * grid is on the page, which is the strictness a Playwright locator had.
 */
export async function scrollGrid(browser: Browser, top: number | 'bottom'): Promise<void> {
  await browser.evaluate((target) => {
    const grids = document.querySelectorAll('[role=grid]');
    const grid = grids[0];
    if (grids.length !== 1 || grid === undefined) {
      throw new Error(`Expected one grid, found ${grids.length}`);
    }

    grid.scrollTop = target === 'bottom' ? grid.scrollHeight : target;
    return null;
  }, top);
}

/**
 * The drawn width of the histogram chart and the whole width of the area that holds it. Throws
 * unless exactly one chart is on the page.
 */
export async function chartWidths(
  browser: Browser,
): Promise<{ readonly drawn: number; readonly available: number }> {
  return browser.evaluate((selector) => {
    const charts = document.querySelectorAll(selector);
    const svg = charts[0];
    if (charts.length !== 1 || svg === undefined) {
      throw new Error(`Expected one chart, found ${charts.length}`);
    }

    return {
      drawn: Number(svg.getAttribute('width')),
      available: Math.floor(svg.parentElement?.getBoundingClientRect().width ?? 0),
    };
  }, HISTOGRAM_SELECTOR);
}
