import type { CDPSession, Page } from 'playwright-core';

import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { gzipSync } from 'node:zlib';
import { chromium } from 'playwright-core';

import { startBff } from './support/bff-server.ts';
import { TOKENS, ingest, startLogbrook } from './support/logbrook-server.ts';

/**
 * Index the measurements read and write.
 */
const INDEX = 'perf';

/**
 * Events ingested before the explorer measurements. The scroll budget is for 50,000 loaded rows, so
 * there must be at least that many to load.
 */
const EVENT_COUNT = 60_000;

/**
 * Events per ingest request.
 */
const BATCH_SIZE = 1_000;

/**
 * Gap between the timestamps of consecutive events, in milliseconds. 60,000 events at 50 ms fill 50
 * minutes, inside the default one-hour range.
 */
const SPACING_MILLIS = 50;

/**
 * Pixels the scroll test moves per animation frame. At the comfortable row height of 30 px this is
 * 50 rows per frame, so every frame draws an entirely new window of rows: a hard flick, not a
 * gentle scroll.
 */
const SCROLL_PX_PER_FRAME = 1_500;

/**
 * Events per second the tail test sends, and how long it sends them.
 */
const TAIL_EVENTS_PER_SECOND = 1_000;

/**
 * How long the tail test runs, in seconds.
 */
const TAIL_SECONDS = 20;

/**
 * Whether a frame took long enough to count as dropped: more than two frames at 60 Hz.
 */
const DROPPED_FRAME_MILLIS = 34;

/**
 * Ingests the events the explorer measurements read, newest a minute ago.
 */
async function seed(server: Awaited<ReturnType<typeof startLogbrook>>): Promise<void> {
  const newest = Date.now() - 60_000;
  const services = ['checkout', 'billing', 'search', 'auth'];
  for (let start = 0; start < EVENT_COUNT; start += BATCH_SIZE) {
    const batch = Array.from({ length: BATCH_SIZE }, (_, offset) => {
      const index = start + offset;
      return {
        time: newest - index * SPACING_MILLIS,
        level: index % 25 === 0 ? 50 : 30,
        msg: `request ${index} handled in ${index % 97} ms`,
        service: services[index % services.length],
        name: 'http',
        hostname: `host-${index % 5}`,
        requestId: `req-${index}`,
        status: index % 25 === 0 ? 500 : 200,
      };
    });
    await ingest(server, INDEX, batch);
  }
}

/**
 * Installs observers in the page: long tasks and frame times.
 */
async function observe(page: Page): Promise<void> {
  await page.evaluate(() => {
    const store = { long: [] as number[], frames: [] as number[] };
    Object.assign(window, { perfStore: store });
    new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        store.long.push(entry.duration);
      }
    }).observe({ type: 'longtask', buffered: false });
    let last = performance.now();
    /**
     * Records the time since the previous frame.
     */
    const tick = (now: number) => {
      store.frames.push(now - last);
      last = now;
      requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  });
}

/**
 * Clears what `observe` recorded and returns it on the next call to `collect`.
 */
async function resetObservers(page: Page): Promise<void> {
  await page.evaluate(() => {
    const store = (window as unknown as { perfStore: { long: number[]; frames: number[] } })
      .perfStore;
    store.long.length = 0;
    store.frames.length = 0;
  });
}

/**
 * Summary of what the observers saw.
 */
type Observed = {
  /**
   * Number of long tasks (50 ms or more).
   */
  readonly longTasks: number;

  /**
   * Duration of the longest task in milliseconds, or 0.
   */
  readonly longestTask: number;

  /**
   * Number of animation frames.
   */
  readonly frames: number;

  /**
   * Frames that took more than two frame times.
   */
  readonly droppedFrames: number;

  /**
   * Longest gap between two frames, in milliseconds.
   */
  readonly maxFrame: number;

  /**
   * Median frame time in milliseconds.
   */
  readonly medianFrame: number;

  /**
   * 95th percentile frame time in milliseconds.
   */
  readonly p95Frame: number;
};

/**
 * Reads the observers' numbers.
 */
async function collect(page: Page): Promise<Observed> {
  const raw = await page.evaluate(
    () =>
      (window as unknown as { perfStore: { long: number[]; frames: number[] } }).perfStore as {
        long: number[];
        frames: number[];
      },
  );
  const sorted = [...raw.frames].sort((a, b) => a - b);
  /**
   * Picks the value at a fraction of the sorted frame times.
   */
  const at = (fraction: number) => sorted[Math.floor((sorted.length - 1) * fraction)] ?? 0;
  return {
    longTasks: raw.long.length,
    longestTask: Math.max(0, ...raw.long),
    frames: raw.frames.length,
    droppedFrames: raw.frames.filter((gap) => gap > DROPPED_FRAME_MILLIS).length,
    maxFrame: Math.max(0, ...raw.frames),
    medianFrame: at(0.5),
    p95Frame: at(0.95),
  };
}

/**
 * Prints one result line.
 */
function report(name: string, value: unknown): void {
  console.log(`${name}: ${JSON.stringify(value)}`);
}

/**
 * JavaScript transferred per route, gzipped, from the files the browser requested. Sizes come from
 * compressing each file here, which is what a gzip-only client downloads.
 */
async function measureJsSize(
  browser: Awaited<ReturnType<typeof chromium.launch>>,
  baseUrl: string,
): Promise<void> {
  const publicDir = resolve(import.meta.dirname, '..', '.output', 'public');
  for (const [name, path] of [
    ['explorer', `/?index=${INDEX}`],
    ['tail', `/tail?index=${INDEX}`],
    ['indexes', `/indexes?index=${INDEX}`],
    ['ingest', `/ingest?index=${INDEX}`],
  ] as const) {
    const context = await browser.newContext();
    const page = await context.newPage();
    const files = new Set<string>();
    page.on('response', (response) => {
      const url = new URL(response.url());
      if (url.pathname.endsWith('.js')) {
        files.add(url.pathname);
      }
    });
    await page.goto(`${baseUrl}${path}`, { waitUntil: 'networkidle' });
    const bytes = [...files].map((file) => gzipSync(readFileSync(resolve(publicDir, `.${file}`))));
    const total = bytes.reduce((sum, item) => sum + item.length, 0);
    report(`js-gzip ${name}`, {
      files: files.size,
      kB: Number((total / 1000).toFixed(1)),
      chunks: [...files].map((file) => file.replace('/assets/', '')),
    });
    await context.close();
  }
}

/**
 * Opens a page on the explorer.
 */
async function openExplorer(
  browser: Awaited<ReturnType<typeof chromium.launch>>,
  baseUrl: string,
  query: string,
): Promise<Page> {
  const context = await browser.newContext({ viewport: { width: 1400, height: 850 } });
  const page = await context.newPage();
  await page.goto(`${baseUrl}/?${query}`);
  await page.getByText(/rows loaded/).waitFor({ timeout: 30_000 });
  return page;
}

/**
 * Scrolls the grid by `SCROLL_PX_PER_FRAME` per frame from the top to the bottom and back, and
 * returns what the observers saw.
 */
async function measureScroll(page: Page): Promise<void> {
  const grid = page.getByRole('grid');
  const target = '50,000 rows loaded';
  const started = Date.now();
  while ((await page.getByText(/rows loaded/).innerText()) !== target) {
    await grid.evaluate((element) => {
      element.scrollTop = element.scrollHeight;
    });
    await page.waitForTimeout(100);
    if (Date.now() - started > 120_000) {
      throw new Error('Did not load 50,000 rows in two minutes');
    }
  }

  report('rows loaded', { text: target, secondsToLoad: (Date.now() - started) / 1000 });
  await grid.evaluate((element) => {
    element.scrollTop = 0;
  });
  await page.waitForTimeout(500);
  await resetObservers(page);
  const scrolled = await grid.evaluate(
    (element, step) =>
      new Promise<{ rows: number; height: number }>((done) => {
        const visited = new Set<string>();
        let direction = 1;
        /**
         * Scrolls one step and stops after the way back to the top.
         */
        const tick = () => {
          element.scrollTop += direction * step;
          for (const row of element.querySelectorAll('[role=row][data-index]')) {
            visited.add(row.getAttribute('data-index') ?? '');
          }
          if (direction === 1 && element.scrollTop + element.clientHeight >= element.scrollHeight) {
            direction = -1;
          }
          if (direction === -1 && element.scrollTop <= 0) {
            done({ rows: visited.size, height: element.scrollHeight });
            return;
          }
          requestAnimationFrame(tick);
        };
        requestAnimationFrame(tick);
      }),
    SCROLL_PX_PER_FRAME,
  );
  report('scroll 50,000 rows, down and up', { ...scrolled, ...(await collect(page)) });
}

/**
 * Times a filter change from the click to the first painted rows, split into the part the server
 * took and the rest.
 */
async function measureFilter(page: Page): Promise<void> {
  await page.waitForLoadState('networkidle');
  await page.waitForTimeout(2_000);
  const results: Array<Record<string, number>> = [];
  for (const service of ['checkout', 'billing', 'search', 'auth']) {
    await page.evaluate(() => {
      Object.assign(window, { filterProbe: { busyAt: 0, idleAt: 0, clickAt: 0 } });
    });
    await page.evaluate(() => {
      const probe = (
        window as unknown as { filterProbe: { busyAt: number; idleAt: number; clickAt: number } }
      ).filterProbe;
      const grid = document.querySelector('[role=grid]');
      new MutationObserver(() => {
        const busy = document.querySelector('[role=grid]')?.getAttribute('aria-busy') === 'true';
        if (busy && probe.busyAt === 0) {
          probe.busyAt = performance.now();
        } else if (!busy && probe.busyAt !== 0 && probe.idleAt === 0) {
          requestAnimationFrame(() =>
            requestAnimationFrame(() => (probe.idleAt = performance.now())),
          );
        }
      }).observe(document.body, {
        attributes: true,
        subtree: true,
        attributeFilter: ['aria-busy'],
      });
      void grid;
    });
    // The click time is taken inside the page, when the event arrives, so the time Playwright spends
    // checking that the button is visible and still is not counted.
    await page.evaluate(() => {
      performance.clearResourceTimings();
      document.addEventListener(
        'click',
        () => {
          (window as unknown as { filterProbe: { clickAt: number } }).filterProbe.clickAt =
            performance.now();
        },
        { capture: true, once: true },
      );
    });
    await page.getByRole('button', { name: new RegExp(`^${service}`) }).click();
    await page.waitForFunction(
      () => (window as unknown as { filterProbe: { idleAt: number } }).filterProbe.idleAt !== 0,
      undefined,
      { timeout: 30_000 },
    );
    const timing = await page.evaluate(() => {
      const probe = (
        window as unknown as { filterProbe: { busyAt: number; idleAt: number; clickAt: number } }
      ).filterProbe;
      const logs = performance
        .getEntriesByType('resource')
        .filter((entry) => entry.name.includes('/logs?')) as PerformanceResourceTiming[];
      const entry = logs.at(-1);
      return {
        clickToRequest: (entry?.requestStart ?? 0) - probe.clickAt,
        server: (entry?.responseStart ?? 0) - (entry?.requestStart ?? 0),
        download: (entry?.responseEnd ?? 0) - (entry?.responseStart ?? 0),
        responseToRows: probe.idleAt - (entry?.responseEnd ?? 0),
        total: probe.idleAt - probe.clickAt,
      };
    });
    results.push(timing);
    // Clear the filter again so the next one starts from the same state.
    await page.getByRole('button', { name: new RegExp(`^${service}`) }).click();
    await page.waitForTimeout(1_500);
  }

  const ownTime = results.map((row) => (row.clickToRequest ?? 0) + (row.responseToRows ?? 0));
  report('filter change', {
    runs: results.map((row) => roundAll(row)),
    ownMillis: ownTime.map(Math.round),
  });
}

/**
 * Rounds every number of a record to whole milliseconds.
 */
function roundAll(record: Record<string, number>): Record<string, number> {
  return Object.fromEntries(Object.entries(record).map(([key, value]) => [key, Math.round(value)]));
}

/**
 * Reads the JS heap in bytes after a forced garbage collection.
 */
async function heapUsed(session: CDPSession): Promise<number> {
  await session.send('HeapProfiler.collectGarbage');
  const { metrics } = await session.send('Performance.getMetrics');
  return metrics.find((metric) => metric.name === 'JSHeapUsedSize')?.value ?? 0;
}

/**
 * Tails the index while 1,000 events per second are ingested, and records frames, long tasks and
 * the heap.
 */
async function measureTail(
  browser: Awaited<ReturnType<typeof chromium.launch>>,
  baseUrl: string,
  server: Awaited<ReturnType<typeof startLogbrook>>,
): Promise<void> {
  const context = await browser.newContext({ viewport: { width: 1400, height: 850 } });
  const page = await context.newPage();
  const session = await context.newCDPSession(page);
  await session.send('Performance.enable');
  await page.goto(`${baseUrl}/tail?index=${INDEX}`);
  await page.getByText('Live', { exact: true }).waitFor({ timeout: 15_000 });
  await observe(page);
  await resetObservers(page);

  const heap: number[] = [await heapUsed(session)];
  let sent = 0;
  for (let second = 0; second < TAIL_SECONDS; second += 1) {
    const started = Date.now();
    const batch = Array.from({ length: TAIL_EVENTS_PER_SECOND }, (_, offset) => ({
      time: Date.now(),
      level: 30,
      msg: `tail ${sent + offset}`,
      service: 'checkout',
    }));
    await ingest(server, INDEX, batch);
    sent += batch.length;
    if (second % 5 === 4) {
      heap.push(await heapUsed(session));
    }

    await page.waitForTimeout(Math.max(0, 1_000 - (Date.now() - started)));
  }

  await page.waitForTimeout(1_500);
  const shown = await page.getByText(/buffered/).innerText();
  report('tail 1,000 events/s', {
    seconds: TAIL_SECONDS,
    sent,
    footer: shown,
    heapMBAfterGc: heap.map((bytes) => Number((bytes / 1e6).toFixed(1))),
    ...(await collect(page)),
  });
  await context.close();
}

/**
 * Selects rows with the profiling build and reports which components rendered on each commit, read
 * from React's DevTools hook. Needs `pnpm build:profile`.
 */
async function measureRerenders(
  browser: Awaited<ReturnType<typeof chromium.launch>>,
  baseUrl: string,
): Promise<void> {
  const context = await browser.newContext({ viewport: { width: 1400, height: 850 } });
  const page = await context.newPage();
  await page.addInitScript(() => {
    const commits: Array<Record<string, number>> = [];
    Object.assign(window, { renderCommits: commits });
    const PERFORMED_WORK = 1;
    /**
     * Counts the components of a fiber tree that did work in the last commit, by name.
     */
    const names = (fiber: any, found: Record<string, number>) => {
      for (let node = fiber; node !== null; node = node.sibling) {
        const type = node.type;
        const isComponent =
          typeof type === 'function' || (type !== null && typeof type === 'object');
        if (isComponent && (node.flags & PERFORMED_WORK) !== 0 && node.alternate !== null) {
          const inner = typeof type === 'function' ? type : (type.render ?? type.type);
          const name = inner?.displayName ?? inner?.name ?? 'anonymous';
          found[name] = (found[name] ?? 0) + 1;
          // Which props changed, for the components worth explaining (shown after the counts).
          const before = node.alternate.memoizedProps ?? {};
          const after = node.memoizedProps ?? {};
          for (const key of new Set([...Object.keys(before), ...Object.keys(after)])) {
            if (before[key] !== after[key]) {
              const label = `${name}.${key}`;
              found[label] = (found[label] ?? 0) + 1;
            }
          }
        }
        // Like the DevTools, descend only where the commit made new children. A subtree that React
        // skipped keeps its old fibers, and with them the "did work" flag of an earlier commit.
        if (node.child !== null && node.alternate !== null && node.child !== node.alternate.child) {
          names(node.child, found);
        }
      }
    };
    Object.assign(window, {
      __REACT_DEVTOOLS_GLOBAL_HOOK__: {
        supportsFiber: true,
        renderers: new Map(),
        inject: () => 1,
        checkDCE: () => {},
        onCommitFiberUnmount: () => {},
        onPostCommitFiberRoot: () => {},
        onCommitFiberRoot: (_id: number, root: any) => {
          const found: Record<string, number> = {};
          names(root.current.child, found);
          commits.push(found);
        },
      },
    });
  });
  await page.goto(`${baseUrl}/?index=${INDEX}&range=1h`);
  await page.getByText('500 rows loaded').waitFor({ timeout: 30_000 });
  await page.waitForLoadState('networkidle');
  await page.waitForTimeout(1_000);

  const selections: Array<Record<string, number>> = [];
  for (const key of ['j', 'j', 'j', 'k']) {
    await page.evaluate(() => {
      (window as unknown as { renderCommits: unknown[] }).renderCommits.length = 0;
    });
    await page.keyboard.press(key);
    await page.waitForTimeout(600);
    const commits = await page.evaluate(
      () => (window as unknown as { renderCommits: Array<Record<string, number>> }).renderCommits,
    );
    const merged: Record<string, number> = {};
    for (const commit of commits) {
      for (const [name, count] of Object.entries(commit)) {
        merged[name] = (merged[name] ?? 0) + count;
      }
    }
    selections.push(merged);
  }

  const interesting = [
    'LogRow',
    'HistogramChart',
    'FacetSidebar',
    'EventInspectorDrawer',
    'LogStreamTable',
    'ExplorerContent',
    'StructuredFilterBar',
    'RangeControls',
  ];
  report(
    'components rendered per selection (j, j, j, k)',
    selections.map((merged) =>
      Object.fromEntries(interesting.map((name) => [name, merged[name] ?? 0])),
    ),
  );
  report(
    'props that changed (component.prop: times), first selection',
    Object.fromEntries(
      Object.entries(selections[1] ?? {}).filter(
        ([label]) =>
          label.includes('.') && interesting.some((name) => label.startsWith(`${name}.`)),
      ),
    ),
  );
  await context.close();
}

/**
 * Runs the measurements named on the command line (all but `rerenders` by default).
 */
async function main(): Promise<void> {
  const wanted = new Set(process.argv.slice(2));
  const all = wanted.size === 0;
  if (!existsSync(resolve(import.meta.dirname, '..', '.output', 'server', 'index.mjs'))) {
    throw new Error('Build first: pnpm build (or pnpm build:profile for `rerenders`).');
  }

  const server = await startLogbrook();
  const bff = await startBff(server.url, { read: TOKENS.read, admin: TOKENS.admin });
  const browser = await chromium.launch();
  try {
    if (all || wanted.has('size')) {
      await measureJsSize(browser, bff.url);
    }

    if (wanted.has('rerenders')) {
      await seed(server);
      await measureRerenders(browser, bff.url);
    }

    if (all || wanted.has('scroll') || wanted.has('filter')) {
      await seed(server);
      const page = await openExplorer(browser, bff.url, `index=${INDEX}&range=1h`);
      await observe(page);
      if (all || wanted.has('filter')) {
        await measureFilter(page);
      }

      if (all || wanted.has('scroll')) {
        await measureScroll(page);
      }

      await page.context().close();
    }

    if (all || wanted.has('tail')) {
      await measureTail(browser, bff.url, server);
    }
  } finally {
    // Each stop runs even when an earlier one fails, so nothing is left running.
    await Promise.allSettled([browser.close(), bff.stop()]);
    await server.stop();
  }
}

await main();
