import type { E2EConfig } from 'e2e';

import { web } from '@e2e-dev/web';

import { openRunRegistry, runRegistryReporter } from './e2e/support/run-registry.ts';

/**
 * Base URL of the only target. Each BFF a test starts listens on its own free port and tests
 * navigate with absolute URLs, so nothing listens here and a relative `goto` fails at once.
 */
const UNUSED_APP_URL = 'http://127.0.0.1:9';

// Creates the run's process registry in the runner process; workers inherit it and skip this.
openRunRegistry();

/**
 * E2e config for the integration and browser tests in `e2e/`: one Chromium target, one worker, no
 * retries, and the timeouts the Vitest config used. Run through `pnpm test:e2e`, which builds the
 * app first.
 */
export default {
  tests: 'e2e/**/*.e2e.ts',
  targets: [
    {
      name: 'chromium',
      engine: web({ viewport: { width: 1400, height: 850 } }),
      app: { url: UNUSED_APP_URL },
    },
  ],
  workers: 1,
  retries: 0,
  timeout: 30_000,
  actionTimeout: 30_000,
  assertionTimeout: 1_000,
  cleanupTimeout: 30_000,
  trace: 'retain-on-failure',
  video: 'off',
  cache: 'off',
  reporters: ['list', runRegistryReporter],
} satisfies E2EConfig;
