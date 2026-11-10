import type { NitroApp } from 'nitro/types';

import { definePlugin } from 'nitro';

import { serverConfig } from './config.server.ts';
import { checkConfiguredTokens } from './credentials.server.ts';
import { createHostGuard } from './host-guard.server.ts';
import './shutdown.server.ts';

/**
 * How long the boot check waits for each answer from Logbrook, in milliseconds.
 */
const BOOT_CHECK_TIMEOUT_MILLIS = 5_000;

/**
 * Puts the `Host` check in front of the whole application. Replacing `nitroApp.fetch` covers every
 * request the Node server receives, static files included, which a route middleware would not:
 * Nitro serves static files before any middleware. The server reads `nitroApp.fetch` after the
 * plugins have run.
 */
function installHostGuard(nitroApp: NitroApp): void {
  const guard = createHostGuard(process.env);
  if (guard.warning !== null) {
    console.warn(`[logbrook-ui] ${guard.warning}`);
  }

  const handle = nitroApp.fetch;
  nitroApp.fetch = async (request) => guard.check(request) ?? (await handle(request));
}

/**
 * Nitro plugin that validates the environment when the server starts, then checks the tokens
 * against Logbrook once.
 *
 * `serverConfig` otherwise runs on the first request, so a missing `LOGBROOK_READ_TOKEN` would
 * start a server whose every page answers `500`. Throwing here stops the process at boot with the
 * message that names the variable, which is what a container orchestrator needs to see.
 *
 * The check against Logbrook is only a warning in the log: Logbrook may start after the UI. It runs
 * in the background so it never delays the listener, and it never logs a token.
 */
export default definePlugin((nitroApp) => {
  const config = serverConfig();
  installHostGuard(nitroApp);
  const deps = {
    upstreamUrl: config.logbrookUrl,
    fetch: (input: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) =>
      fetch(input, { ...init, signal: AbortSignal.timeout(BOOT_CHECK_TIMEOUT_MILLIS) }),
  };
  void checkConfiguredTokens(config, deps).then((warnings) => {
    for (const warning of warnings) {
      console.warn(`[logbrook-ui] ${warning}`);
    }
  });
});
