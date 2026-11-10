/**
 * Key of the shared controller on `globalThis`. The Nitro plugin and the TanStack Start server
 * bundle are built separately, so a module-level singleton could exist twice in one process; a
 * global key makes both see the same controller.
 */
const CONTROLLER_KEY = Symbol.for('logbrook-ui.shutdown-controller');

/**
 * Aborted when the process is told to stop (`SIGTERM` from `docker stop`, `SIGINT` from Ctrl+C).
 *
 * Nitro's graceful shutdown waits for open connections before it closes the server, and a tail
 * stream never ends by itself. Without this signal one open tail makes every stop wait for the
 * whole grace period (5 s by default) and then cut the connection anyway. The proxy ends its event
 * streams when this signal fires, so the server closes as soon as the last ordinary request is
 * done. The browser's `EventSource` reconnects with its last event ID, which is what it does after
 * any dropped connection.
 */
export const shutdownSignal: AbortSignal = sharedController().signal;

/**
 * The process-wide controller behind `shutdownSignal`, created with its signal handlers on first
 * use.
 */
function sharedController(): AbortController {
  const holder = globalThis as { [CONTROLLER_KEY]?: AbortController };
  const existing = holder[CONTROLLER_KEY];
  if (existing !== undefined) {
    return existing;
  }

  const controller = new AbortController();
  for (const signal of ['SIGINT', 'SIGTERM'] as const) {
    process.once(signal, () => controller.abort(new Error(`Received ${signal}`)));
  }

  holder[CONTROLLER_KEY] = controller;
  return controller;
}
