import type { Reporter } from 'e2e';

import { existsSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { REGISTRY_ENV, cleanupRegistry, recordWorker } from './processes.ts';

/**
 * Environment variable that holds the PID of the process that created the registry. A registry path
 * inherited from a shell whose owner is gone is stale and must not be reused.
 */
const OWNER_ENV = 'LOGBROOK_UI_IT_REGISTRY_OWNER';

/**
 * Whether the registry in the environment belongs to this run: its directory exists and its owner
 * is this process (the config loaded again in the runner) or its parent (a worker, which the runner
 * forks directly). Any other owner, even a live one, is a value left in a shell by an earlier run.
 */
function ownsRegistry(registryDir: string | undefined, owner: number): boolean {
  return (
    registryDir !== undefined &&
    existsSync(registryDir) &&
    (owner === process.pid || owner === process.ppid)
  );
}

/**
 * Creates the registry directory in which the test workers record their processes and temporary
 * directories, when this process is the runner. Workers inherit the variables and only record
 * themselves, and a config that loads again in the runner does not create a second registry. A
 * registry from any other owner is stale, so it is replaced. The `exit` handler is the backstop
 * that stops whatever the workers left when no reporter runs (an early interrupt, an abandoned
 * run).
 */
export function openRunRegistry(): void {
  const existing = process.env[REGISTRY_ENV];
  const owner = Number(process.env[OWNER_ENV]);
  if (ownsRegistry(existing, owner)) {
    if (owner === process.ppid) {
      recordWorker();
    }

    return;
  }

  const registryDir = mkdtempSync(join(tmpdir(), 'logbrook-ui-it-registry-'));
  process.env[REGISTRY_ENV] = registryDir;
  process.env[OWNER_ENV] = String(process.pid);
  process.once('exit', () => cleanupRegistry(registryDir));
}

/**
 * Reporter that stops what the workers left behind once a run has finished. A worker that is killed
 * (a blocked event loop, a forced stop) runs none of its own handlers; the runner reads the
 * registry instead. The directory is kept so a later run of a watch session can append to it.
 */
export const runRegistryReporter: Reporter = {
  /**
   * Name shown in reporter output.
   */
  name: 'logbrook-ui-process-registry',

  /**
   * Stops the processes and deletes the directories the registry lists.
   */
  onRunFinished: async () => {
    const registryDir = process.env[REGISTRY_ENV];
    if (registryDir !== undefined) {
      cleanupRegistry(registryDir, { keepDirectory: true });
    }
  },
};
