import type { ChildProcess } from 'node:child_process';

import { spawnSync } from 'node:child_process';
import { appendFileSync, readFileSync, rmSync } from 'node:fs';
import { join } from 'node:path';

/**
 * Environment variable that names the registry directory of the current test run. The runner
 * process creates the directory when `e2e.config.ts` loads (see `run-registry.ts`); every process
 * the tests start is recorded in it, so the runner, which outlives its workers, can stop whatever a
 * killed worker left behind.
 */
export const REGISTRY_ENV = 'LOGBROOK_UI_IT_REGISTRY';

/**
 * How long a child gets to exit after `SIGTERM` before it is killed, in milliseconds. A BFF with no
 * connections exits in about a second.
 */
const STOP_GRACE_MILLIS = 15_000;

/**
 * Name of the registry file that lists `pid<TAB>marker` lines.
 */
const PIDS_FILE = 'pids';

/**
 * Name of the registry file that lists directories to delete, one per line.
 */
const DIRS_FILE = 'dirs';

/**
 * Name of the registry file that lists the PIDs of the runner's test workers, one per line.
 */
const WORKERS_FILE = 'workers';

/**
 * Text the command line of a test worker contains. A worker whose event loop is blocked runs no
 * handler and can outlive a forced interrupt, so workers record themselves for the runner.
 */
const WORKER_MARKER = 'e2e/dist/run/worker';

/**
 * Children this process started and has not seen exit.
 */
const liveChildren = new Set<ChildProcess>();

/**
 * Temporary directories this process created and has not removed.
 */
const liveDirs = new Set<string>();

/**
 * Whether the exit and signal handlers are installed.
 */
let handlersInstalled = false;

/**
 * Records a child so that it is stopped when this process exits or is interrupted, and so that the
 * runner can find it if this process is killed without running any handler. `marker` is a text the
 * child's command line contains, which the teardown checks before it signals a PID, so a recycled
 * PID is never killed by mistake.
 */
export function trackChild(child: ChildProcess, marker: string): void {
  installHandlers();
  liveChildren.add(child);
  child.once('exit', () => liveChildren.delete(child));
  if (child.pid !== undefined) {
    appendToRegistry(PIDS_FILE, `${child.pid}\t${marker}\n`);
  }
}

/**
 * Records this process as a test worker of the run, so the exiting runner can kill it when it is
 * stuck. Called when the config loads in a worker, before any test code can block.
 */
export function recordWorker(): void {
  appendToRegistry(WORKERS_FILE, `${process.pid}\n`);
}

/**
 * Records a temporary directory that is deleted when this process exits or is interrupted, and by
 * the runner's registry cleanup.
 */
export function trackDir(dir: string): void {
  installHandlers();
  liveDirs.add(dir);
  appendToRegistry(DIRS_FILE, `${dir}\n`);
}

/**
 * Forgets a directory the caller has deleted.
 */
export function untrackDir(dir: string): void {
  liveDirs.delete(dir);
}

/**
 * Whether the child has already exited. `exit` fires once, so waiting for it on a dead child would
 * wait forever.
 */
function hasExited(child: ChildProcess): boolean {
  return child.exitCode !== null || child.signalCode !== null;
}

/**
 * Stops a child and waits until it has exited: `SIGTERM` first, `SIGKILL` when it is still alive
 * after the grace period. Returns at once for a child that has already exited, and never rejects.
 */
export async function stopChild(
  child: ChildProcess,
  graceMillis: number = STOP_GRACE_MILLIS,
): Promise<void> {
  if (hasExited(child)) {
    return;
  }

  const exited = new Promise<void>((done) => child.once('exit', () => done()));
  child.kill('SIGTERM');
  const timer = setTimeout(() => child.kill('SIGKILL'), graceMillis);
  await exited;
  clearTimeout(timer);
}

/**
 * Kills every tracked child and deletes every tracked directory, synchronously. Used from `exit`
 * and signal handlers, where nothing can be awaited, so the children get `SIGKILL`.
 */
export function cleanupNow(): void {
  for (const child of liveChildren) {
    child.kill('SIGKILL');
  }

  liveChildren.clear();
  for (const dir of liveDirs) {
    rmSync(dir, { recursive: true, force: true });
  }

  liveDirs.clear();
}

/**
 * Stops what a finished or killed run left behind, using the registry written by `trackChild` and
 * `trackDir`: every listed PID whose command line still contains its marker gets `SIGKILL`, and
 * every listed directory is deleted. Used by the runner when a run ends. With `keepDirectory` the
 * registry directory stays, emptied, so a later run in the same process (watch mode) can append to
 * it; otherwise it is deleted too, and any test worker that is still alive is killed first: the
 * runner is exiting, so such a worker is stuck and would otherwise spin as an orphan.
 */
export function cleanupRegistry(
  registryDir: string,
  options: { readonly keepDirectory?: boolean } = {},
): void {
  if (options.keepDirectory !== true) {
    killWorkers(registryDir);
  }

  for (const line of readLines(join(registryDir, PIDS_FILE))) {
    const [pid, marker] = line.split('\t');
    if (pid !== undefined && marker !== undefined && commandOf(Number(pid)).includes(marker)) {
      process.kill(Number(pid), 'SIGKILL');
    }
  }

  for (const dir of readLines(join(registryDir, DIRS_FILE))) {
    rmSync(dir, { recursive: true, force: true });
  }

  if (options.keepDirectory === true) {
    rmSync(join(registryDir, PIDS_FILE), { force: true });
    rmSync(join(registryDir, DIRS_FILE), { force: true });
    return;
  }

  rmSync(registryDir, { recursive: true, force: true });
}

/**
 * Installs the handlers once: `exit` cleans up synchronously, and `SIGINT`, `SIGTERM` and `SIGHUP`
 * clean up first. When nobody else listens for the signal, it is delivered again so the process
 * still dies of it; a program with its own handler (the dev script) decides for itself.
 */
function installHandlers(): void {
  if (handlersInstalled) {
    return;
  }

  handlersInstalled = true;
  process.once('exit', cleanupNow);
  for (const signal of ['SIGINT', 'SIGTERM', 'SIGHUP'] as const) {
    process.once(signal, () => {
      cleanupNow();
      if (process.listenerCount(signal) === 0) {
        process.kill(process.pid, signal);
      }
    });
  }
}

/**
 * Appends a line to a registry file when a registry directory is configured.
 */
function appendToRegistry(file: string, line: string): void {
  const registryDir = process.env[REGISTRY_ENV];
  if (registryDir !== undefined) {
    appendFileSync(join(registryDir, file), line);
  }
}

/**
 * Kills every recorded test worker other than this process that is still a worker by its command
 * line, so a recycled PID is never signalled.
 */
function killWorkers(registryDir: string): void {
  for (const line of readLines(join(registryDir, WORKERS_FILE))) {
    const pid = Number(line);
    if (pid !== process.pid && commandOf(pid).includes(WORKER_MARKER)) {
      process.kill(pid, 'SIGKILL');
    }
  }
}

/**
 * The lines of a registry file, or none when it does not exist.
 */
function readLines(path: string): string[] {
  try {
    return readFileSync(path, 'utf8')
      .split('\n')
      .filter((line) => line !== '');
  } catch {
    return [];
  }
}

/**
 * The command line of a process, or an empty string when it no longer exists.
 */
function commandOf(pid: number): string {
  const result = spawnSync('ps', ['-o', 'command=', '-p', String(pid)], { encoding: 'utf8' });
  return result.status === 0 ? result.stdout : '';
}
