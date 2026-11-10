import type { ChildProcess } from 'node:child_process';

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { resolve } from 'node:path';

import { freePort } from './free-port.ts';
import { stopChild, trackChild } from './processes.ts';

/**
 * How long to wait for the BFF to start listening, in milliseconds.
 */
const STARTUP_TIMEOUT_MILLIS = 20_000;

/**
 * How long `runBffUntilExit` waits for the process to exit by itself before it kills it, in
 * milliseconds.
 */
const EXIT_TIMEOUT_MILLIS = 20_000;

/**
 * A running build of the BFF.
 */
export type BffServer = {
  /**
   * Base URL without a trailing slash.
   */
  readonly url: string;

  /**
   * Port it listens on.
   */
  readonly port: number;

  /**
   * Stops the process.
   */
  readonly stop: () => Promise<void>;

  /**
   * Everything the process has printed so far, for tests of the boot log.
   */
  readonly output: () => string;
};

/**
 * The Logbrook tokens a test BFF is started with. Each combination a test needs (read-only, with an
 * admin token, scoped, wrong) is its own BFF process, because the tokens are read at boot.
 */
export type BffTokens = {
  /**
   * Value of `LOGBROOK_READ_TOKEN`.
   */
  readonly read: string;

  /**
   * Value of `LOGBROOK_ADMIN_TOKEN`, or omitted to leave it unset.
   */
  readonly admin?: string;
};

/**
 * Extra environment for a test BFF, for the variables that configure how it listens (`HOST`,
 * `ALLOWED_HOSTS`). Nothing of the caller's own environment is passed on.
 */
export type BffEnv = Readonly<Record<string, string>>;

/**
 * Starts the built app (`.output/server/index.mjs`, the same thing `pnpm start` runs) in production
 * mode on a free port, pointed at `logbrookUrl` with `tokens` in its environment. Throws when the
 * app has not been built or does not start listening.
 */
export async function startBff(
  logbrookUrl: string,
  tokens: BffTokens,
  extraEnv: BffEnv = {},
): Promise<BffServer> {
  const port = await freePort();
  const env: Record<string, string> = {
    PORT: String(port),
    LOGBROOK_URL: logbrookUrl,
    LOGBROOK_READ_TOKEN: tokens.read,
    ...extraEnv,
  };
  if (tokens.admin !== undefined) {
    env.LOGBROOK_ADMIN_TOKEN = tokens.admin;
  }

  const child = spawnBff(env);
  const output = collectOutput(child);
  await waitForListening(child);
  return { url: `http://127.0.0.1:${port}`, port, stop: () => stopChild(child), output };
}

/**
 * Runs the built app with exactly `env` (plus `PATH` and production mode) and waits for it to exit.
 * For the tests of what happens at boot when the environment is wrong. Returns the exit code and
 * everything it printed.
 */
export async function runBffUntilExit(
  env: Readonly<Record<string, string>>,
): Promise<{ readonly code: number | null; readonly output: string }> {
  const child = spawnBff(env);
  const output = collectOutput(child);
  const timer = setTimeout(() => child.kill('SIGKILL'), EXIT_TIMEOUT_MILLIS);
  const code = await new Promise<number | null>((done) => child.once('exit', done));
  clearTimeout(timer);
  return { code, output: output() };
}

/**
 * Spawns the built app with `env`. The caller's own environment is not passed on, so a
 * `LOGBROOK_READ_TOKEN` in the developer's shell can never leak into a test.
 */
function spawnBff(env: Readonly<Record<string, string>>): ChildProcess {
  const appRoot = resolve(import.meta.dirname, '..', '..');
  const entry = resolve(appRoot, '.output', 'server', 'index.mjs');
  if (!existsSync(entry)) {
    throw new Error('The app is not built. Run `pnpm build` in ui/ first.');
  }

  const child = spawn(process.execPath, [entry], {
    cwd: appRoot,
    env: { PATH: process.env.PATH, NODE_ENV: 'production', ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  trackChild(child, entry);
  return child;
}

/**
 * Buffers the child's output so tests can read the boot log.
 */
function collectOutput(child: ChildProcess): () => string {
  let text = '';
  child.stdout?.on('data', (chunk: Buffer) => {
    text += chunk.toString();
  });
  child.stderr?.on('data', (chunk: Buffer) => {
    text += chunk.toString();
  });
  return () => text;
}

/**
 * Resolves when the child prints its "Listening" line, and rejects when it exits first or the
 * timeout passes.
 */
function waitForListening(child: ChildProcess): Promise<void> {
  return new Promise((resolveReady, reject) => {
    let output = '';
    const timer = setTimeout(() => {
      child.kill('SIGKILL');
      reject(new Error(`BFF did not start listening:\n${output}`));
    }, STARTUP_TIMEOUT_MILLIS);

    /**
     * Collects output and resolves once the server reports that it is listening.
     */
    const onData = (chunk: Buffer) => {
      output += chunk.toString();
      if (output.includes('Listening')) {
        clearTimeout(timer);
        resolveReady();
      }
    };
    child.stdout?.on('data', onData);
    child.stderr?.on('data', onData);
    child.once('exit', (code) => {
      clearTimeout(timer);
      reject(new Error(`BFF exited with ${code}:\n${output}`));
    });
  });
}
