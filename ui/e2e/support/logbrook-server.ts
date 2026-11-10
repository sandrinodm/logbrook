import type { ChildProcess } from 'node:child_process';

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

import { freePort } from './free-port.ts';
import { stopChild, trackChild, trackDir, untrackDir } from './processes.ts';

/**
 * Tokens the test server is configured with. They satisfy Logbrook's token rules (16 to 512 visible
 * characters, distinct, not starting with `replace-with-`).
 */
export const TOKENS = {
  /**
   * Ingestion token; its events carry `INGEST_SOURCE`.
   */
  ingest: 'ingest-token-0123456789',

  /**
   * Read token with no source or index restriction.
   */
  read: 'read-token-0123456789',

  /**
   * Read token limited to `INGEST_SOURCE`, which therefore sees index names only.
   */
  scopedRead: 'scoped-read-token-0123456789',

  /**
   * Admin token for index management.
   */
  admin: 'admin-token-0123456789',
} as const;

/**
 * Source the ingest token stamps on events, and the only source the scoped read token may read.
 */
export const INGEST_SOURCE = 'fixtures';

/**
 * Tail subscriber limit of the test server. It is small so a leaked subscription shows up as a
 * `429` quickly.
 */
export const MAX_TAIL_SUBSCRIBERS = 2;

/**
 * How often the test server polls for new tail events, in milliseconds. The server's minimum is 250.
 */
const TAIL_POLL_MILLIS = 250;

/**
 * How long to wait for the server to accept connections, in milliseconds.
 */
const STARTUP_TIMEOUT_MILLIS = 20_000;

/**
 * A running Logbrook server with a temporary data directory.
 */
export type LogbrookServer = {
  /**
   * Base URL without a trailing slash.
   */
  readonly url: string;

  /**
   * Stops the process and deletes the data directory.
   */
  readonly stop: () => Promise<void>;
};

/**
 * Finds the `logbrook` binary: `LOGBROOK_BIN`, else a debug or release build in the repository's
 * `target` directory. Throws with the command to build it when none exists.
 */
export function findLogbrookBinary(): string {
  const fromEnv = process.env.LOGBROOK_BIN;
  const repoRoot = resolve(import.meta.dirname, '..', '..', '..');
  const candidates = [
    fromEnv,
    join(repoRoot, 'target', 'debug', 'logbrook'),
    join(repoRoot, 'target', 'release', 'logbrook'),
  ];
  const found = candidates.find((path) => path !== undefined && existsSync(path));
  if (found === undefined) {
    throw new Error('No logbrook binary found. Run `cargo build` in the repository root.');
  }

  return found;
}

/**
 * Starts a Logbrook server on a free port with a fresh data directory and the `TOKENS` above, and
 * waits until `/ready` answers.
 */
export async function startLogbrook(): Promise<LogbrookServer> {
  const port = await freePort();
  const dataDir = await mkdtemp(join(tmpdir(), 'logbrook-ui-it-'));
  trackDir(dataDir);
  const configPath = join(dataDir, 'logbrook.toml');
  await writeFile(configPath, serverConfig(port, join(dataDir, 'data')));

  const child = spawn(findLogbrookBinary(), ['--config', configPath, 'serve'], {
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  trackChild(child, configPath);
  const output = collectOutput(child);
  const url = `http://127.0.0.1:${port}`;
  try {
    await waitUntilReady(url, child, output);
  } catch (error) {
    await stopChild(child, 0);
    await removeDir(dataDir);
    throw error;
  }

  return { url, stop: () => stopProcess(child, dataDir) };
}

/**
 * Ingests events into `index` over HTTP with the ingest token.
 *
 * `events` are Pino-style records (`time`, `level`, `msg`, ...). Throws when the server refuses the
 * batch, with its message.
 */
export async function ingest(
  server: LogbrookServer,
  index: string,
  events: ReadonlyArray<Record<string, unknown>>,
): Promise<void> {
  const response = await fetch(`${server.url}/indexes/${index}/logs/ingest`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${TOKENS.ingest}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify(events),
  });
  if (!response.ok) {
    throw new Error(`Ingest failed: ${response.status} ${await response.text()}`);
  }
}

/**
 * Renders the server configuration for a data directory and port.
 */
function serverConfig(port: number, dataDir: string): string {
  return [
    `bind = "127.0.0.1:${port}"`,
    `admin_tokens = ["${TOKENS.admin}"]`,
    'retention_days = 7',
    `max_tail_subscribers = ${MAX_TAIL_SUBSCRIBERS}`,
    `tail_poll_ms = ${TAIL_POLL_MILLIS}`,
    '',
    '[storage]',
    `data_dir = "${dataDir}"`,
    '',
    '[ingest_tokens]',
    `"${TOKENS.ingest}" = "${INGEST_SOURCE}"`,
    '',
    '[read_tokens]',
    `"${TOKENS.read}" = []`,
    `"${TOKENS.scopedRead}" = ["${INGEST_SOURCE}"]`,
    '',
  ].join('\n');
}

/**
 * Buffers the child's output so a startup failure can show it.
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
 * Polls `/ready` until it answers `200`, the process exits, or the timeout passes.
 */
async function waitUntilReady(
  url: string,
  child: ChildProcess,
  output: () => string,
): Promise<void> {
  const deadline = Date.now() + STARTUP_TIMEOUT_MILLIS;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(`logbrook exited with ${child.exitCode}:\n${output()}`);
    }

    if (await isReady(url)) {
      return;
    }

    await new Promise((done) => setTimeout(done, 100));
  }

  throw new Error(`logbrook did not become ready:\n${output()}`);
}

/**
 * Whether `/ready` answers `200`. A refused connection means the server is not up yet.
 */
async function isReady(url: string): Promise<boolean> {
  try {
    const response = await fetch(`${url}/ready`);
    return response.ok;
  } catch {
    return false;
  }
}

/**
 * Terminates the process, waits for it to exit and removes its data directory.
 */
async function stopProcess(child: ChildProcess, dataDir: string): Promise<void> {
  await stopChild(child);
  await removeDir(dataDir);
}

/**
 * Deletes a temporary directory and stops tracking it.
 */
async function removeDir(dir: string): Promise<void> {
  await rm(dir, { recursive: true, force: true });
  untrackDir(dir);
}
