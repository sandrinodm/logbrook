import { createServerFn } from '@tanstack/react-start';
import { Schema } from 'effect';

import type { IndexFailure, IndexListOutcome } from '../lib/logbrook/index-list-outcome.ts';

import { IndexNameInput } from '../lib/inputs.ts';
import { failureOf } from '../lib/logbrook/index-list-outcome.ts';
import { serverConfig } from '../server/config.server.ts';
import { createIndex, deleteIndex, listIndexes } from '../server/credentials.server.ts';

/**
 * What an index mutation returns: success, or the HTTP status and the reason, so the form can tell
 * "index limit reached" (`429`) from "protected index" (`400`). Thrown errors lose their status on
 * the way to the browser, so failures are returned instead.
 */
export type IndexMutationOutcome = { readonly ok: true } | IndexFailure;

/**
 * Lists the indexes the configured read token can reach. This is what the index switcher offers, so
 * it never includes an index only the admin token may see. Used by the layout's loader, so the
 * first page load has the list already. A failure is returned with its status and `Retry-After`,
 * not thrown (see `IndexFailure`).
 */
export const listReadableIndexesFn = createServerFn({ method: 'GET' }).handler(
  (): Promise<IndexListOutcome> => listWith(serverConfig().readToken),
);

/**
 * Lists the indexes for the management view: with the admin token when one is configured, which
 * returns every index with sizes and policy, otherwise with the read token. A failure is returned,
 * not thrown.
 */
export const listManagedIndexesFn = createServerFn({ method: 'GET' }).handler(
  (): Promise<IndexListOutcome> => {
    const { adminToken, readToken } = serverConfig();
    return listWith(adminToken ?? readToken);
  },
);

/**
 * Creates an index with the configured admin token. Idempotent, as Logbrook's `PUT
 * /indexes/{index}` is. At the index limit the outcome carries status `429`. Like every server
 * function it rejects cross-origin requests, which matters because the reverse proxy in front of
 * the app authenticates the user but does not stop cross-site requests.
 */
export const createIndexFn = createServerFn({ method: 'POST' })
  .validator(Schema.toStandardSchemaV1(IndexNameInput))
  .handler(({ data }) =>
    mutate((adminToken) => createIndex(data.name, adminToken, upstreamDeps())),
  );

/**
 * Deletes an index with the configured admin token. Logbrook refuses `default` and indexes that its
 * configuration declares with status `400`. Same-origin only, like `createIndexFn`.
 */
export const deleteIndexFn = createServerFn({ method: 'POST' })
  .validator(Schema.toStandardSchemaV1(IndexNameInput))
  .handler(({ data }) =>
    mutate((adminToken) => deleteIndex(data.name, adminToken, upstreamDeps())),
  );

/**
 * Runs an admin-token mutation and turns its failure into an outcome. Without a configured admin
 * token it answers status `501` and a message for the operator, without any request to Logbrook.
 */
async function mutate(
  action: (adminToken: string) => Promise<void>,
): Promise<IndexMutationOutcome> {
  const { adminToken } = serverConfig();
  if (adminToken === null) {
    return {
      ok: false,
      status: 501,
      message:
        'Index management is unavailable: this server has no admin token. The operator must set LOGBROOK_ADMIN_TOKEN.',
      retryAfterSeconds: null,
    };
  }

  try {
    await action(adminToken);
    return { ok: true };
  } catch (error) {
    return failureOf(error);
  }
}

/**
 * Lists the indexes with `token`, turning every failure into an outcome.
 */
async function listWith(token: string): Promise<IndexListOutcome> {
  try {
    return { ok: true, indexes: await listIndexes(token, upstreamDeps()) };
  } catch (error) {
    return failureOf(error);
  }
}

/**
 * The Logbrook server to call and the `fetch` to call it with.
 */
function upstreamDeps() {
  return { upstreamUrl: serverConfig().logbrookUrl, fetch };
}
