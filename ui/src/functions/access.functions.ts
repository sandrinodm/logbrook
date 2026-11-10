import { createServerFn } from '@tanstack/react-start';

import { serverConfig } from '../server/config.server.ts';

/**
 * What the UI may know about the access the server has to Logbrook: facts and booleans, never a
 * token.
 */
export type AccessInfo = {
  /**
   * Whether the server has an admin token, which enables creating and deleting indexes.
   */
  readonly canManageIndexes: boolean;
};

/**
 * Reports what the server's configured tokens allow. Whoever can reach the app gets this access;
 * the app does no authentication of its own.
 */
export const getAccessFn = createServerFn({ method: 'GET' }).handler((): AccessInfo => ({
  canManageIndexes: serverConfig().adminToken !== null,
}));
