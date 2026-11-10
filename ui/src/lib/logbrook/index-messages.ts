/**
 * Words for a refused index creation, keyed on the HTTP status. `429` is the index limit; the
 * server's own text is kept for anything else.
 */
export function describeCreateFailure(status: number, message: string): string {
  if (status === 429) {
    return 'Logbrook is at its index limit (4 by default, counting default). Delete an index you no longer need, or raise the limit in the server configuration.';
  }

  if (status === 401 || status === 403) {
    return 'Logbrook refused the admin token of this server. The operator must fix LOGBROOK_ADMIN_TOKEN.';
  }

  return message;
}

/**
 * Words for a refused index deletion. `400` is a protected index: `default`, or one declared in the
 * server configuration.
 */
export function describeDeleteFailure(status: number, message: string): string {
  if (status === 400) {
    return `${message}. default and indexes declared in the server configuration cannot be deleted; remove a configured index from the configuration and restart the server first.`;
  }

  if (status === 404) {
    return 'That index no longer exists.';
  }

  if (status === 401 || status === 403) {
    return 'Logbrook refused the admin token of this server. The operator must fix LOGBROOK_ADMIN_TOKEN.';
  }

  return message;
}
