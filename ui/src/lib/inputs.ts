import { Schema } from 'effect';

/**
 * Logbrook index names: 1 to 63 lowercase letters, digits, `-` or `_`, starting with a letter or
 * digit.
 */
const INDEX_NAME_PATTERN = /^[a-z0-9][a-z0-9_-]{0,62}$/;

/**
 * A valid Logbrook index name.
 */
export const IndexName = Schema.String.check(Schema.isPattern(INDEX_NAME_PATTERN));

/**
 * Input of the index create and delete server functions.
 */
export const IndexNameInput = Schema.Struct({ name: IndexName });
