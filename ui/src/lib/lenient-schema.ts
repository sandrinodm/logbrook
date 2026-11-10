import { Effect, Option, Schema } from 'effect';

/**
 * Wraps a schema so that a value that fails to decode counts as absent instead of failing the whole
 * search. A hand-edited or stale URL then loses one parameter rather than the page.
 */
export function lenient<S extends Schema.Top>(schema: S) {
  return Schema.optional(
    Schema.catchDecoding(() => Effect.succeed(Option.none<S['Type']>()))(schema),
  );
}
