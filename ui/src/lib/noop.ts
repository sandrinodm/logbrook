/**
 * Does nothing. Passed to `.catch` where a failed prefetch is deliberately ignored, because the
 * component that needs the data reads the query and shows the error itself.
 */
export function noop(): void {}
