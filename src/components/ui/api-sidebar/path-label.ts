import spec from "@/api/openapi.json";

/**
 * Sidebar label for an operation: its path as written in the spec
 * (`/indexes/{index}/logs`). The method chip beside it already shows the verb,
 * and the operation's summary is the page heading.
 *
 * Nimbus identifies an operation by its `operationId`, or without one by a
 * coordinate of the form `get/indexes/index/logs`: the method, then the path
 * without braces.
 */
const paths = new Map<string, string>();

for (const [path, item] of Object.entries(spec.paths as Record<string, Record<string, { operationId?: string }>>)) {
  for (const [method, operation] of Object.entries(item)) {
    paths.set(operation.operationId ?? `${method}${path.replace(/[{}]/g, "")}`, path);
  }
}

export function pathLabel(coordinate: string, fallback: string): string {
  return paths.get(coordinate) ?? fallback;
}
