/**
 * Number of placeholder rows in the panel skeleton.
 */
const SKELETON_ROWS = 6;

/**
 * Placeholder for the Indexes and Ingest views while their data loads: a strip and a few table
 * rows, in the places the real ones take. The server renders it as the fallback for these
 * client-only routes.
 */
export function PanelSkeleton() {
  const rows = Array.from({ length: SKELETON_ROWS }, (_, index) => index);
  return (
    <div className="flex flex-1 flex-col overflow-hidden" aria-busy="true">
      <div className="h-12 shrink-0 border-b border-border bg-card/40" />
      <div className="flex flex-col gap-px p-2">
        {rows.map((row) => (
          <div
            key={row}
            className="h-12 animate-pulse rounded-sm bg-muted/60 motion-reduce:animate-none"
          />
        ))}
      </div>
    </div>
  );
}
