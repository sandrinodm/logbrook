/**
 * Number of placeholder rows. Enough to fill a tall window; the rest is clipped.
 */
const SKELETON_ROWS = 40;

/**
 * Height of a skeleton row in pixels, equal to the log table's compact row height so the real rows
 * replace the skeleton without moving the page.
 */
const SKELETON_ROW_HEIGHT_PX = 24;

/**
 * Placeholder for the data views while their chunk or data loads, and the fallback the server
 * renders for client-only routes: a toolbar strip, a histogram block and rows of the same grid and
 * height as the log table.
 */
export function DataViewSkeleton() {
  const rows = Array.from({ length: SKELETON_ROWS }, (_, index) => index);
  return (
    <div className="flex flex-1 flex-col overflow-hidden" aria-busy="true">
      <div className="h-10 shrink-0 border-b border-border bg-card/40" />
      <div className="h-24 shrink-0 border-b border-border bg-card/20" />
      <div className="flex flex-1 flex-col gap-px overflow-hidden p-2">
        {rows.map((row) => (
          <div
            key={row}
            style={{ height: SKELETON_ROW_HEIGHT_PX }}
            className="shrink-0 animate-pulse rounded-sm bg-muted/60 motion-reduce:animate-none"
          />
        ))}
      </div>
    </div>
  );
}
