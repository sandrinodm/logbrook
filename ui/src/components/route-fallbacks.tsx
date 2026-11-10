import type { ErrorComponentProps } from '@tanstack/react-router';

import { Link } from '@tanstack/react-router';

import { Button } from '@/components/ui/reui-base';

/**
 * Shown inside the app frame when a route's loader or component throws: the error message and a way
 * to try again.
 */
export function RouteError({ error, reset }: ErrorComponentProps) {
  return (
    <div className="flex flex-1 flex-col items-center justify-center gap-3 p-6 text-center">
      <p className="text-sm font-medium text-foreground">Something went wrong</p>
      <p className="max-w-md font-mono text-xs break-words text-muted-foreground">
        {error instanceof Error ? error.message : String(error)}
      </p>
      <Button variant="outline" size="sm" onClick={reset}>
        Try again
      </Button>
    </div>
  );
}

/**
 * Shown for a URL no route matches, with a link back to the explorer.
 */
export function RouteNotFound() {
  return (
    <div className="flex flex-1 flex-col items-center justify-center gap-3 p-6 text-center">
      <p className="text-sm font-medium text-foreground">Page not found</p>
      <Link to="/" className="text-xs text-muted-foreground underline hover:text-foreground">
        Back to logs
      </Link>
    </div>
  );
}
