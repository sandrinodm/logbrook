import { Button } from '@/components/ui/reui-base';
import { describeError, nextPageOptions } from '@/lib/explorer/describe-error';

/**
 * Props of `NextPageError`.
 */
type NextPageErrorProps = {
  /**
   * The error of the failed page request.
   */
  readonly error: unknown;

  /**
   * Requests the same page again.
   */
  readonly onRetry: () => void;

  /**
   * Starts the search over from the newest event.
   */
  readonly onRestart: () => void;

  /**
   * Narrows the time range.
   */
  readonly onNarrow: () => void;
};

/**
 * Strip under the loaded rows that says an older page could not be loaded. The rows stay on screen;
 * the actions concern only the failed page: try it again, or, when the server refused the cursor
 * (it expired), start the search over.
 */
export function NextPageError({ error, onRetry, onRestart, onNarrow }: NextPageErrorProps) {
  const description = describeError(error);
  const options = nextPageOptions(error);
  return (
    <div
      role="alert"
      className="flex flex-wrap items-center gap-x-3 gap-y-1 border-b border-border bg-amber-500/10 px-3 py-1.5 font-sans text-xs text-foreground">
      <span>
        <strong className="font-medium">Older events could not be loaded.</strong>{' '}
        {description.title}.{' '}
        {description.serverMessage === null ? null : (
          <span className="font-mono text-[11px] text-muted-foreground">
            {description.serverMessage}
          </span>
        )}
      </span>
      <span className="flex items-center gap-2">
        {options.retry ? (
          <Button variant="outline" size="sm" onClick={onRetry}>
            Try this page again
          </Button>
        ) : null}
        {options.restart ? (
          <Button variant="outline" size="sm" onClick={onRestart}>
            Start the search again
          </Button>
        ) : null}
        {options.narrow ? (
          <Button variant="outline" size="sm" onClick={onNarrow}>
            Show the last 15 minutes
          </Button>
        ) : null}
      </span>
    </div>
  );
}
