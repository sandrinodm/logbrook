import { Button } from '@/components/ui/reui-base';
import { describeError } from '@/lib/explorer/describe-error';

/**
 * What the panel can do on the user's behalf.
 */
type ErrorPanelProps = {
  /**
   * The error to explain.
   */
  readonly error: unknown;

  /**
   * Runs the failed request again.
   */
  readonly onRetry: () => void;

  /**
   * Narrows the query, for errors that a smaller query avoids (`413`, `504`). Omit it when the view
   * has nothing to narrow.
   */
  readonly onNarrow?: () => void;
};

/**
 * Explains a failed request: a title, the cause in plain words, the server's own message when it
 * has one, and the action that fits the status (retry, narrow the range).
 */
export function ErrorPanel({ error, onRetry, onNarrow }: ErrorPanelProps) {
  const description = describeError(error);
  const waitHint =
    description.retryAfterSeconds === null
      ? null
      : `The server asked to wait ${description.retryAfterSeconds} s before the next try.`;

  return (
    <div
      role="alert"
      className="flex flex-1 flex-col items-center justify-center gap-2 px-6 py-12 text-center">
      <p className="text-sm font-medium text-foreground">{description.title}</p>
      <p className="max-w-md text-xs text-muted-foreground">{description.detail}</p>
      {waitHint === null ? null : <p className="text-xs text-muted-foreground">{waitHint}</p>}
      {description.serverMessage === null ? null : (
        <p className="max-w-lg rounded border border-border bg-secondary/40 px-2 py-1 font-mono text-[11px] break-words text-muted-foreground">
          {description.serverMessage}
        </p>
      )}
      <div className="mt-2 flex items-center gap-2">
        <Button variant="outline" size="sm" onClick={onRetry}>
          Try again
        </Button>
        {description.action === 'narrow' && onNarrow !== undefined ? (
          <Button variant="outline" size="sm" onClick={onNarrow}>
            Show the last 15 minutes
          </Button>
        ) : null}
      </div>
    </div>
  );
}
