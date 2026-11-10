import { Schema } from 'effect';
import { useState } from 'react';

import type { IndexMutationOutcome } from '@/functions/indexes.functions';

import { BaseDialog } from '@/components/ui/base-dialog';
import { Button } from '@/components/ui/reui-base';
import { IndexName } from '@/lib/inputs';
import { settleOutcome } from '@/lib/logbrook/index-list-outcome';
import { describeCreateFailure, describeDeleteFailure } from '@/lib/logbrook/index-messages';

/**
 * What the create dialog needs: the names that exist, and how to create one.
 */
type CreateIndexDialogProps = {
  /**
   * Names of the indexes that exist, to say so instead of sending a no-op `PUT`.
   */
  readonly existingNames: ReadonlyArray<string>;

  /**
   * Creates the index on the server and resolves with the outcome.
   */
  readonly onCreate: (name: string) => Promise<IndexMutationOutcome>;
};

/**
 * What the delete dialog needs: the index to delete, or `null` when closed.
 */
type DeleteIndexDialogProps = {
  /**
   * Name of the index to delete, or `null` while the dialog is closed.
   */
  readonly target: string | null;

  /**
   * Deletes the index on the server and resolves with the outcome.
   */
  readonly onDelete: (name: string) => Promise<IndexMutationOutcome>;

  /**
   * Closes the dialog.
   */
  readonly onClose: () => void;
};

/**
 * Button and dialog that create an index (`PUT /indexes/{index}`). The name is checked against the
 * server's rules before the request, and a refusal such as the `429` index limit is shown in the
 * dialog, which stays open.
 */
export function CreateIndexDialog({ existingNames, onCreate }: CreateIndexDialogProps) {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  /**
   * Validates the name, creates the index and closes the dialog on success.
   */
  const submit = async (event: React.SyntheticEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmed = name.trim();
    if (!Schema.is(IndexName)(trimmed)) {
      setError(
        'Use 1 to 63 lowercase letters, digits, hyphens or underscores, starting with a letter or digit.',
      );
      return;
    }

    if (existingNames.includes(trimmed)) {
      setError(`An index named ${trimmed} already exists.`);
      return;
    }

    setPending(true);
    const outcome = await settleOutcome(onCreate(trimmed)).finally(() => setPending(false));
    if (outcome.ok) {
      setName('');
      setError(null);
      setOpen(false);
    } else {
      setError(describeCreateFailure(outcome.status, outcome.message));
    }
  };

  return (
    <BaseDialog
      open={open}
      onOpenChange={setOpen}
      trigger={<Button size="sm">New index</Button>}
      title="Create an index"
      description="Creates an empty index with the server's default retention and size limit. Both are set in the server configuration."
      maxWidthClass="max-w-md">
      <form onSubmit={submit} className="space-y-3">
        <div>
          <label htmlFor="new-index-name" className="block text-[11px] text-muted-foreground">
            Name
          </label>
          <input
            id="new-index-name"
            type="text"
            value={name}
            autoComplete="off"
            spellCheck={false}
            onChange={(event) => {
              setName(event.target.value);
              setError(null);
            }}
            placeholder="billing-webhooks"
            className="mt-1 h-7.5 w-full rounded border border-border bg-background px-2.5 font-mono text-xs"
          />
        </div>

        {error === null ? null : (
          <p role="alert" className="text-xs text-[#e05263]">
            {error}
          </p>
        )}

        <div className="flex justify-end gap-2 pt-2">
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button type="submit" disabled={pending || name.trim() === ''}>
            {pending ? 'Creating…' : 'Create'}
          </Button>
        </div>
      </form>
    </BaseDialog>
  );
}

/**
 * Dialog that deletes an index after the user types its name. `default` and configured indexes are
 * refused by the server with a `400`, which the dialog explains.
 */
export function DeleteIndexDialog({ target, onDelete, onClose }: DeleteIndexDialogProps) {
  return (
    <BaseDialog
      open={target !== null}
      onOpenChange={(open) => {
        if (!open) {
          onClose();
        }
      }}
      title={`Delete ${target ?? ''}?`}
      description="Permanently removes the index's database and archive files."
      maxWidthClass="max-w-md">
      {target === null ? null : (
        <DeleteForm key={target} target={target} onDelete={onDelete} onClose={onClose} />
      )}
    </BaseDialog>
  );
}

/**
 * The typed-name confirmation inside the delete dialog. It is keyed on the target, so its state
 * starts empty for every index.
 */
function DeleteForm({
  target,
  onDelete,
  onClose,
}: {
  readonly target: string;
  readonly onDelete: DeleteIndexDialogProps['onDelete'];
  readonly onClose: () => void;
}) {
  const [typed, setTyped] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  /**
   * Deletes the index once the typed name matches, and closes the dialog on success.
   */
  const submit = async (event: React.SyntheticEvent<HTMLFormElement>) => {
    event.preventDefault();
    setPending(true);
    const outcome = await settleOutcome(onDelete(target)).finally(() => setPending(false));
    if (outcome.ok) {
      onClose();
    } else {
      setError(describeDeleteFailure(outcome.status, outcome.message));
    }
  };

  return (
    <form onSubmit={submit} className="space-y-3 text-xs text-muted-foreground">
      <p>
        Stop producers that write to <code className="text-foreground">{target}</code> first: a
        later ingestion recreates an empty index.
      </p>
      <div>
        <label htmlFor="delete-index-confirm" className="block text-[11px]">
          Type <code className="text-foreground">{target}</code> to confirm
        </label>
        <input
          id="delete-index-confirm"
          type="text"
          value={typed}
          autoComplete="off"
          spellCheck={false}
          onChange={(event) => {
            setTyped(event.target.value);
            setError(null);
          }}
          className="mt-1 h-7.5 w-full rounded border border-border bg-background px-2.5 font-mono text-xs text-foreground"
        />
      </div>

      {error === null ? null : (
        <p role="alert" className="text-[#e05263]">
          {error}
        </p>
      )}

      <div className="flex justify-end gap-2">
        <Button variant="outline" onClick={onClose}>
          Cancel
        </Button>
        <Button type="submit" variant="destructive" disabled={pending || typed !== target}>
          {pending ? 'Deleting…' : 'Delete index'}
        </Button>
      </div>
    </form>
  );
}
