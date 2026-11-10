import { BaseDialog } from '@/components/ui/base-dialog';
import { Kbd } from '@/components/ui/reui-base';
import { SHORTCUT_GROUPS } from '@/lib/shortcuts';

/**
 * Props of `ShortcutHelp`.
 */
type ShortcutHelpProps = {
  /**
   * Whether the dialog is showing.
   */
  readonly open: boolean;

  /**
   * Called when the dialog asks to open or close (Escape, the close button, a click outside).
   */
  readonly onOpenChange: (open: boolean) => void;
};

/**
 * Dialog that lists every keyboard shortcut. Focus moves into it when it opens and returns to where
 * it was when it closes.
 */
export function ShortcutHelp({ open, onOpenChange }: ShortcutHelpProps) {
  return (
    <BaseDialog
      open={open}
      onOpenChange={onOpenChange}
      title="Keyboard shortcuts"
      description="Single keys are ignored while you are typing in a field."
      maxWidthClass="max-w-md">
      <div className="space-y-4">
        {SHORTCUT_GROUPS.map((group) => (
          <section key={group.title} aria-label={group.title}>
            <h3 className="mb-1 font-mono text-[10px] text-quiet uppercase">{group.title}</h3>
            <dl className="space-y-1">
              {group.shortcuts.map((shortcut) => (
                <div key={shortcut.action} className="flex items-center justify-between gap-4">
                  <dt className="text-xs text-foreground">{shortcut.action}</dt>
                  <dd className="flex shrink-0 items-center gap-1">
                    {shortcut.keys.map((keys) => (
                      <Kbd key={keys}>{keys}</Kbd>
                    ))}
                  </dd>
                </div>
              ))}
            </dl>
          </section>
        ))}
      </div>
    </BaseDialog>
  );
}
