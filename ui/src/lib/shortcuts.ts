/**
 * One row of the shortcut help: the keys, and what they do.
 */
export type Shortcut = {
  /**
   * The keys to press, each as it is printed on the key. Alternatives are separate entries.
   */
  readonly keys: ReadonlyArray<string>;

  /**
   * What the shortcut does, in a few words.
   */
  readonly action: string;
};

/**
 * A titled list of shortcuts that work in the same place.
 */
export type ShortcutGroup = {
  /**
   * Where the shortcuts work.
   */
  readonly title: string;

  /**
   * The shortcuts of the group.
   */
  readonly shortcuts: ReadonlyArray<Shortcut>;
};

/**
 * Every keyboard shortcut of the app, as the help dialog lists them. Single keys are ignored while
 * the focus is in a text field; `Ctrl K` works there too.
 */
export const SHORTCUT_GROUPS: ReadonlyArray<ShortcutGroup> = [
  {
    title: 'Everywhere',
    shortcuts: [
      { keys: ['Ctrl K', '⌘ K'], action: 'Open the command palette' },
      { keys: ['t'], action: 'Switch between Logs and Live Tail, keeping the filters' },
      { keys: ['?'], action: 'Show this list' },
      { keys: ['Esc'], action: 'Leave a field, or close the open panel' },
    ],
  },
  {
    title: 'Logs',
    shortcuts: [
      { keys: ['j', '↓'], action: 'Select the next event and open it' },
      { keys: ['k', '↑'], action: 'Select the previous event and open it' },
      { keys: ['Enter'], action: 'Open the focused event' },
      { keys: ['/'], action: 'Search the message' },
      { keys: ['Esc'], action: 'Close the event panel' },
    ],
  },
  {
    title: 'Logs histogram (Tab to focus it)',
    shortcuts: [
      { keys: ['←', '→'], action: 'Move the cursor to the previous or next time bucket' },
      { keys: ['Home', 'End'], action: 'Jump to the first or the last bucket' },
      { keys: ['Shift ←', 'Shift →'], action: 'Extend the selection over more buckets' },
      { keys: ['Enter'], action: 'Zoom to the selection, or to the bucket under the cursor' },
      { keys: ['Esc'], action: 'Clear the selection' },
    ],
  },
  {
    title: 'Live Tail',
    shortcuts: [{ keys: ['/'], action: 'Search the message' }],
  },
];

/**
 * Whether a key event came from a text field, a select or an editable element.
 */
export function isEditableTarget(target: EventTarget | null): boolean {
  return (
    target instanceof HTMLElement &&
    (target.isContentEditable || ['INPUT', 'TEXTAREA', 'SELECT'].includes(target.tagName))
  );
}

/**
 * Whether a key event came from inside an open dialog (the palette, the help), where the page
 * behind is not the target and the dialog handles `Escape` itself.
 */
export function isDialogTarget(target: EventTarget | null): boolean {
  return target instanceof HTMLElement && target.closest('[role="dialog"]') !== null;
}

/**
 * Whether single-key shortcuts must stay out of this key event: the user is typing, or a dialog has
 * the focus.
 */
export function isTypingTarget(target: EventTarget | null): boolean {
  return isEditableTarget(target) || isDialogTarget(target);
}

/**
 * Whether a key event carries a modifier that makes it something other than a plain shortcut key.
 * Shift alone is allowed, because `?` needs it.
 */
export function hasCommandModifier(event: KeyboardEvent): boolean {
  return event.ctrlKey || event.metaKey || event.altKey;
}
