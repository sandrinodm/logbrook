import { Dialog } from '@base-ui/react/dialog';
import * as React from 'react';

import { cn } from '@/lib/utils';

export function BaseDialog({
  open,
  onOpenChange,
  trigger,
  title,
  description,
  children,
  maxWidthClass = 'max-w-xl',
}: {
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
  trigger?: React.ReactElement;
  title: React.ReactNode;
  description?: React.ReactNode;
  children: React.ReactNode;
  maxWidthClass?: string;
}) {
  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      {trigger ? <Dialog.Trigger render={trigger} /> : null}
      <Dialog.Portal>
        <Dialog.Backdrop className="fixed inset-0 z-50 bg-black/50 transition-opacity duration-100 data-[starting-style]:opacity-0" />
        <Dialog.Popup
          className={cn(
            'fixed top-1/2 left-1/2 z-50 w-[92vw] -translate-x-1/2 -translate-y-1/2 rounded-lg border border-border bg-popover p-4 text-popover-foreground shadow-xl outline-none',
            maxWidthClass,
          )}>
          <div className="flex items-start justify-between gap-4 border-b border-border pb-3">
            <div>
              <Dialog.Title className="text-xs font-medium text-foreground">{title}</Dialog.Title>
              {description ? (
                <Dialog.Description className="mt-0.5 text-[11px] text-muted-foreground">
                  {description}
                </Dialog.Description>
              ) : null}
            </div>
            <Dialog.Close
              aria-label="Close"
              className="cursor-pointer rounded p-1 text-xs text-muted-foreground hover:bg-accent hover:text-foreground">
              ✕
            </Dialog.Close>
          </div>
          <div className="mt-3">{children}</div>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  );
}
