import { Collapsible } from '@base-ui/react/collapsible';
import { Popover } from '@base-ui/react/popover';
import { Switch } from '@base-ui/react/switch';
import { Tabs } from '@base-ui/react/tabs';
import { Tooltip } from '@base-ui/react/tooltip';
import { cva, type VariantProps } from 'class-variance-authority';
import * as React from 'react';

import { cn, getLevelMeta } from '@/lib/utils';

const buttonVariants = cva(
  'inline-flex cursor-pointer items-center justify-center gap-1.5 rounded text-xs font-normal whitespace-nowrap transition-colors select-none focus-visible:ring-1 focus-visible:ring-ring focus-visible:outline-none disabled:pointer-events-none disabled:opacity-40',
  {
    variants: {
      variant: {
        default: 'bg-foreground font-medium text-background hover:opacity-90',
        secondary:
          'border border-border bg-secondary text-secondary-foreground hover:bg-accent hover:text-foreground',
        outline:
          'border border-border bg-transparent text-muted-foreground hover:bg-accent hover:text-foreground',
        ghost: 'text-muted-foreground hover:bg-accent hover:text-foreground',
        destructive:
          'border border-rose-500/20 bg-rose-500/10 text-rose-600 hover:bg-rose-500/15 dark:text-[#e05263]',
      },
      size: {
        default: 'h-7 px-2.5',
        sm: 'h-6 px-2 text-[11px]',
        xs: 'h-5.5 px-1.5 text-[11px]',
        lg: 'h-8 px-3 text-xs',
        icon: 'h-7 w-7',
      },
    },
    defaultVariants: {
      variant: 'default',
      size: 'default',
    },
  },
);

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>, VariantProps<typeof buttonVariants> {}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, type = 'button', ...props }, ref) => (
    <button
      ref={ref}
      type={type}
      className={cn(buttonVariants({ variant, size, className }))}
      {...props}
    />
  ),
);
Button.displayName = 'Button';

const badgeVariants = cva(
  'inline-flex items-center gap-1 rounded border px-1.5 py-0 font-mono text-[10px] leading-4 font-normal select-none',
  {
    variants: {
      variant: {
        default: 'border-border bg-secondary text-foreground',
        secondary: 'border-border bg-secondary/60 text-muted-foreground',
        outline: 'border-border bg-transparent text-muted-foreground',
        emerald: 'border-border bg-secondary text-foreground',
        amber: 'border-amber-500/25 bg-amber-500/8 text-amber-700 dark:text-[#d4973b]',
        rose: 'border-rose-500/25 bg-rose-500/8 text-rose-600 dark:text-[#e05263]',
        sky: 'border-border bg-secondary text-muted-foreground',
      },
    },
    defaultVariants: {
      variant: 'secondary',
    },
  },
);

export interface BadgeProps
  extends React.HTMLAttributes<HTMLSpanElement>, VariantProps<typeof badgeVariants> {}

export function Badge({ className, variant, ...props }: BadgeProps) {
  return <span className={cn(badgeVariants({ variant }), className)} {...props} />;
}

export function LevelBadge({
  level,
  showNumeric = false,
  compact = false,
  className,
}: {
  level: number;
  showNumeric?: boolean;
  compact?: boolean;
  className?: string;
}) {
  const meta = getLevelMeta(level);
  if (compact) {
    return (
      <span
        className={cn(
          'inline-block font-mono text-[11px] tracking-tight tabular-nums select-none',
          meta.textClass,
          className,
        )}>
        {meta.shortLabel}
      </span>
    );
  }
  return (
    <span
      className={cn(
        'inline-flex items-center gap-1 rounded border px-1.5 py-0 font-mono text-[10px] leading-4 select-none',
        meta.badgeClass,
        className,
      )}>
      <span>{meta.shortLabel}</span>
      {showNumeric ? <span className="opacity-60">{level}</span> : null}
    </span>
  );
}

export function StorageTierBadge({
  tier,
  compact = false,
}: {
  tier: 'duckdb_hot' | 'parquet_archive';
  compact?: boolean;
}) {
  if (tier === 'duckdb_hot') {
    if (compact) {
      return null;
    }
    return <span className="font-mono text-[10px] text-muted-foreground">duckdb</span>;
  }
  return (
    <span
      title="Archived Parquet segment"
      className={cn(
        'rounded border border-border px-1 font-mono text-[10px] leading-4 text-quiet',
        compact ? '' : 'px-1.5',
      )}>
      parquet
    </span>
  );
}

export function Kbd({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <kbd
      className={cn(
        'inline-flex h-4 min-w-4 items-center justify-center rounded border border-border bg-secondary px-1 font-mono text-[10px] text-muted-foreground',
        className,
      )}>
      {children}
    </kbd>
  );
}

export function BaseSwitch({
  checked,
  onCheckedChange,
  label,
  className,
}: {
  checked: boolean;
  onCheckedChange: (checked: boolean) => void;
  label?: string;
  className?: string;
}) {
  return (
    <label
      className={cn(
        'inline-flex cursor-pointer items-center gap-1.5 text-[11px] text-muted-foreground select-none hover:text-foreground',
        className,
      )}>
      <Switch.Root
        checked={checked}
        onCheckedChange={onCheckedChange}
        className={cn(
          'relative inline-flex h-3.5 w-6 shrink-0 cursor-pointer rounded-full border border-border transition-colors duration-150',
          checked ? 'bg-foreground' : 'bg-secondary',
        )}>
        <Switch.Thumb
          className={cn(
            'pointer-events-none block h-2.5 w-2.5 translate-y-[1px] rounded-full transition-transform duration-150',
            checked ? 'translate-x-[11px] bg-background' : 'translate-x-[1px] bg-muted-foreground',
          )}
        />
      </Switch.Root>
      {label ? <span>{label}</span> : null}
    </label>
  );
}

export function BasePopover({
  trigger,
  children,
  align = 'start',
  sideOffset = 5,
  className,
  open,
  onOpenChange,
}: {
  trigger: React.ReactElement;
  children: React.ReactNode;
  align?: 'start' | 'center' | 'end';
  sideOffset?: number;
  className?: string;
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
}) {
  return (
    <Popover.Root open={open} onOpenChange={onOpenChange}>
      <Popover.Trigger render={trigger} />
      <Popover.Portal>
        <Popover.Positioner sideOffset={sideOffset} align={align} className="z-50">
          <Popover.Popup
            className={cn(
              'z-50 min-w-[200px] rounded-md border border-border bg-popover p-1.5 text-popover-foreground shadow-lg transition-all duration-100 outline-none data-[starting-style]:scale-98 data-[starting-style]:opacity-0',
              className,
            )}>
            {children}
          </Popover.Popup>
        </Popover.Positioner>
      </Popover.Portal>
    </Popover.Root>
  );
}

export function BaseTooltip({
  content,
  children,
}: {
  content: React.ReactNode;
  children: React.ReactElement;
}) {
  return (
    <Tooltip.Provider delay={200}>
      <Tooltip.Root>
        <Tooltip.Trigger render={children} />
        <Tooltip.Portal>
          <Tooltip.Positioner sideOffset={5} className="z-50">
            <Tooltip.Popup className="z-50 max-w-xs rounded border border-border bg-popover px-2 py-1 text-[11px] text-popover-foreground shadow-md">
              {content}
            </Tooltip.Popup>
          </Tooltip.Positioner>
        </Tooltip.Portal>
      </Tooltip.Root>
    </Tooltip.Provider>
  );
}

export { Tabs as BaseTabs, Collapsible as BaseCollapsible };
