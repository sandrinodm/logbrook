import { Progress } from '@base-ui/react/progress';

import { cn } from '@/lib/utils';

export function BaseProgress({
  value,
  max = 100,
  indicatorClassName,
  className,
}: {
  value: number;
  max?: number;
  indicatorClassName?: string;
  className?: string;
}) {
  const pct = Math.min(100, Math.max(0, (value / max) * 100));
  return (
    <Progress.Root value={pct} max={100} className={cn('w-full', className)}>
      <Progress.Track className="h-1 w-full overflow-hidden rounded-full bg-secondary">
        <Progress.Indicator
          className={cn('h-full bg-foreground/65 transition-all duration-300', indicatorClassName)}
          style={{ width: `${pct}%` }}
        />
      </Progress.Track>
    </Progress.Root>
  );
}
