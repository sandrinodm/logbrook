import { Rows3, Text, X } from 'lucide-react';

import type { TimeZoneMode } from '@/lib/explorer/format';

/**
 * Row density of the table.
 */
export type Density = 'compact' | 'comfortable';

/**
 * The display settings, all kept in the URL.
 */
type DisplayControlsProps = {
  /**
   * Time zone of the timestamps.
   */
  readonly zone: TimeZoneMode;

  /**
   * Row density.
   */
  readonly density: Density;

  /**
   * Whether long messages wrap onto several lines.
   */
  readonly wrap: boolean;

  /**
   * Attribute keys pinned as columns.
   */
  readonly columns: ReadonlyArray<string>;

  /**
   * Called with the new time zone.
   */
  readonly onZone: (zone: TimeZoneMode) => void;

  /**
   * Called with the new density.
   */
  readonly onDensity: (density: Density) => void;

  /**
   * Called with the new wrap setting.
   */
  readonly onWrap: (wrap: boolean) => void;

  /**
   * Called with a pinned key to unpin.
   */
  readonly onUnpin: (key: string) => void;
};

/**
 * Class names of a toggle button, by whether it is on.
 */
function toggleClass(on: boolean): string {
  const base =
    'inline-flex h-6 cursor-pointer items-center gap-1 rounded px-1.5 font-mono text-[11px]';
  return `${base} ${on ? 'bg-secondary text-foreground' : 'text-muted-foreground hover:text-foreground'}`;
}

/**
 * Strip above the table: time zone, density and wrap switches, and the pinned columns with a way to
 * unpin each. Everything here is a URL param, so a shared link looks the same.
 */
export function DisplayControls(props: DisplayControlsProps) {
  const { zone, density, wrap, columns } = props;
  return (
    <div className="flex flex-wrap items-center justify-end gap-1.5">
      {columns.map((key) => (
        <span
          key={key}
          className="inline-flex h-6 items-center gap-1 rounded border border-border bg-secondary px-2 font-mono text-[11px] text-foreground">
          <span className="text-muted-foreground">col</span>
          {key}
          <button
            type="button"
            aria-label={`Unpin column ${key}`}
            onClick={() => props.onUnpin(key)}
            className="cursor-pointer text-muted-foreground hover:text-foreground">
            <X className="h-2.5 w-2.5" />
          </button>
        </span>
      ))}

      <div className="inline-flex items-center rounded border border-border p-0.5">
        <button
          type="button"
          aria-pressed={zone === 'utc'}
          onClick={() => props.onZone('utc')}
          className={toggleClass(zone === 'utc')}>
          UTC
        </button>
        <button
          type="button"
          aria-pressed={zone === 'local'}
          onClick={() => props.onZone('local')}
          className={toggleClass(zone === 'local')}>
          Local
        </button>
      </div>

      <button
        type="button"
        aria-pressed={density === 'compact'}
        title="Compact rows"
        onClick={() => props.onDensity(density === 'compact' ? 'comfortable' : 'compact')}
        className={toggleClass(density === 'compact')}>
        <Rows3 className="h-3 w-3" />
        Compact
      </button>

      <button
        type="button"
        aria-pressed={wrap}
        title="Wrap long messages"
        onClick={() => props.onWrap(!wrap)}
        className={toggleClass(wrap)}>
        <Text className="h-3 w-3" />
        Wrap
      </button>
    </div>
  );
}
