import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

/**
 * Joins class names, dropping falsy ones, and resolves conflicting Tailwind utilities so the last
 * one wins.
 */
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

/**
 * Presentation of one severity: its names and the Tailwind classes that colour it.
 */
export type LevelMeta = {
  /**
   * The numeric level this metadata describes.
   */
  level: number;

  /**
   * Full upper-case name, such as `ERROR`.
   */
  label: string;

  /**
   * Three-letter name for narrow columns, such as `ERR`.
   */
  shortLabel: string;

  /**
   * Classes for text in the level's colour.
   */
  textClass: string;

  /**
   * Classes for a badge in the level's colours.
   */
  badgeClass: string;

  /**
   * Classes for a small filled dot.
   */
  dotClass: string;

  /**
   * Classes for the accent on the left edge of a table row.
   */
  rowAccentClass: string;
};

/**
 * Returns the label and colours for a severity level, grouping levels the way Pino does: 60 and
 * above fatal, 50 error, 40 warn, 30 info, 20 debug, anything lower trace.
 */
export function getLevelMeta(level: number): LevelMeta {
  if (level >= 60) {
    return {
      level,
      label: 'FATAL',
      shortLabel: 'FTL',
      textClass: 'text-rose-600 dark:text-[#e55365] font-medium',
      badgeClass: 'bg-rose-500/10 text-rose-700 dark:text-[#e55365] border-rose-500/20',
      dotClass: 'bg-[#e55365]',
      rowAccentClass: 'border-l-2 border-l-[#e55365]/80 bg-rose-500/[0.02]',
    };
  }
  if (level >= 50) {
    return {
      level,
      label: 'ERROR',
      shortLabel: 'ERR',
      textClass: 'text-rose-600 dark:text-[#e05263]',
      badgeClass: 'bg-rose-500/8 text-rose-700 dark:text-[#e05263] border-rose-500/20',
      dotClass: 'bg-[#e05263]',
      rowAccentClass: 'border-l-2 border-l-[#e05263]/70',
    };
  }
  if (level >= 40) {
    return {
      level,
      label: 'WARN',
      shortLabel: 'WRN',
      textClass: 'text-amber-700 dark:text-[#d4973b]',
      badgeClass: 'bg-amber-500/8 text-amber-800 dark:text-[#d4973b] border-amber-500/20',
      dotClass: 'bg-[#d4973b]',
      rowAccentClass: 'border-l-2 border-l-[#d4973b]/60',
    };
  }
  if (level >= 30) {
    return {
      level,
      label: 'INFO',
      shortLabel: 'INF',
      textClass: 'text-muted-foreground',
      badgeClass: 'bg-secondary text-muted-foreground border-border',
      dotClass: 'bg-muted-foreground/50',
      rowAccentClass: 'border-l-2 border-l-transparent',
    };
  }
  if (level >= 20) {
    return {
      level,
      label: 'DEBUG',
      shortLabel: 'DBG',
      textClass: 'text-quiet',
      badgeClass: 'bg-secondary text-quiet border-border',
      dotClass: 'bg-quiet',
      rowAccentClass: 'border-l-2 border-l-transparent',
    };
  }
  return {
    level,
    label: 'TRACE',
    shortLabel: 'TRC',
    textClass: 'text-quiet',
    badgeClass: 'bg-secondary text-quiet border-border',
    dotClass: 'bg-quiet',
    rowAccentClass: 'border-l-2 border-l-transparent',
  };
}

/**
 * Formats Unix milliseconds as `HH:mm:ss.SSS` in UTC.
 */
export function formatShortTimeUtc(ms: number): string {
  const d = new Date(ms);
  return d.toISOString().slice(11, 23);
}
