import { Link } from '@tanstack/react-router';
import { useEffect, useState } from 'react';

import type { TailNotice } from '@/lib/logbrook/tail';
import type { TailSearch } from '@/lib/logbrook/tail-search';

import { Button } from '@/components/ui/reui-base';
import { tailStore } from '@/lib/logbrook/tail';
import { explorerSearchFor } from '@/lib/logbrook/tail-search';

/**
 * Props of `TailNoticeBanner`.
 */
type TailNoticeBannerProps = {
  /**
   * The condition to explain.
   */
  notice: TailNotice;

  /**
   * The tail's search params, carried into the explorer link.
   */
  search: TailSearch;

  /**
   * The index being followed.
   */
  index: string;
};

/**
 * Explains why the tail is not delivering events and what the reader can do: search the window it
 * missed in the explorer, retry, or restart. A rejected server token has no button: only the
 * operator can fix it.
 */
export function TailNoticeBanner({ notice, search, index }: TailNoticeBannerProps) {
  return (
    <output className="flex flex-wrap items-center justify-between gap-3 border-b border-border bg-secondary/40 px-4 py-2 text-xs">
      <span className="text-muted-foreground">
        <strong className="font-medium text-foreground">{headline(notice)}</strong> {detail(notice)}
        {notice.retryAt === null ? null : <Countdown retryAt={notice.retryAt} />}
      </span>
      <span className="flex items-center gap-1.5">
        {notice.window === null ? null : (
          <Link
            to="/"
            search={explorerSearchFor(search, index, notice.window)}
            className="inline-flex h-6 items-center rounded border border-border bg-secondary px-2 text-[11px] text-secondary-foreground hover:bg-accent">
            Search this window
          </Link>
        )}
        <NoticeActions kind={notice.kind} />
      </span>
    </output>
  );
}

/**
 * The buttons that fit a notice kind.
 */
function NoticeActions({ kind }: { kind: TailNotice['kind'] }) {
  if (kind === 'gap') {
    return (
      <Button size="sm" onClick={() => tailStore.restart()}>
        Restart from now
      </Button>
    );
  }

  if (kind === 'unauthorized' || kind === 'forbidden') {
    return null;
  }

  if (kind === 'limit' || kind === 'offline') {
    return (
      <Button size="sm" variant="secondary" onClick={() => tailStore.retryNow()}>
        Retry now
      </Button>
    );
  }

  return (
    <Button size="sm" variant="secondary" onClick={() => tailStore.reconnect()}>
      Retry
    </Button>
  );
}

/**
 * The bold first sentence of the banner for a notice kind.
 */
function headline(notice: TailNotice): string {
  switch (notice.kind) {
    case 'gap':
      return 'Some events were missed.';
    case 'error':
      return 'The tail stopped on a server error.';
    case 'limit':
      return 'Too many live tails are open.';
    case 'unauthorized':
      return "The server's Logbrook token was rejected.";
    case 'forbidden':
      return 'This token may not read that.';
    case 'failed':
      return 'The server refused the tail.';
    case 'offline':
      return 'Connection lost.';
  }
}

/**
 * The explanation after the headline. Server text is shown as sent; the subscriber limit gets a
 * fuller explanation because its server message is terse.
 */
function detail(notice: TailNotice): string {
  switch (notice.kind) {
    case 'limit':
      return 'The server limits live tails and shares that limit between all users. Close one in another tab or wait for a slot. ';
    case 'gap':
      return `${notice.message}. Search that window in the explorer to see what the tail skipped, or restart the tail from now.`;
    case 'unauthorized':
      return 'The operator must fix LOGBROOK_READ_TOKEN. ';
    case 'forbidden':
      return `${notice.message}. The read token's scope (the indexes and sources it is limited to) is the cause. Pick another index, remove the source filter, or ask the operator for a broader read token. `;
    case 'offline':
      return 'Reconnecting from the last event. ';
    default:
      return `${notice.message} `;
  }
}

/**
 * "Retrying in 12 s", counting down to `retryAt`.
 */
function Countdown({ retryAt }: { retryAt: number }) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1_000);
    return () => clearInterval(timer);
  }, []);

  const seconds = Math.max(0, Math.ceil((retryAt - now) / 1_000));
  return <span className="tabular-nums">Retrying in {seconds} s.</span>;
}
