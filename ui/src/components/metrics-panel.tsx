import type { ReactNode } from 'react';

import type { MetricsState } from '@/lib/logbrook/use-metrics';

import { BaseProgress } from '@/components/ui/base-progress';
import { Button } from '@/components/ui/reui-base';
import { READ_TOKEN_REJECTED_MESSAGE } from '@/lib/logbrook/errors';
import {
  formatBytes,
  formatCount,
  formatRate,
  formatSeconds,
  usagePercent,
} from '@/lib/logbrook/format';
import {
  counterWentBack,
  ratesBetween,
  type IndexStorageMetrics,
  type MetricsSnapshot,
} from '@/lib/logbrook/metrics';
import { METRICS_POLL_MS } from '@/lib/logbrook/metrics-query';

/**
 * Server metrics from `/metrics`: the state of the server, request and ingest totals with rates
 * between scrapes, admission budgets, latency per endpoint and storage per index. A `403` shows why
 * instead of an error, because only an unrestricted read token may read metrics.
 */
export function MetricsPanel({ metrics }: { readonly metrics: MetricsState }) {
  return (
    <section className="space-y-4" aria-label="Server metrics">
      <div className="flex items-baseline justify-between">
        <h2 className="text-xs font-medium text-foreground">Server metrics</h2>
        <span className="font-mono text-[11px] text-quiet">
          GET /metrics every {METRICS_POLL_MS / 1000} s
        </span>
      </div>
      <MetricsBody metrics={metrics} />
    </section>
  );
}

/**
 * Picks what the panel shows: the refusal, an error, the loading line or the scrape.
 */
function MetricsBody({ metrics }: { readonly metrics: MetricsState }) {
  if (metrics.access === 'forbidden') {
    return <MetricsForbidden />;
  }

  if (metrics.current === null) {
    return metrics.errorMessage === null ? (
      <p className="text-xs text-muted-foreground">Loading metrics…</p>
    ) : (
      <MetricsFailed
        message={failureMessage(metrics)}
        rejected={metrics.errorStatus === 401}
        onRetry={metrics.refetch}
      />
    );
  }

  return (
    <>
      {metrics.errorMessage === null ? null : (
        <MetricsStale
          takenAt={metrics.current.takenAt}
          message={failureMessage(metrics)}
          rejected={metrics.errorStatus === 401}
          onRetry={metrics.refetch}
        />
      )}
      <MetricsContent current={metrics.current} previous={metrics.previous} />
    </>
  );
}

/**
 * Warns that the figures below are the last good scrape and no longer live: the time of that scrape
 * and why the newer ones failed. A `401` offers no retry, because only the operator can fix the
 * token.
 */
function MetricsStale({
  takenAt,
  message,
  rejected,
  onRetry,
}: {
  readonly takenAt: number;
  readonly message: string;
  readonly rejected: boolean;
  readonly onRetry: () => void;
}) {
  return (
    <div
      role="alert"
      className="mb-4 flex flex-wrap items-center gap-x-3 gap-y-1 rounded border border-border bg-amber-500/10 px-3 py-2 text-xs text-foreground">
      <span>
        <strong className="font-medium">These figures are out of date.</strong> The last good scrape
        was at <time dateTime={new Date(takenAt).toISOString()}>{formatTime(takenAt)}</time>; the
        newer ones failed: {message}
      </span>
      <MetricsFailureAction rejected={rejected} onRetry={onRetry} />
    </div>
  );
}

/**
 * The action after a failed scrape: none when the server's token was rejected, else try again.
 */
function MetricsFailureAction({
  rejected,
  onRetry,
}: {
  readonly rejected: boolean;
  readonly onRetry: () => void;
}) {
  if (rejected) {
    return null;
  }

  return (
    <Button variant="outline" size="sm" onClick={onRetry}>
      Try again
    </Button>
  );
}

/**
 * The words for the failed scrape: the operator message for a `401`, else the error's own text.
 */
function failureMessage(metrics: MetricsState): string {
  return metrics.errorStatus === 401 ? READ_TOKEN_REJECTED_MESSAGE : (metrics.errorMessage ?? '');
}

/**
 * Formats a Unix-millisecond time as a local clock time with seconds.
 */
function formatTime(millis: number): string {
  return new Date(millis).toLocaleTimeString();
}

/**
 * Explains the `403`: metrics reveal every index's footprint, so Logbrook only gives them to a read
 * token with no source or index restriction.
 */
function MetricsForbidden() {
  return (
    <div className="rounded border border-border bg-card/40 p-4 text-xs text-muted-foreground">
      <p className="font-medium text-foreground">Metrics are not available to this token</p>
      <p className="mt-1">
        Logbrook answered 403: <code>/metrics</code> needs a read token with no source or index
        restriction, and the server's configured read token is restricted. The operator can
        configure an unrestricted read token to show server metrics.
      </p>
    </div>
  );
}

/**
 * Shows a failed first request that is not a refusal, with a retry button unless the server's token
 * was rejected (`401`).
 */
function MetricsFailed({
  message,
  rejected,
  onRetry,
}: {
  readonly message: string;
  readonly rejected: boolean;
  readonly onRetry: () => void;
}) {
  return (
    <div className="space-y-2 text-xs">
      <p role="alert" className="text-[#e05263]">
        Could not read metrics: {message}
      </p>
      <MetricsFailureAction rejected={rejected} onRetry={onRetry} />
    </div>
  );
}

/**
 * All sections of one scrape. Rates come from the previous scrape when there is one; a counter that
 * went down means the server restarted, and the rates wait for the next scrape.
 */
function MetricsContent({
  current,
  previous,
}: {
  readonly current: MetricsSnapshot;
  readonly previous: MetricsSnapshot | null;
}) {
  const restarted = previous !== null && counterWentBack(previous.summary, current.summary);
  const rates = previous === null ? null : ratesBetween(previous, current);
  const { counters, gauges } = current.summary;
  const elapsedSeconds =
    previous === null ? null : Math.round((current.takenAt - previous.takenAt) / 1000);

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-center gap-x-4 gap-y-1 font-mono text-[11px]">
        <ReadyBadge ready={gauges.ready} />
        <span className="text-quiet">
          Totals count since the server process started and reset when it restarts.
        </span>
      </div>

      {restarted ? (
        <output className="block text-xs text-[#d4973b]">
          The server restarted since the previous scrape: its counters began again from zero, so
          rates resume with the next one.
        </output>
      ) : null}

      <Group title="Ingest and queries" note={rateNote(elapsedSeconds)}>
        <Stat
          name="logbrook_ingested_events_total"
          label="Events ingested"
          value={formatCount(counters.ingestedEvents)}
          rate={restarted ? null : rates?.ingestedEvents}
          unit="events/s"
        />
        <Stat
          name="logbrook_ingest_rejected_total"
          label="Ingest requests rejected"
          value={formatCount(counters.ingestRejected)}
        />
        <Stat
          name="logbrook_http_requests_total"
          label="HTTP requests"
          value={formatCount(counters.httpRequests)}
          rate={restarted ? null : rates?.httpRequests}
          unit="req/s"
        />
        <Stat
          name="logbrook_http_errors_total"
          label="HTTP error responses"
          value={formatCount(counters.httpErrors)}
        />
        <Stat
          name="logbrook_query_requests_total"
          label="Queries"
          value={formatCount(counters.queryRequests)}
          rate={restarted ? null : rates?.queryRequests}
          unit="req/s"
        />
        <Stat
          name="logbrook_query_errors_total"
          label="Query errors"
          value={formatCount(counters.queryErrors)}
        />
      </Group>

      <Group title="Load now">
        <Stat
          name="logbrook_http_active_requests"
          label="Active requests"
          value={formatCount(gauges.activeRequests)}
        />
        <Stat
          name="logbrook_tail_active_subscribers"
          label="Live tail subscribers"
          value={formatCount(gauges.tailSubscribers)}
        />
        <Stat
          name="logbrook_tail_gaps_total"
          label="Tail gaps"
          value={formatCount(counters.tailGaps)}
        />
        <Stat
          name="logbrook_ingest_available_requests"
          label="Free request permits"
          value={formatCount(gauges.availableRequests)}
        />
        <Stat
          name="logbrook_decoded_available_bytes"
          label="Free decoded memory"
          value={formatBytes(gauges.decodedAvailableBytes)}
        />
        <Stat
          name="logbrook_raw_available_bytes"
          label="Free request-body memory"
          value={formatBytes(gauges.rawAvailableBytes)}
        />
      </Group>

      <Overloads causes={current.summary.overloads} />
      <Latency endpoints={current.summary.endpoints} />
      <Storage indexes={current.summary.indexes} />
    </div>
  );
}

/**
 * Says what window the rates cover.
 */
function rateNote(elapsedSeconds: number | null): string {
  return elapsedSeconds === null
    ? 'Rates appear after the second scrape.'
    : `Rates are the change over the last ${elapsedSeconds} s.`;
}

/**
 * Readiness of the whole server: every index's storage is available.
 */
function ReadyBadge({ ready }: { readonly ready: boolean | null }) {
  if (ready === null) {
    return <span className="text-quiet">readiness unknown</span>;
  }

  return ready ? (
    <span className="text-foreground">storage ready</span>
  ) : (
    <span className="text-[#e05263]">storage not ready</span>
  );
}

/**
 * A titled block of statistics.
 */
function Group({
  title,
  note,
  children,
}: {
  readonly title: string;
  readonly note?: string;
  readonly children: ReactNode;
}) {
  return (
    <div className="space-y-1">
      <div className="flex items-baseline justify-between">
        <h3 className="text-xs font-medium text-foreground">{title}</h3>
        {note === undefined ? null : <span className="text-[10px] text-quiet">{note}</span>}
      </div>
      <dl className="divide-y divide-border/60 border-t border-b border-border font-mono text-[11px] tabular-nums">
        {children}
      </dl>
    </div>
  );
}

/**
 * One statistic: a label with the series name beneath, the value, and optionally a rate.
 */
function Stat({
  name,
  label,
  value,
  rate,
  unit,
}: {
  readonly name: string;
  readonly label: string;
  readonly value: string;
  readonly rate?: number | null | undefined;
  readonly unit?: string;
}) {
  return (
    <div className="flex items-center justify-between gap-4 py-1.5">
      <dt>
        <div className="text-foreground">{label}</div>
        <div className="text-[10px] text-quiet">{name}</div>
      </dt>
      <dd className="text-right">
        <div className="text-foreground">{value}</div>
        {unit === undefined || rate === undefined ? null : (
          <div className="text-[10px] text-quiet">
            {formatRate(rate)} {unit}
          </div>
        )}
      </dd>
    </div>
  );
}

/**
 * Admission rejections by cause, or a line saying there were none.
 */
function Overloads({
  causes,
}: {
  readonly causes: ReadonlyArray<{ readonly cause: string; readonly count: number }>;
}) {
  return (
    <Group title="Requests refused under load">
      {causes.length === 0 ? (
        <p className="py-1.5 text-quiet">None since the server started.</p>
      ) : (
        causes.map((entry) => (
          <Stat
            key={entry.cause}
            name={`logbrook_http_overload_total{cause="${entry.cause}"}`}
            label={entry.cause}
            value={formatCount(entry.count)}
          />
        ))
      )}
    </Group>
  );
}

/**
 * Latency per endpoint since the server started: mean, and the 95th percentile as the upper bound
 * of its histogram bucket.
 */
function Latency({ endpoints }: { readonly endpoints: MetricsSnapshot['summary']['endpoints'] }) {
  if (endpoints.length === 0) {
    return null;
  }

  return (
    <div className="space-y-1">
      <h3 className="text-xs font-medium text-foreground">Handler latency by endpoint</h3>
      <table className="w-full border-collapse border-t border-b border-border text-left font-mono text-[11px] tabular-nums">
        <thead>
          <tr className="text-[10px] text-quiet">
            <th className="py-1 font-normal">Endpoint</th>
            <th className="py-1 text-right font-normal">Requests</th>
            <th className="py-1 text-right font-normal">Mean</th>
            <th className="py-1 text-right font-normal">p95 at most</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border/60">
          {endpoints.map((endpoint) => (
            <tr key={endpoint.endpoint}>
              <td className="py-1.5 text-foreground">{endpoint.endpoint}</td>
              <td className="py-1.5 text-right">{formatCount(endpoint.count)}</td>
              <td className="py-1.5 text-right">{formatSeconds(endpoint.meanSeconds)}</td>
              <td className="py-1.5 text-right">{formatSeconds(endpoint.p95UpperBoundSeconds)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/**
 * Storage diagnostics per index: size against its target, whether ingestion is paused at the
 * target, and worker and failure counters.
 */
function Storage({ indexes }: { readonly indexes: ReadonlyArray<IndexStorageMetrics> }) {
  if (indexes.length === 0) {
    return null;
  }

  return (
    <div className="space-y-1">
      <h3 className="text-xs font-medium text-foreground">Storage by index</h3>
      <div className="divide-y divide-border/60 border-t border-b border-border font-mono text-[11px] tabular-nums">
        {indexes.map((index) => (
          <StorageRow key={index.index} index={index} />
        ))}
      </div>
    </div>
  );
}

/**
 * One index's storage line: size against target, then queue, readers and failure counters.
 */
function StorageRow({ index }: { readonly index: IndexStorageMetrics }) {
  const percent = usagePercent(index.sizeBytes ?? undefined, index.sizeTargetBytes);
  return (
    <div className="space-y-1 py-2">
      <div className="flex justify-between">
        <span className="text-foreground">{index.index}</span>
        <span className="text-muted-foreground">
          {formatBytes(index.sizeBytes)} /{' '}
          {index.sizeTargetBytes === null ? 'no limit' : formatBytes(index.sizeTargetBytes)}
        </span>
      </div>
      {percent === null ? null : <BaseProgress value={percent} max={100} />}
      <div className="flex flex-wrap gap-x-4 text-[10px] text-quiet">
        {index.sizePaused ? (
          <span className="text-[#d4973b]">ingestion paused at size limit</span>
        ) : null}
        <span>queue used {formatCount(index.queueUsed)}</span>
        <span>readers busy {formatCount(index.readersBusy)}</span>
        <span>query timeouts {formatCount(index.queryTimeouts)}</span>
        <span>maintenance failures {formatCount(index.maintenanceFailures)}</span>
        <span>archive files {formatCount(index.archiveFiles)}</span>
      </div>
    </div>
  );
}
