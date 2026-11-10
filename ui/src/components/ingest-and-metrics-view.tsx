import { useState } from 'react';

import type { AccessCapabilities } from '@/lib/logbrook/capabilities';
import type { IndexInfo } from '@/lib/logbrook/types';
import type { MetricsState } from '@/lib/logbrook/use-metrics';

import { MetricsPanel } from '@/components/metrics-panel';

/**
 * Props of the Ingest and metrics view.
 */
type IngestAndMetricsViewProps = {
  /**
   * The indexes the read token can reach, for the ingest example.
   */
  readonly indexes: ReadonlyArray<IndexInfo>;

  /**
   * What the server's configured tokens can do.
   */
  readonly capabilities: AccessCapabilities;

  /**
   * The polled `/metrics` state.
   */
  readonly metrics: MetricsState;
};

/**
 * Two panes: how to send events to Logbrook, with what the server's tokens can reach, and the
 * server's metrics. The UI never ingests: sending events is for producers holding an ingestion
 * token, which the BFF deliberately does not proxy.
 */
export function IngestAndMetricsView({
  indexes,
  capabilities,
  metrics,
}: IngestAndMetricsViewProps) {
  return (
    <div className="grid flex-1 grid-cols-1 divide-y divide-border overflow-y-auto bg-background lg:grid-cols-12 lg:divide-x lg:divide-y-0">
      <div className="space-y-6 p-6 lg:col-span-5">
        <IngestGuide indexes={indexes} />
        <ServerAccess capabilities={capabilities} metricsAccess={metrics.access} />
      </div>
      <div className="p-6 lg:col-span-7">
        <MetricsPanel metrics={metrics} />
      </div>
    </div>
  );
}

/**
 * A curl example for `POST /indexes/{index}/logs/ingest` against a chosen index. It uses
 * placeholders for the server URL and the ingestion token, because the UI knows neither.
 */
function IngestGuide({ indexes }: { readonly indexes: ReadonlyArray<IndexInfo> }) {
  const [chosen, setChosen] = useState<string | null>(null);
  const index = chosen ?? indexes[0]?.name ?? 'default';
  const command = [
    `curl -X POST "$LOGBROOK_URL/indexes/${index}/logs/ingest" \\`,
    '  -H "Authorization: Bearer $INGEST_TOKEN" \\',
    '  -H "Content-Type: application/json" \\',
    '  -d "[{\\"time\\": $(date +%s)000, \\"level\\": 30, \\"msg\\": \\"hello\\"}]"',
  ].join('\n');

  return (
    <section className="space-y-3" aria-label="Sending events">
      <h2 className="text-xs font-medium text-foreground">Sending events</h2>
      <p className="text-xs text-muted-foreground">
        This UI only reads. Producers send batches to Logbrook directly with an ingestion token,
        which stamps the batch with its source. Ingesting into a missing index creates it.
      </p>
      <div>
        <label htmlFor="ingest-example-index" className="block text-[11px] text-muted-foreground">
          Index
        </label>
        <select
          id="ingest-example-index"
          value={index}
          onChange={(event) => setChosen(event.target.value)}
          className="mt-1 h-7.5 w-full rounded border border-border bg-background px-2 font-mono text-xs">
          {indexes.map((entry) => (
            <option key={entry.name} value={entry.name}>
              {entry.name}
            </option>
          ))}
        </select>
      </div>
      <pre className="overflow-x-auto rounded border border-border bg-secondary/30 p-3 font-mono text-[11px] leading-relaxed text-foreground">
        {command}
      </pre>
      <p className="text-[11px] text-quiet">
        By default a batch holds up to 1,000 events and 1 MiB, and commits atomically.
      </p>
    </section>
  );
}

/**
 * What the server's configured tokens are and what they reach. Logbrook cannot list its tokens, so
 * the section states only what the UI knows: whether an admin token is configured, and what the
 * index list and the metrics request showed about the read token. No token value is ever shown.
 */
function ServerAccess({
  capabilities,
  metricsAccess,
}: {
  readonly capabilities: AccessCapabilities;
  readonly metricsAccess: MetricsState['access'];
}) {
  return (
    <section className="space-y-2" aria-label="Server access">
      <h2 className="text-xs font-medium text-foreground">Server access</h2>
      <dl className="divide-y divide-border/60 border-t border-b border-border text-[11px]">
        <AccessRow
          label="Read token"
          state="held"
          detail={describeIndexDetail(capabilities.indexDetail)}
        />
        <AccessRow
          label="Metrics"
          state={metricsState(metricsAccess)}
          detail={describeMetrics(metricsAccess)}
        />
        <AccessRow
          label="Admin token"
          state={capabilities.canManageIndexes ? 'held' : 'not held'}
          detail={
            capabilities.canManageIndexes
              ? 'Can create and delete indexes.'
              : 'Indexes are read-only. The operator can set LOGBROOK_ADMIN_TOKEN to enable creating and deleting them.'
          }
        />
        <AccessRow
          label="Ingestion token"
          state="never held"
          detail="The UI does not store or use ingestion tokens."
        />
      </dl>
    </section>
  );
}

/**
 * One line of the access list.
 */
function AccessRow({
  label,
  state,
  detail,
}: {
  readonly label: string;
  readonly state: string;
  readonly detail: string;
}) {
  return (
    <div className="py-2">
      <div className="flex items-baseline justify-between gap-4">
        <dt className="font-mono text-foreground">{label}</dt>
        <dd className="font-mono text-muted-foreground">{state}</dd>
      </div>
      <p className="mt-0.5 text-[10px] text-quiet">{detail}</p>
    </div>
  );
}

/**
 * Describes what the index list showed about the read token.
 */
function describeIndexDetail(detail: AccessCapabilities['indexDetail']): string {
  if (detail === 'full') {
    return 'Lists indexes with sizes, retention and readiness.';
  }

  if (detail === 'names-only') {
    return 'Restricted to particular sources: lists index names only.';
  }

  return 'No indexes are visible to this token.';
}

/**
 * Short state word for the metrics row.
 */
function metricsState(access: MetricsState['access']): string {
  if (access === 'allowed') {
    return 'available';
  }

  return access === 'forbidden' ? 'refused (403)' : 'checking';
}

/**
 * Explains the metrics row.
 */
function describeMetrics(access: MetricsState['access']): string {
  if (access === 'forbidden') {
    return 'Metrics need a read token with no source or index restriction.';
  }

  return access === 'allowed'
    ? 'The read token has no source or index restriction.'
    : 'Waiting for the first scrape.';
}
