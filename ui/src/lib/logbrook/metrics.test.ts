import { describe, expect, it } from 'vite-plus/test';

import type { MetricsSnapshot } from './metrics.ts';

import { counterRate, counterWentBack, ratesBetween, summarizeMetrics } from './metrics.ts';
import { parsePrometheusText } from './prometheus.ts';

/**
 * A scrape in the shape `src/http/metrics.rs` produces, with two indexes.
 */
const SCRAPE = `
# TYPE logbrook_ingested_events_total counter
logbrook_ingested_events_total 1500
logbrook_ingest_rejected_total 2
logbrook_ready 1
logbrook_http_requests_total 90
logbrook_http_errors_total 4
logbrook_http_active_requests 1
logbrook_tail_active_subscribers 3
logbrook_tail_gaps_total 0
logbrook_query_requests_total 40
logbrook_query_errors_total 1
logbrook_ingest_available_requests 60
logbrook_decoded_available_bytes 67108864
logbrook_raw_available_bytes 33554432
logbrook_http_duration_seconds_bucket{endpoint="GET /indexes",le="0.001"} 0
logbrook_http_duration_seconds_bucket{endpoint="GET /indexes",le="0.01"} 8
logbrook_http_duration_seconds_bucket{endpoint="GET /indexes",le="0.1"} 10
logbrook_http_duration_seconds_bucket{endpoint="GET /indexes",le="+Inf"} 10
logbrook_http_duration_seconds_sum{endpoint="GET /indexes"} 0.05
logbrook_http_duration_seconds_count{endpoint="GET /indexes"} 10
logbrook_http_duration_seconds_bucket{endpoint="GET /metrics",le="0.001"} 0
logbrook_http_duration_seconds_bucket{endpoint="GET /metrics",le="+Inf"} 0
logbrook_http_duration_seconds_sum{endpoint="GET /metrics"} 0
logbrook_http_duration_seconds_count{endpoint="GET /metrics"} 0
logbrook_http_overload_total{cause="metrics_capacity"} 5
logbrook_storage_size_target_bytes{index="default"} 0
logbrook_storage_size_bytes{index="default"} 4096
logbrook_storage_size_paused{index="default"} 0
logbrook_storage_queue_used{index="default"} 2
logbrook_storage_size_target_bytes{index="payments"} 1000000
logbrook_storage_size_bytes{index="payments"} 250000
logbrook_storage_size_paused{index="payments"} 1
logbrook_storage_archive_files{index="payments"} 7
`;

/**
 * Wraps a summary in a snapshot taken at `takenAt`.
 */
function snapshot(text: string, takenAt: number): MetricsSnapshot {
  return { takenAt, summary: summarizeMetrics(parsePrometheusText(text)) };
}

describe('summarizeMetrics', () => {
  const summary = summarizeMetrics(parsePrometheusText(SCRAPE));

  it('reads counters and gauges', () => {
    expect(summary.counters).toMatchObject({
      ingestedEvents: 1500,
      ingestRejected: 2,
      httpRequests: 90,
      queryRequests: 40,
      tailGaps: 0,
    });
    expect(summary.gauges).toMatchObject({
      ready: true,
      tailSubscribers: 3,
      decodedAvailableBytes: 67108864,
    });
  });

  it('reports a series the server does not export as null, not zero', () => {
    const empty = summarizeMetrics(parsePrometheusText('logbrook_ready 0'));

    expect(empty.gauges.ready).toBe(false);
    expect(empty.counters.ingestedEvents).toBeNull();
    expect(empty.indexes).toEqual([]);
  });

  it('reads overload causes', () => {
    expect(summary.overloads).toEqual([{ cause: 'metrics_capacity', count: 5 }]);
  });

  it('computes mean and the p95 bucket bound per endpoint, busiest first', () => {
    expect(summary.endpoints.map((endpoint) => endpoint.endpoint)).toEqual([
      'GET /indexes',
      'GET /metrics',
    ]);
    expect(summary.endpoints[0]).toMatchObject({ count: 10, meanSeconds: 0.005 });
    expect(summary.endpoints[0]?.p95UpperBoundSeconds).toBe(0.1);
    expect(summary.endpoints[1]).toMatchObject({ meanSeconds: null, p95UpperBoundSeconds: null });
  });

  it('reads storage per index and treats a target of 0 as no limit', () => {
    const [fallback, payments] = summary.indexes;

    expect(fallback).toMatchObject({ index: 'default', sizeBytes: 4096, sizeTargetBytes: null });
    expect(fallback?.sizePaused).toBe(false);
    expect(payments).toMatchObject({
      index: 'payments',
      sizeTargetBytes: 1_000_000,
      sizePaused: true,
      archiveFiles: 7,
      queueUsed: null,
    });
  });
});

describe('counterRate', () => {
  it('divides the change by the elapsed time', () => {
    expect(counterRate(100, 160, 30_000)).toBe(2);
  });

  it('gives no rate when a reading is missing or no time passed', () => {
    expect(counterRate(null, 5, 1000)).toBeNull();
    expect(counterRate(5, null, 1000)).toBeNull();
    expect(counterRate(1, 2, 0)).toBeNull();
  });

  it('gives no rate when the counter fell, because the server restarted', () => {
    expect(counterRate(900, 12, 5000)).toBeNull();
  });
});

describe('ratesBetween and counterWentBack', () => {
  const before = snapshot('logbrook_ingested_events_total 100\nlogbrook_http_requests_total 10', 0);
  const after = snapshot(
    'logbrook_ingested_events_total 150\nlogbrook_http_requests_total 20',
    5000,
  );

  it('computes rates between two snapshots', () => {
    expect(ratesBetween(before, after)).toEqual({
      ingestedEvents: 10,
      httpRequests: 2,
      queryRequests: null,
    });
  });

  it('detects a restart from any falling counter', () => {
    const restarted = snapshot('logbrook_ingested_events_total 3', 10_000);

    expect(counterWentBack(after.summary, restarted.summary)).toBe(true);
    expect(counterWentBack(before.summary, after.summary)).toBe(false);
  });
});
