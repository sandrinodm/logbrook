import type { PrometheusSample } from './prometheus.ts';

import { samplesNamed, valueOf } from './prometheus.ts';

/**
 * The scalar counters of `/metrics`. Counters count since the server process started and restart
 * from zero when it does.
 */
export type MetricCounters = {
  /**
   * `logbrook_ingested_events_total`: events durably committed.
   */
  readonly ingestedEvents: number | null;

  /**
   * `logbrook_ingest_rejected_total`: rejected ingest requests.
   */
  readonly ingestRejected: number | null;

  /**
   * `logbrook_http_requests_total`: all HTTP requests, including scrapes of `/metrics`.
   */
  readonly httpRequests: number | null;

  /**
   * `logbrook_http_errors_total`: HTTP error responses.
   */
  readonly httpErrors: number | null;

  /**
   * `logbrook_query_requests_total`: query requests.
   */
  readonly queryRequests: number | null;

  /**
   * `logbrook_query_errors_total`: query error responses.
   */
  readonly queryErrors: number | null;

  /**
   * `logbrook_tail_gaps_total`: tail gaps caused by expiry or byte overrun.
   */
  readonly tailGaps: number | null;
};

/**
 * The scalar gauges of `/metrics`, which go up and down.
 */
export type MetricGauges = {
  /**
   * `logbrook_ready`: whether every index's storage is ready.
   */
  readonly ready: boolean | null;

  /**
   * `logbrook_http_active_requests`: handlers running now.
   */
  readonly activeRequests: number | null;

  /**
   * `logbrook_tail_active_subscribers`: live tails open now.
   */
  readonly tailSubscribers: number | null;

  /**
   * `logbrook_ingest_available_requests`: free HTTP admission permits.
   */
  readonly availableRequests: number | null;

  /**
   * `logbrook_decoded_available_bytes`: free decoded-memory budget, in bytes.
   */
  readonly decodedAvailableBytes: number | null;

  /**
   * `logbrook_raw_available_bytes`: free wire-body memory budget, in bytes.
   */
  readonly rawAvailableBytes: number | null;
};

/**
 * Latency of one endpoint, accumulated since the server started.
 */
export type EndpointLatency = {
  /**
   * Endpoint label, for example `GET /indexes/{index}/logs`.
   */
  readonly endpoint: string;

  /**
   * Requests observed.
   */
  readonly count: number;

  /**
   * Mean handler time in seconds, or `null` before the first request.
   */
  readonly meanSeconds: number | null;

  /**
   * Upper bound of the histogram bucket that holds the 95th percentile, in seconds. `Infinity`
   * means the slowest bucket; `null` before the first request.
   */
  readonly p95UpperBoundSeconds: number | null;
};

/**
 * Admission rejections for one cause.
 */
export type OverloadCause = {
  /**
   * Why requests were refused, as the server labels it.
   */
  readonly cause: string;

  /**
   * Rejections since the server started.
   */
  readonly count: number;
};

/**
 * Storage diagnostics of one index.
 */
export type IndexStorageMetrics = {
  /**
   * Index name; `default` when the server runs without per-index labels.
   */
  readonly index: string;

  /**
   * `logbrook_storage_size_bytes`: bytes counted against the size target.
   */
  readonly sizeBytes: number | null;

  /**
   * `logbrook_storage_size_target_bytes`, or `null` when the index has no size limit (the server
   * exports 0).
   */
  readonly sizeTargetBytes: number | null;

  /**
   * `logbrook_storage_size_paused`: ingestion is paused at the size target.
   */
  readonly sizePaused: boolean;

  /**
   * `logbrook_storage_queue_used`: writer queue entries in use.
   */
  readonly queueUsed: number | null;

  /**
   * `logbrook_storage_readers_busy`: query workers running.
   */
  readonly readersBusy: number | null;

  /**
   * `logbrook_storage_query_timeouts_total`: queries that hit their deadline.
   */
  readonly queryTimeouts: number | null;

  /**
   * `logbrook_storage_maintenance_failures_total`: failed maintenance passes.
   */
  readonly maintenanceFailures: number | null;

  /**
   * `logbrook_storage_archive_files`: Parquet files in the archive.
   */
  readonly archiveFiles: number | null;
};

/**
 * What the metrics view shows, read out of one scrape of `/metrics`.
 */
export type MetricsSummary = {
  /**
   * Scalar counters.
   */
  readonly counters: MetricCounters;

  /**
   * Scalar gauges.
   */
  readonly gauges: MetricGauges;

  /**
   * Admission rejections by cause; empty when none happened.
   */
  readonly overloads: ReadonlyArray<OverloadCause>;

  /**
   * Latency per endpoint, busiest first.
   */
  readonly endpoints: ReadonlyArray<EndpointLatency>;

  /**
   * Storage diagnostics per index, by name.
   */
  readonly indexes: ReadonlyArray<IndexStorageMetrics>;
};

/**
 * A scrape together with the time the browser received it, which rates are measured against.
 */
export type MetricsSnapshot = {
  /**
   * When the response arrived, in Unix milliseconds.
   */
  readonly takenAt: number;

  /**
   * The parsed scrape.
   */
  readonly summary: MetricsSummary;
};

/**
 * Counter change per second between two snapshots.
 */
export type CounterRates = {
  /**
   * Events committed per second.
   */
  readonly ingestedEvents: number | null;

  /**
   * HTTP requests per second.
   */
  readonly httpRequests: number | null;

  /**
   * Query requests per second.
   */
  readonly queryRequests: number | null;
};

/**
 * Reads the series the view uses out of parsed samples. A series the server does not export comes
 * back as `null`, so the view can say so instead of printing zero.
 */
export function summarizeMetrics(samples: ReadonlyArray<PrometheusSample>): MetricsSummary {
  return {
    counters: readCounters(samples),
    gauges: readGauges(samples),
    overloads: readOverloads(samples),
    endpoints: readEndpoints(samples),
    indexes: readIndexes(samples),
  };
}

/**
 * Rate of change of a counter between two readings, in units per second.
 *
 * Returns `null` when no rate can be stated: a reading is missing, no time passed, or the counter
 * went down, which means the server restarted and the counter began again from zero.
 */
export function counterRate(
  previous: number | null,
  current: number | null,
  elapsedMs: number,
): number | null {
  if (previous === null || current === null || elapsedMs <= 0 || current < previous) {
    return null;
  }

  return ((current - previous) / elapsedMs) * 1000;
}

/**
 * Whether any counter fell between two snapshots, which means the server restarted in between.
 */
export function counterWentBack(previous: MetricsSummary, current: MetricsSummary): boolean {
  const before = Object.values(previous.counters);
  const after = Object.values(current.counters);
  return after.some((value, position) => {
    const old = before[position];
    return value !== null && old !== null && old !== undefined && value < old;
  });
}

/**
 * Computes the per-second rates between two snapshots.
 */
export function ratesBetween(previous: MetricsSnapshot, current: MetricsSnapshot): CounterRates {
  const elapsedMs = current.takenAt - previous.takenAt;
  const before = previous.summary.counters;
  const after = current.summary.counters;
  return {
    ingestedEvents: counterRate(before.ingestedEvents, after.ingestedEvents, elapsedMs),
    httpRequests: counterRate(before.httpRequests, after.httpRequests, elapsedMs),
    queryRequests: counterRate(before.queryRequests, after.queryRequests, elapsedMs),
  };
}

/**
 * Reads the scalar counters.
 */
function readCounters(samples: ReadonlyArray<PrometheusSample>): MetricCounters {
  return {
    ingestedEvents: valueOf(samples, 'logbrook_ingested_events_total'),
    ingestRejected: valueOf(samples, 'logbrook_ingest_rejected_total'),
    httpRequests: valueOf(samples, 'logbrook_http_requests_total'),
    httpErrors: valueOf(samples, 'logbrook_http_errors_total'),
    queryRequests: valueOf(samples, 'logbrook_query_requests_total'),
    queryErrors: valueOf(samples, 'logbrook_query_errors_total'),
    tailGaps: valueOf(samples, 'logbrook_tail_gaps_total'),
  };
}

/**
 * Reads the scalar gauges.
 */
function readGauges(samples: ReadonlyArray<PrometheusSample>): MetricGauges {
  const ready = valueOf(samples, 'logbrook_ready');
  return {
    ready: ready === null ? null : ready === 1,
    activeRequests: valueOf(samples, 'logbrook_http_active_requests'),
    tailSubscribers: valueOf(samples, 'logbrook_tail_active_subscribers'),
    availableRequests: valueOf(samples, 'logbrook_ingest_available_requests'),
    decodedAvailableBytes: valueOf(samples, 'logbrook_decoded_available_bytes'),
    rawAvailableBytes: valueOf(samples, 'logbrook_raw_available_bytes'),
  };
}

/**
 * Reads the admission rejections, one entry per cause.
 */
function readOverloads(samples: ReadonlyArray<PrometheusSample>): ReadonlyArray<OverloadCause> {
  return samplesNamed(samples, 'logbrook_http_overload_total').map((sample) => ({
    cause: sample.labels.cause ?? 'unknown',
    count: sample.value,
  }));
}

/**
 * Reads the latency of every endpoint, busiest first.
 */
function readEndpoints(samples: ReadonlyArray<PrometheusSample>): ReadonlyArray<EndpointLatency> {
  const counts = samplesNamed(samples, 'logbrook_http_duration_seconds_count');
  const sums = samplesNamed(samples, 'logbrook_http_duration_seconds_sum');
  const buckets = samplesNamed(samples, 'logbrook_http_duration_seconds_bucket');
  const endpoints = counts.map((countSample) => {
    const endpoint = countSample.labels.endpoint ?? 'unknown';
    const sum = sums.find((sample) => sample.labels.endpoint === endpoint)?.value ?? null;
    const own = buckets.filter((sample) => sample.labels.endpoint === endpoint);
    return {
      endpoint,
      count: countSample.value,
      meanSeconds: sum === null || countSample.value === 0 ? null : sum / countSample.value,
      p95UpperBoundSeconds: percentileBound(own, countSample.value, 0.95),
    };
  });

  return endpoints.toSorted((a, b) => b.count - a.count);
}

/**
 * Finds the upper bound of the first cumulative bucket that holds `quantile` of `total`
 * observations. Returns `null` when there are none.
 */
function percentileBound(
  buckets: ReadonlyArray<PrometheusSample>,
  total: number,
  quantile: number,
): number | null {
  if (total === 0) {
    return null;
  }

  const ordered = buckets
    .map((sample) => ({ bound: parseBound(sample.labels.le), count: sample.value }))
    .toSorted((a, b) => a.bound - b.bound);
  const hit = ordered.find((bucket) => bucket.count >= total * quantile);
  return hit === undefined ? null : hit.bound;
}

/**
 * Parses a histogram `le` label; `+Inf` and a missing label are infinite.
 */
function parseBound(le: string | undefined): number {
  const bound = Number(le);
  return le === undefined || Number.isNaN(bound) ? Number.POSITIVE_INFINITY : bound;
}

/**
 * Reads the storage diagnostics of every index, found through the `index` label of the size series.
 */
function readIndexes(samples: ReadonlyArray<PrometheusSample>): ReadonlyArray<IndexStorageMetrics> {
  const sizes = samplesNamed(samples, 'logbrook_storage_size_bytes');
  return sizes.map((size) => {
    const index = size.labels.index ?? 'default';
    /**
     * Reads one of this index's storage series.
     */
    const read = (name: string) => indexValue(samples, name, index);
    const target = read('logbrook_storage_size_target_bytes');
    return {
      index,
      sizeBytes: size.value,
      sizeTargetBytes: target === null || target === 0 ? null : target,
      sizePaused: read('logbrook_storage_size_paused') === 1,
      queueUsed: read('logbrook_storage_queue_used'),
      readersBusy: read('logbrook_storage_readers_busy'),
      queryTimeouts: read('logbrook_storage_query_timeouts_total'),
      maintenanceFailures: read('logbrook_storage_maintenance_failures_total'),
      archiveFiles: read('logbrook_storage_archive_files'),
    };
  });
}

/**
 * Returns a storage series value for one index, or `null` when absent.
 */
function indexValue(
  samples: ReadonlyArray<PrometheusSample>,
  name: string,
  index: string,
): number | null {
  const found = samplesNamed(samples, name).find(
    (sample) => (sample.labels.index ?? 'default') === index,
  );
  return found?.value ?? null;
}
