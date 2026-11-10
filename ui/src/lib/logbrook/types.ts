/**
 * A scalar an attribute filter can compare against. The server accepts strings, numbers and
 * booleans; null, objects and arrays are rejected.
 */
export type AttributeValue = string | number | boolean;

/**
 * One top-level attribute equality condition of the `filter` query parameter. `eq` is the only
 * operator the server supports.
 */
export type AttributeCondition = {
  /**
   * Attribute key directly under the event's `attributes`; nested paths are not supported.
   */
  readonly key: string;

  /**
   * Comparison operator.
   */
  readonly op: 'eq';

  /**
   * Value the attribute must equal. Types must agree: `123` does not match `"123"`.
   */
  readonly value: AttributeValue;
};

/**
 * One stored log event as the search and tail endpoints return it.
 *
 * `event_time` and `received_at` are Unix milliseconds. Optional canonical fields come back as
 * `null` when the producer did not send them. `attributes` holds every other key of the original
 * record, so its values are `unknown` until a view narrows them.
 */
export type LogEvent = {
  /**
   * Server-assigned id, a string. Unique within an index; use it as a list key.
   */
  readonly id: string;

  /**
   * Source stamped by the ingestion token.
   */
  readonly source: string;

  /**
   * When the event happened, in Unix milliseconds.
   */
  readonly event_time: number;

  /**
   * When the server received it, in Unix milliseconds.
   */
  readonly received_at: number;

  /**
   * Severity number; 50 and above are errors.
   */
  readonly level: number;

  /**
   * The log message.
   */
  readonly message: string;

  /**
   * Emitting service, or `null`.
   */
  readonly service: string | null;

  /**
   * Logger name, or `null`.
   */
  readonly logger: string | null;

  /**
   * Emitting host, or `null`.
   */
  readonly host: string | null;

  /**
   * Process id, or `null`.
   */
  readonly pid: number | null;

  /**
   * All other keys of the original record, kept as sent.
   */
  readonly attributes: Readonly<Record<string, unknown>>;
};

/**
 * One page of `GET /indexes/{index}/logs`: events newest first, and the cursor of the next older
 * page, or `null` on the last page.
 */
export type SearchResult = {
  /**
   * The page's events, newest first.
   */
  readonly events: ReadonlyArray<LogEvent>;

  /**
   * Opaque cursor for the next page; only valid with the same range, filters and `limit`.
   */
  readonly next_cursor: string | null;
};

/**
 * One value of a facet and how many matching events carry it. Level values are strings such as
 * `"50"`.
 */
export type FacetValue = {
  /**
   * The facet value.
   */
  readonly value: string;

  /**
   * Number of matching events with this value.
   */
  readonly count: number;
};

/**
 * The five facets of `GET /indexes/{index}/logs/facets`, each at most 100 values by descending
 * count. Events without an optional field are left out of that field's list.
 */
export type Facets = {
  /**
   * Values of `service`.
   */
  readonly services: ReadonlyArray<FacetValue>;

  /**
   * Values of `logger`.
   */
  readonly loggers: ReadonlyArray<FacetValue>;

  /**
   * Values of `host`.
   */
  readonly hosts: ReadonlyArray<FacetValue>;

  /**
   * Values of `level`, as strings.
   */
  readonly levels: ReadonlyArray<FacetValue>;

  /**
   * Values of `source`.
   */
  readonly sources: ReadonlyArray<FacetValue>;
};

/**
 * One non-empty histogram bucket. Empty buckets are not returned.
 */
export type HistogramBucket = {
  /**
   * Bucket start in Unix milliseconds, counted from the requested `from`.
   */
  readonly time: number;

  /**
   * Number of matching events in the bucket.
   */
  readonly count: number;
};

/**
 * The body of `GET /indexes/{index}/logs/histogram`.
 */
export type Histogram = {
  /**
   * Non-empty buckets in ascending time order; fill the gaps with zeros when charting.
   */
  readonly buckets: ReadonlyArray<HistogramBucket>;
};

/**
 * One entry of `GET /indexes`. A source-restricted read token receives only `name`; every other
 * field is then absent.
 */
export type IndexInfo = {
  /**
   * Index name.
   */
  readonly name: string;

  /**
   * Total size of the index's files in bytes, measured when the request was made.
   */
  readonly size_bytes?: number;

  /**
   * Size of the DuckDB database file in bytes.
   */
  readonly database_bytes?: number;

  /**
   * Size of the write-ahead log in bytes.
   */
  readonly wal_bytes?: number;

  /**
   * Size of the Parquet archive in bytes.
   */
  readonly archive_bytes?: number;

  /**
   * Size of temporary files in bytes.
   */
  readonly temp_bytes?: number;

  /**
   * Size of all other files in bytes.
   */
  readonly other_bytes?: number;

  /**
   * Size target in bytes, or `null` when the index has no size limit.
   */
  readonly max_size_bytes?: number | null;

  /**
   * Retention in milliseconds.
   */
  readonly retention_ms?: number;

  /**
   * Whether the index storage is available.
   */
  readonly ready?: boolean;
};

/**
 * The body of `GET /indexes`.
 */
export type IndexList = {
  /**
   * The indexes the credential can reach.
   */
  readonly indexes: ReadonlyArray<IndexInfo>;
};

/**
 * The JSON body of every error response.
 */
export type ErrorBody = {
  /**
   * Explanation for people; do not parse it as an error code.
   */
  readonly error: string;
};

/**
 * Filters shared by search, facets, histogram and tail.
 *
 * `source` is the only OR list; every other filter is one exact value, and `minLevel` matches
 * `level >= minLevel`. A cursor is only valid for the exact filters, time range and `limit` it came
 * from.
 */
export type LogFilters = {
  /**
   * Sources to include, joined with OR.
   */
  readonly source?: ReadonlyArray<string>;

  /**
   * Exact, case-sensitive service name.
   */
  readonly service?: string;

  /**
   * Exact, case-sensitive logger name.
   */
  readonly logger?: string;

  /**
   * Exact, case-sensitive host name.
   */
  readonly host?: string;

  /**
   * Literal, case-sensitive substring of the message.
   */
  readonly message?: string;

  /**
   * Lowest severity to include.
   */
  readonly minLevel?: number;

  /**
   * Attribute equality conditions, at most 16, all of which must match.
   */
  readonly attributes?: ReadonlyArray<AttributeCondition>;
};

/**
 * A closed-open time range in Unix milliseconds.
 */
export type TimeRange = {
  /**
   * Inclusive start.
   */
  readonly from: number;

  /**
   * Exclusive end.
   */
  readonly to: number;
};

/**
 * Parameters of a search: a frozen time range, the filters, and an optional page size.
 */
export type SearchParams = TimeRange &
  LogFilters & {
    /**
     * Page size; the server caps it at 1,000 by default.
     */
    readonly limit?: number;
  };

/**
 * Parameters of a histogram request.
 */
export type HistogramParams = TimeRange &
  LogFilters & {
    /**
     * Bucket duration in milliseconds. Without it the server picks 60 seconds, or the smallest
     * interval that stays within 2,000 buckets.
     */
    readonly intervalMs?: number;
  };
