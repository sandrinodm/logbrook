# Logbrook concepts

Logbrook stores structured application logs in named indexes and lets operators search retained events through the CLI and HTTP API.

## Events and time

An **event** is one recorded occurrence from an application. It includes an event ID, timestamp, severity, message, optional service/logger/host fields, and structured attributes.

**Event time** is the timestamp supplied by the producer. Searches, histograms, archival, and age retention use this timestamp. **Received time** records when Logbrook received the event; it may be later for delayed logs.

An **accepted batch** is a group of events whose transaction has committed and for which Logbrook returned an ingestion success response. Admission to an in-memory queue alone does not count as acceptance. If a committed response is lost and a producer retries, duplicate events are possible.

## Indexes, services, and sources

An **index** is a named collection of events with its own DuckDB database, Parquet archives, and retention policies. Each request selects an index through its URL. The `default` index always exists, and event IDs are unique within an index.

A **service** is the application or component that produced an event, such as an API or background worker. It comes from the event's `service` field.

A **source** is the origin assigned to an ingestion credential by server configuration. It cannot be overridden by the event payload. One source can contain logs from several services. Read credentials can restrict both sources and indexes; both restrictions apply to each query.

## Search and live tail

**Search** selects retained events in a time range with structured field filters. It returns events newest first and uses cursors to fetch additional pages with the same bounds and filters.

A **facet** groups matching events by a built-in field, such as service or host, and returns counts for its values. A **histogram** groups matching events into time buckets.

**Live tail** follows newly ingested events, with a retained replay when resuming from a saved SSE event ID. It follows ingestion order, so delayed events can appear even when their event time is older. A gap means the client should reconcile retained history with search.

## Archives, retention, and backups

An **archive** is a Parquet file containing older events that remain part of an index's searchable data. Archival moves events out of the recent-event table without removing them from searches.

**Retention** determines how long events remain available and sets an optional disk-size target for each index. Size pressure can expire events before their age limit. The size target is not a hard filesystem quota, and extending retention cannot recover expired events.

A **backup** is a consistent copy of the complete data directory made while the server is stopped. It includes DuckDB files, any WAL, archive metadata, and Parquet files. An archive alone is not a backup.

See the [query cookbook](QUERY-EXAMPLES.md) for examples and the [operations guide](OPERATIONS.md) for storage, retention, and backup procedures.
