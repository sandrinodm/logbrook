# Logbrook concepts

Logbrook stores structured application logs in named indexes. You search them through the CLI and HTTP API.

## Events and time

An **event** is one log record from an application. It has an event ID, a timestamp, a severity level, a message, optional service, logger, and host fields, and structured attributes.

**Event time** is the timestamp the producer sends. Searches, histograms, archival, and age-based retention all use it. **Received time** is when Logbrook received the event, which is later for delayed logs.

An **accepted batch** is a batch that Logbrook has committed to storage and answered with a success response. If the response is lost and the producer retries, the events are stored twice.

## Indexes, services, and sources

An **index** is a named collection of events with its own DuckDB database, Parquet archives, and retention settings. Each request names its index in the URL. The `default` index always exists. Event IDs are unique within an index.

A **service** is the application or component that produced an event, such as an API or background worker. It comes from the event's `service` field.

A **source** is the name the server configuration assigns to an ingestion credential. The event payload can't override it. One source can contain logs from several services. Read credentials can be limited to some sources and some indexes, and a query must satisfy both limits.

## Search and live tail

**Search** returns retained events in a time range that match field filters, newest first. A cursor fetches the next page with the same range and filters.

A **facet** groups matching events by a built-in field, such as service or host, and returns counts for its values. A **histogram** groups matching events into time buckets.

**Live tail** streams newly ingested events. A client that reconnects with its last SSE event ID first receives the retained events it missed, then new ones. Tail follows ingestion order, so a delayed event can appear after events with a later event time. If the server reports a gap, use search to fetch what the stream missed.

## Archives, retention, and backups

An **archive** is a Parquet file of older events. Archived events stay searchable alongside recent ones.

**Retention** sets how long each index keeps events, plus an optional disk-size target. When an index reaches its size target, Logbrook expires its oldest events before they reach the age limit. The size target is not a hard filesystem quota. Extending retention does not bring back expired events.

A **backup** is a copy of the whole data directory, taken while the server is stopped. It includes the DuckDB files, any WAL, archive metadata, and Parquet files. An archive file on its own is not a backup.

See the [query cookbook](QUERY-EXAMPLES.md) for examples and the [operations guide](OPERATIONS.md) for storage, retention, and backup procedures.
