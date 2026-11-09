---
name: logbrook
description: Install Logbrook with Docker or Docker Compose, connect applications to its structured log ingestion API, and investigate logs through its CLI and HTTP search, counts, facets, histograms, and live tail. Use when deploying Logbrook, configuring log shipping, or answering questions about logs stored in Logbrook.
---

# Logbrook

Help the user deploy Logbrook, send application logs to it, or investigate an existing index. Follow only the path the task needs: querying an existing server doesn't need a new deployment.

## Choose the workflow

| Task | Read |
| --- | --- |
| Install with Docker or Compose, configure storage and retention, or connect containers | [Installation](references/installation.md), with the [Compose template](assets/compose.yaml) |
| Connect Docker's fluentd driver or a collector over Forward | [Forward receiver](references/forward.md) |
| Configure an application, integrate Pino, or batch larger volumes | [Application logging](references/application-logging.md) |
| Search events, investigate errors, compare time windows, or compute insights | [Queries and insights](references/queries-and-insights.md) |

The references ship with the skill. Resolve their paths from this directory, even when the skill is installed outside a Logbrook checkout. Commands assume Bash or Zsh, Docker with the Compose plugin, `curl` and `jq`; the Pino examples also need Node.js and npm.

## Establish the target

Reuse the user's existing deployment and configuration when there is one. Find out the server URL, the index, which credential role the task needs, and the deployment directory, application directory or investigation time window. Ask only for missing details that change the work. Keep credentials in the environment or a local secret file, never in the conversation.

Published images are at `ghcr.io/sandrinodm/logbrook`. Pick a completed release's version tag or digest. If the requested release is still running, tell the user the image isn't published yet. A failed pull on its own doesn't mean the package is private or the installation is wrong.

## Product contract

- Several applications can write to one index and tell themselves apart with `service`. Each index has its own DuckDB database, Parquet archives and retention policy under `/data/indexes/<name>/`. One server process owns a data directory.
- Ingestion tokens write, read tokens query, and optional admin tokens manage indexes. Admin tokens can't read events. The ingestion token sets `source`; the application sets `service`.
- Searches cover recent events in DuckDB and archived events in Parquet together. Archiving keeps events searchable; retention deletes them.
- Every request targets one index and uses structured filters. Top-level attribute equality is available through URL-encoded JSON `filter` (CLI `--filter`) on search, count, facets, histogram and tail. Conditions use `eq` and AND with the other filters; strings, numbers and booleans don't match each other. Nested paths, ranges, SQL, Lucene or KQL, regex and grouping by arbitrary attributes aren't available; use a complete paged export for those. Cursors are bound to the normalized selection, so restart the search after changing filters or when the server rejects a cursor.
- A successful ingestion response means the batch committed. If a response is lost and the client retries, events can be stored twice; there are no idempotency keys.

For a server of another version, read its `/openapi.json` and the matching repository release before using an option you haven't seen. The full contract is in the [API reference](https://github.com/sandrinodm/logbrook/blob/main/docs/API.md), the [configuration example](https://github.com/sandrinodm/logbrook/blob/main/logbrook.example.toml) and the [query cookbook](https://github.com/sandrinodm/logbrook/blob/main/docs/QUERY-EXAMPLES.md).

## Completion evidence

| Task | Done when |
| --- | --- |
| Installation | The server is ready, storage survives a container restart, and a synthetic event can be ingested and queried through the intended connection path. |
| Application integration | A uniquely identifiable record from the real application reaches the chosen index with its service and context fields, and queued logs drain on normal shutdown. |
| Investigation | Findings name the index, time bounds, filters, matching counts and supporting events. Separate observed patterns from hypotheses, and say whether pagination, retention or delivery loss limits completeness. |

Creating a demonstration event is a write. For an investigation-only request, use existing events. Don't combine routine setup or investigation with permanently deleting an index or volume, or with shortening retention.
