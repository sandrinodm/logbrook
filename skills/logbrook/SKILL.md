---
name: logbrook
description: Install Logbrook with Docker or Docker Compose, connect applications to its structured log ingestion API, and investigate logs through its CLI and HTTP search, counts, facets, histograms, and live tail. Use when deploying Logbrook, configuring log shipping, or answering questions about logs stored in Logbrook.
---

# Logbrook

Help the user deploy Logbrook, send application logs, or investigate an existing index. Follow the relevant path; a query against an existing server does not require a new deployment.

## Choose the workflow

| Task | Read |
| --- | --- |
| Install with Docker or Compose, configure storage and retention, or connect containers | [Installation](references/installation.md), with the ready-to-use [Compose template](assets/compose.yaml) |
| Configure an application, integrate Pino, or batch larger volumes | [Application logging](references/application-logging.md) |
| Search events, investigate errors, compare time windows, or compute insights | [Queries and insights](references/queries-and-insights.md) |

These references travel with the skill. Resolve their paths from this directory, even when the skill is installed outside a Logbrook checkout. Commands use Bash or Zsh, Docker with the Compose plugin, and `curl`/`jq`; Pino examples also need Node.js and npm.

## Establish the target

Reuse the user's deployment and configuration when available. Determine the server URL, index, relevant credential role, and either the deployment directory, application directory, or investigation time window. Ask only for missing details that affect the work. Keep credentials in the environment or a local secret file rather than the conversation.

Use `ghcr.io/sandrinodm/logbrook` for published images. Select a completed release's version tag or digest. If a requested release is still running, report that publication is pending; a failed pull alone does not prove the package is private or the installation is misconfigured.

## Product contract

- Multiple applications can write to one index using distinct `service` fields. Each index has its own DuckDB database, Parquet archives, and retention policy under `/data/indexes/<name>/`. One server process owns a data directory.
- Ingestion tokens write; read tokens query; optional admin tokens manage indexes. Admin tokens do not grant event access. Ingestion credentials assign `source`, while applications provide `service`.
- Recent DuckDB events and archived Parquet events are searched together. Archiving preserves searchability; retention removes history.
- The API uses structured filters on one index per request. Arbitrary SQL, Lucene/KQL, regex, and arbitrary attribute filters are not exposed. Use complete bounded exports for client-side analysis.
- A successful ingestion response confirms the batch committed. A lost response followed by a retry can duplicate events; the server has no idempotency keys.

For a different server version, inspect its `/openapi.json` and consult the matching repository release before using an unfamiliar option. The current [API reference](https://github.com/sandrinodm/logbrook/blob/main/docs/API.md), [configuration example](https://github.com/sandrinodm/logbrook/blob/main/logbrook.example.toml), and [query cookbook](https://github.com/sandrinodm/logbrook/blob/main/docs/QUERY-EXAMPLES.md) cover the complete contract.

## Completion evidence

- **Installation:** the server is ready, storage persists across a container restart, and a synthetic event can be ingested and queried through the intended connection path.
- **Application integration:** a uniquely identifiable record from the actual application reaches the chosen index with its service and context fields; queued logs drain on normal shutdown.
- **Investigation:** findings identify the index, time bounds, filters, matching counts, and supporting events. Distinguish observed patterns from hypotheses, and state whether pagination, retention, or delivery loss limits completeness.

Creating a demonstration event is a write. For an investigation-only request, use existing events. Keep routine setup and investigation separate from permanent index deletion, volume deletion, or shortening retention.
