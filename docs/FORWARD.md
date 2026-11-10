# Fluent Forward receiver

Logbrook can receive logs over the
[Forward protocol v1](https://github.com/fluent/fluentd/wiki/Forward-Protocol-Specification-v1)
from Docker's `fluentd` logging driver, Fluentd and Fluent Bit, in the same
`logbrook serve` process as the HTTP API. It is off by default and configured
only in TOML. HTTP tokens are still required; Forward clients authenticate as
separate principals.

## Configure a local Docker route

Merge this into the server configuration, keeping your HTTP credentials:

```toml
[indexes.application]

[forward]
enabled = true

[[forward.listeners]]
name = "docker-local"
bind = "127.0.0.1:24224"
security = "trusted_local"
principal = "docker-host"

[forward.principals.docker-host]
source = "docker-host"
indexes = ["application"]

[[forward.routes]]
listener = "docker-local"
tag_prefix = "application."
index = "application"
normalizer = "docker"
json_profile = "json_auto"
service_from = "tag_suffix"
host = "application-host"
# application_attributes = "top_level"   # see Application attributes below
```

Routes and principals can only name indexes declared under `[indexes.<name>]`,
or `default`. Declared indexes are created at startup and can't be deleted
through the API. The principal sets the stored `source`, and the route sets the
destination index; nothing in a record can change either. Use `tag` instead of
`tag_prefix` to match one exact tag. Routes on the same listener can't overlap.
A tag that matches no route, or a route to an index the principal isn't allowed
to write, closes the connection without an ACK.

To run the receiver in a container, bind `0.0.0.0:24224`, set
`trusted_network = true`, and publish `127.0.0.1:24524:24224`. Any other
container on the receiver's network can then connect, so use an isolated network
or an authenticated listener for sources you don't trust. The
[Forward Compose example](../examples/forward/compose.yaml) does this and keeps
the image's non-root user, a read-only root filesystem, all capabilities dropped
and a 35-second stop grace period.

Point the application container's logging driver at the address the Docker
daemon sees:

```yaml
logging:
  driver: fluentd
  options:
    fluentd-address: "127.0.0.1:24524"
    fluentd-async: "true"
    fluentd-sub-second-precision: "true"
    fluentd-request-ack: "true"
    fluentd-buffer-limit: "8192"
    fluentd-write-timeout: "5s"
    tag: "application.api"
    cache-max-size: "10m"
    cache-max-file: "3"
```

Containers pick up logging options only when recreated. Check the options
against your Docker Engine version and send a test event before moving real
traffic. `fluentd-write-timeout` doesn't limit the wait for an ACK. The
`cache-*` options keep a local copy for `docker logs` only.

Keep `fluentd-async` on when you set `fluentd-request-ack`. Without it, Docker
Engine 28.0.4 (whose driver uses fluent-logger-golang 1.9.0) sends stdout and
stderr lines from separate goroutines over one connection and can mix up their
ACKs. The driver then resends both lines in a loop without backoff, which
stores duplicates and overloads the receiver. In async mode the driver sends one
envelope at a time.

## Delivery and compatibility

Supported modes are Message, Forward, PackedForward (as `bin` or `str`) and
gzip CompressedPackedForward, including payloads made of several gzip members.
Timestamps can be integer seconds or EventTime (extension type 0, eight bytes,
nanoseconds below one billion). Events are stored with millisecond precision;
the original seconds and nanoseconds are kept in `attributes.forward`.

Fluent Bit's logs metadata format is supported when the envelope sets
`fluent_signal = 0`. Entries then have the shape
`[[timestamp, metadata_map], record]`. The metadata is stored in
`attributes.forward.metadata`, with the original bytes in
`original_metadata_msgpack_b64` and its encoding in `metadata_encoding`.
Metadata entries without `fluent_signal = 0`, malformed metadata and other
signal values (metrics, traces) are rejected.

Each envelope is validated as a whole and committed to one index in one
transaction; it is never split. If the envelope carries a `chunk` option,
Logbrook sends the ACK only after the commit. An empty envelope is a valid no-op
and is ACKed. A corrupt last entry or a bad gzip checksum rejects every entry in
the envelope. Rejected, overloaded or failed envelopes close the connection
without an ACK. There is no negative ACK with a reason, so a sender can retry a
permanently invalid record forever.

A sender that retries after losing the connection or the ACK can store
duplicates; there is no deduplication. A commit that has started finishes even
if the client disconnects.

Connections stay open between envelopes. Set `keepalive = false` on a listener
to close each connection after its first envelope. Nil values are treated as
keepalives. Other values that aren't arrays are ignored, and the eighth one on a
session closes it. Malformed or oversized values close the session immediately.
Unknown options, including `size`, are ignored. Only `compressed = "gzip"`
turns on decompression. A packed payload must end on an entry boundary.

UDP heartbeats are off by default. If the sender uses them, set
`udp_heartbeat = true` on the listener; a non-loopback listener also needs
`allow_peers` with the senders' addresses. Fluentd 1.20.1 defaults to
`heartbeat_type transport`. For ACK-based TCP or TLS delivery,
`heartbeat_type none` skips the separate probes and relies on the ACK timeout.

`chunk` tokens must be strings unless `allow_binary_chunk = true`; they are
echoed back byte for byte. With the default `zero_time_to_receipt = true`, an
event with a zero timestamp gets the receipt time and
`attributes.forward.zero_time_substituted = true`. Set it to `false` to keep
the zero timestamp, which falls outside the retention window and is rejected.

Not supported: JSON transport, zstd, metrics and traces, reassembly of
fragmented records, quarantine of rejected records, deduplication, replication
and clustering.

### Writer queue share

Forward shares each index's writer queue with HTTP and can use at most half
of it: half of `storage.queue_bytes / max_indexes` and half of
`storage.queue_capacity`, which is 4 MiB and 8 requests with the defaults.
Enabling Forward requires `storage.queue_capacity` of at least 2. The queued
size includes serialized attributes and per-event overhead, so it's larger than
the wire size, and the default 8 MiB `max_normalized_bytes` is larger than the
share. An envelope over its share is always rejected: reduce the sender's batch
size or raise `storage.queue_bytes`. An envelope that finds the share full is
overload and can be retried.

## Normalization

Generic routes (the default `normalizer`) store the record under
`attributes.application`, unless the route sets
[`application_attributes`](#application-attributes). The message is the first
string field listed in `message_fields` (default `["message", "msg"]`), or the
record as JSON when none matches. Generic routes don't detect Docker records
from a `log` field.

Docker routes (`normalizer = "docker"`) need `container_id`, `container_name`,
`source` and `log`, each present once as a string. Otherwise the event is stored
as a generic record with a fallback marker in `attributes.logbrook`. The driver's
`source` (stdout or stderr) goes to `attributes.docker.stream`, not the stored
source. `container_name` keeps its leading slash. Other driver fields go to
`attributes.docker.extra`.

Docker routes parse the `log` line according to `json_profile`:

| `json_profile` | Message and level |
| --- | --- |
| `text` (default) | The line as is, at `default_level` (default 30) |
| `pino` | String `msg`; nonnegative integer `level` |
| `json_auto` | First string in `message_fields`; `level` or `severity` name |

`json_auto` maps level names, ignoring case: trace 10, debug 20, info 30,
warn or warning 40, error 50, fatal or critical 60. Anything else gets
`default_level`. Both JSON profiles also read `logger` or `name`, a nonnegative
integer `pid`, and `service`. A line that isn't a JSON object with a message
keeps the original line, including whitespace and stack traces. When parsing
changes the message, the original line is also stored in
`attributes.docker.log`.

The stored service is the first that is set of: the route's `service` or the
tag suffix (`service_from = "tag_suffix"`, not allowed on exact `tag` routes),
the field named by `compose_service_label` (at the top level or inside a
`labels` map), the application's `service` field, and the container name
without its leading slash. The host comes only from the route's `host`, never
from the peer address.

Events use the Forward timestamp by default. To use a timestamp from the record,
set both `application_time_field = "time"` and
`application_time_format = "unix_ms"` (or `"unix_seconds"`). The value must be
an integer within the index's retention window and future-skew limit. Otherwise
the event keeps the Forward timestamp and
`attributes.logbrook.application_time_status` records why.

Docker splits long lines into partial messages. A record with any `partial_*`
field is stored as its own event without JSON parsing, with `partial_id`,
`partial_ordinal`, `partial_last`, the original fields and a validity flag in
`attributes.docker.fragment`.

### Application attributes

By default the application's fields stay together under
`attributes.application`. Attribute filters match only top-level attributes, so
a field such as `runId` can't be filtered there. To store the fields at the top
level of `attributes` instead, set the option on the route:

```toml
application_attributes = "top_level"   # default: "nested"
```

With `top_level`, a field like `runId` is stored at `attributes.runId`, and
`attributes.application` is removed. The message, level, logger, pid, service
and application time are derived as before. On a Docker route, the fields are
the top-level keys of the JSON log line, so the line must parse as a JSON object
(`parse_status` is `structured` or `json_without_message`).

Logbrook writes `forward`, `docker` and `logbrook` at the top level itself. An
application field with one of those names is stored in
`attributes.logbrook.conflicting_fields` instead. A field named `application`
is an ordinary field and is stored at `attributes.application`.

Each event that is moved gets `attributes.forward.application_attributes` set
to `top_level`. These events are left as the default layout produces them, with
no marker:

- Records that Logbrook preserves with `msgpack_typed_v1`, because their
  `application` value holds tagged arrays. See
  [MessagePack preservation](#messagepack-preservation).
- Docker events where the log line isn't parsed as a JSON object: text lines,
  fragments and lines over the depth limit. Their `attributes.application`
  holds the raw driver record, which `attributes.docker` already covers.
- Records on Docker routes that aren't Docker records, because a driver field
  (`log`, `container_id`, `container_name`, `source`) is missing, repeated or
  not a string. They're stored as generic records with
  `attributes.logbrook.docker_status = "generic_fallback"`.

Only the top-level keys of the record or JSON line move. Nested objects stay as
they are. The option applies to events received after the change. Events stored
earlier are not rewritten. The new keys count toward the same size limits as
any other attribute.

Fields that move become ordinary attributes. Fields such as `msg`, `level`,
`time` and `service` are separate from the event's own message, level,
timestamp and service. A filter on the attribute `level` matches that field's
value only. It is not a severity filter; use `min_level` for that.

On Docker routes with `json_profile = "text"` (the default), the log line isn't
parsed, so no fields move. Generic routes move the record's fields whatever
`json_profile` says.

Numbers keep the value they have after projection. A MessagePack `float32` is
stored widened to `float64`, so `0.1` sent as `float32` is stored as
`0.10000000149011612`, and an equality filter on `0.1` doesn't match it.

Promoting a top-level key with `promoted_string_keys` makes string equality
filters on it faster. Other filters don't benefit. An index that has
never had promoted keys can still get them. Once an index has been opened with a
non-empty key set, its keys can't change. Rows stored before that keep using the
slower JSON filter until export or compaction backfills them. See
[promoted string attributes](OPERATIONS.md#promoted-string-attributes).

### MessagePack preservation

Records must be maps with string keys at the top level; anything else rejects
the envelope. Records with unique UTF-8 keys and JSON-compatible values are
stored as plain JSON. Records with duplicate keys, invalid UTF-8, binary values,
unknown extensions, non-finite floats or non-string nested keys use the
`msgpack_typed_v1` encoding:

- `attributes.application` holds the record as tagged arrays, with map entries
  as ordered pairs.
- `attributes.forward.record_encoding` is `msgpack_typed_v1`.
- `attributes.forward.original_record_msgpack_b64` holds the exact original
  bytes.

The tags are `map`, `array`, `string`, `string_bytes`, `binary`, `integer`,
`float32`, `float64`, `extension`, `boolean` and `nil`. Every value and map key
is tagged, so a record's own arrays can't be mistaken for tags. Integers are
decimal strings, and floats are their IEEE 754 bits in hex. See the
[golden fixture](../tests/fixtures/forward/typed-v1.json). If a Docker route
parses JSON from such a record, the parsed object goes to
`attributes.docker.parsed_application`.

## Authentication and listener settings

Startup checks every listener and route before accepting events. If a listener
can't bind or its TLS material can't be loaded, startup fails and the HTTP API
doesn't start.

| Listener setting | Meaning |
| --- | --- |
| `name` | Unique listener name that routes refer to |
| `bind` | TCP address; use this or `unix_path` |
| `security` | `trusted_local` (default), `shared_key` or `mtls` |
| `principal` | Principal for every client of a `trusted_local` or `shared_key` listener |
| `trusted_network` | Allow `trusted_local` on a non-loopback address; default false |
| `allow_peers` | Exact peer IP allowlist; empty (default) allows all |
| `keepalive` | Keep connections open between envelopes; default true |
| `udp_heartbeat` | Answer UDP heartbeats on the same port; default false |
| `unix_path`, `unix_mode` | Absolute socket path and mode, e.g. `384` (0600) or `432` (0660) |
| `tls` | Certificate and key paths, plus client CA and fingerprints for mTLS |

A Unix socket's parent directory must be owned by the service user or root and
not be world-writable. `unix_mode` must include owner read and write and can't
grant access to others. Logbrook won't bind over an existing path, even a stale
socket; check its owner and remove it yourself. Unix sockets can't use TLS or
UDP heartbeats. The UDP heartbeat answers only a single zero byte, at most 10
times per second per listener.

A shared-key listener:

```toml
[[forward.listeners]]
name = "secure"
bind = "127.0.0.1:24225"
security = "shared_key"
server_hostname = "logbrook.example"
shared_key = { env = "FORWARD_SHARED_KEY" }
principal = "collector"
```

To require a username and password as well, add
`users = { sender = { principal = "collector", password = { file = "/run/secrets/forward-password" } } }`.
Each user authenticates as its own principal, and the listener `principal` is
then optional. A secret reference takes exactly one of `file` or `env`; a file
is read byte for byte, including a trailing newline. The shared key doesn't
encrypt traffic, so add TLS across untrusted networks.

TLS takes
`tls = { certificate = "/run/secrets/server.pem", private_key = "/run/secrets/server-key.pem" }`.
For `security = "mtls"`, also set `client_ca` and `client_fingerprints`, a map
from lowercase SHA-256 fingerprints of client leaf certificates (64 hex
characters) to principal names. The mapped principal's `indexes` still limit
where it can write. The runtime user must be able to read the TLS files.

## Limits, memory, and operation

Forward settings apply on restart and don't change the HTTP limits. Startup and
`logbrook --config <file> check-config` reject unknown keys, overlapping routes,
unknown principals, undeclared indexes and limits that contradict each other.

| `[forward]` setting | Default |
| --- | --- |
| `enabled` | false |
| `max_connections` | 1,024 |
| `max_frame_bytes` | 8 MiB |
| `max_expanded_bytes` | 32 MiB |
| `max_events_per_frame` | 10,000 |
| `max_event_bytes`, `max_message_bytes` | 256 KiB each |
| `max_normalized_bytes` | 8 MiB per envelope |
| `max_metadata_depth` | 32 |
| `max_tag_bytes`, `max_chunk_bytes` | 1 KiB each |
| `raw_budget_bytes`, `decoded_budget_bytes` | 128 MiB each |
| `max_cpu_jobs` | 4 |
| `max_expansion_ratio`, `max_gzip_members` | 1,000 / 128 |
| `decode_timeout_ms`, `handshake_timeout_ms` | 5,000 each |
| `frame_idle_timeout_ms`, `frame_total_timeout_ms` | 5,000 / 30,000 |
| `session_idle_timeout_ms` | 300,000 |
| `admission_timeout_ms`, `ack_write_timeout_ms` | 5,000 each |
| `allow_binary_chunk`, `zero_time_to_receipt` | false / true |

Two budgets must cover the largest frame:

- `raw_budget_bytes` must be at least `3 * max_frame_bytes + 1024`.
- `decoded_budget_bytes` must be at least
  `2 * max_expanded_bytes + max_normalized_bytes`.

Each session decodes one envelope at a time. The gzip ratio limit can reject
valid, very repetitive logs; if it does, raise it and rely on
`max_expanded_bytes`. `max_normalized_bytes` counts the stored form, including
JSON escaping, Base64 and typed encoding, so a small MessagePack record can
exceed it. These budgets cover Forward's own buffers only: when sizing a host,
add DuckDB memory, TLS state and kernel socket buffers.

The HTTP API, including `/ready`, starts only after every Forward listener is
bound, and the process shuts down if a listener fails later. Overload doesn't
affect readiness. On shutdown, Logbrook stops accepting envelopes, drops
incomplete frames without an ACK, and finishes admitted commits and their ACKs
within `shutdown_timeout_secs`. The sender may retry anything not ACKed by
then.

`/metrics` exports `logbrook_forward_*` counters and gauges, and a
`logbrook_forward_duration_seconds` histogram with a `stage` label
(`preparation`, `decode`, `admission`, `commit`, `ack`). Closed sessions are
logged with the listener name, a reason code, the error message (such as
which budget was full) and a diagnostic ID: the first eight, then each power of
two. Logs never include records, chunk tokens or
credentials. See [operations](OPERATIONS.md) for retention, storage and
backups.
