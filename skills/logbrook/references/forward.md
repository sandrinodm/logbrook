# Fluent Forward ingestion

Forward is optional and off by default. Check that the running version has it,
then follow the
[Forward configuration reference](https://github.com/sandrinodm/logbrook/blob/main/docs/FORWARD.md)
for that version, including its supported modes and limits. The repository's
[container example](https://github.com/sandrinodm/logbrook/tree/main/examples/forward)
uses Docker's built-in fluentd driver and keeps the usual container hardening.

Configure a listener, a principal with a fixed source and an explicit list of
allowed indexes, and static routes that don't overlap, each to an index declared
in the configuration. Set `normalizer = "docker"` for Docker output; generic
routes don't detect Docker records from their field names. HTTP credentials are
separate and still needed for the server and for queries.

A trusted listener on loopback suits a local daemon. A listener inside a
container needs `trusted_network = true` even when the host port is published
only on loopback, and every other container on that network can then connect.
Across trust boundaries, use shared-key authentication with TLS, or mTLS with
client certificate fingerprints mapped to principals. Keep secrets out of the
conversation and out of Git.

Request ACKs only from a driver version that supports them. An ACK means the
whole envelope committed; a missing ACK doesn't mean it failed, and a retry after
a lost response can store duplicates. Docker's partial-line fragments are stored
as separate events and never reassembled. When replacing an existing collector,
keep its configuration and anything that reads its output until a separate
migration has verified counts and a rollback path.

Completion evidence: a uniquely identifiable record from the real application,
queried from the intended index with the configured source and service. Also
verify that it survives a receiver restart, that ACKs arrive, and which Engine or
collector version and options are in use. Don't present configured connection
limits as measured capacity, and don't claim clustering or exactly-once delivery.
