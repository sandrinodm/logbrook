#!/usr/bin/env python3
"""Seed fuzzers from checked-in independent Ruby sender captures and hand-built lift envelopes."""
from pathlib import Path

root = Path(__file__).resolve().parent.parent
fixtures = root / "tests" / "fixtures" / "forward"
corpus = root / "fuzz" / "corpus"
for name in ("message_eventtime", "forward", "packed_bin", "packed_str", "gzip_multimember"):
    wire = (fixtures / f"{name}.msgpack").read_bytes()
    for target in ("framing", "envelope", "docker_normalize"):
        (corpus / target).mkdir(parents=True, exist_ok=True)
        (corpus / target / f"{name}.msgpack").write_bytes(wire)

# Fixtures use a fixstr tag followed by bin8; validate before slicing so an
# encoder change cannot silently turn these seeds into a different surface.
for fixture, output in (("packed_bin", "entries"), ("gzip_multimember", "gzip")):
    wire = (fixtures / f"{fixture}.msgpack").read_bytes()
    assert wire[:2] == bytes([0x93, 0xAB])
    assert wire[13] == 0xC4
    payload = wire[15 : 15 + wire[14]]
    (corpus / "packed_payload" / f"{output}.bin").write_bytes(payload)
    if fixture == "gzip_multimember":
        corrupt = bytearray(payload)
        corrupt[-8] ^= 1
        (corpus / "packed_payload" / "gzip_bad_checksum.bin").write_bytes(corrupt)
        (corpus / "packed_payload" / "gzip_truncated.bin").write_bytes(payload[:-1])

message = (fixtures / "message_eventtime.msgpack").read_bytes()
assert message[13:15] == bytes([0xD7, 0])
(corpus / "options_timestamps" / "eventtime.bin").write_bytes(message[13:23])
(corpus / "options_timestamps" / "chunk_option.bin").write_bytes(message[-21:])
(corpus / "options_timestamps" / "nil.bin").write_bytes(bytes([0xC0]))
(corpus / "framing" / "nil.bin").write_bytes(bytes([0xC0]))
(corpus / "envelope" / "reserved_marker.bin").write_bytes(bytes([0xC1]))


def _str(text):
    data = text.encode()
    assert len(data) < 256
    return (bytes([0xA0 | len(data)]) if len(data) < 32 else bytes([0xD9, len(data)])) + data


def _map(*pairs):
    assert len(pairs) < 16
    return bytes([0x80 | len(pairs)]) + b"".join(_str(k) + v for k, v in pairs)


# Message-mode envelopes whose records reach the `top_level` lifting path: a Docker
# record whose JSON log line has a reserved key (`docker`) next to ordinary keys,
# and a generic record with a nested `application` field.
docker_line = '{"msg":"started","level":30,"runId":"r1","docker":"x","application":{"a":1}}'
docker_record = _map(
    ("container_id", _str("abc")),
    ("container_name", _str("/worker")),
    ("source", _str("stdout")),
    ("log", _str(docker_line)),
)
generic_record = _map(
    ("message", _str("started")),
    ("runId", _str("r1")),
    ("logbrook", _map(("a", _str("b")))),
    ("application", _map(("a", bytes([1])))),
)
for name, tag, record in (
    ("lift_docker_json", "docker.worker", docker_record),
    ("lift_generic_reserved", "app.api", generic_record),
):
    wire = bytes([0x94]) + _str(tag) + bytes([0]) + record + _map(("chunk", _str("c1")))
    for target in ("envelope", "docker_normalize"):
        (corpus / target).mkdir(parents=True, exist_ok=True)
        (corpus / target / f"{name}.msgpack").write_bytes(wire)
