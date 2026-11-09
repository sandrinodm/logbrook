# Forward fuzzing

Five libFuzzer targets run the Forward parser and normalizers from `src/forward`
under AddressSanitizer with overflow checks. This is a standalone crate that
pulls in the `src/forward` modules by path, so it doesn't link storage or DuckDB,
and the fuzzing dependencies never reach the server executable.

| Target | Input and what it checks |
| --- | --- |
| `framing` | Arbitrary wire bytes; framing the whole buffer and framing it in fragments agree, and the capacity used stays within the preflight estimate |
| `envelope` | Whole envelopes, including gzip; each accepted record decodes from its preserved bytes and normalizes the same way every time |
| `packed_payload` | Raw packed entries or gzip members inside valid bin32 or str32 envelopes; payload framing, corruption and expansion limits |
| `options_timestamps` | Arbitrary MessagePack bytes in the options and timestamp positions of an otherwise valid envelope |
| `docker_normalize` | Whole envelopes normalized with the text, Pino and `json_auto` profiles; output is deterministic, bytes are preserved and messages stay within their limit |

Install the pinned tools and run from the repository root:

```sh
rustup toolchain install nightly-2026-11-01 --profile minimal --component rust-src
cargo +nightly-2026-11-01 install cargo-fuzz --version 0.13.2 --locked
python3 fuzz/seed_corpus.py
cargo +nightly-2026-11-01 fuzz run envelope -- \
  -max_total_time=60 -max_len=65536 -timeout=3 \
  -rss_limit_mb=1024 -malloc_limit_mb=32 -dict="$PWD/fuzz/forward.dict"
```

Swap in another target name to fuzz a different surface, and raise
`-max_total_time` for longer runs before a release. cargo-fuzz builds the
standard library with the same sanitizer and checks.

`fuzz/src/lib.rs` caps every input at 64 KiB, expanded data at 256 KiB, decoded
allocations at 2 MiB, and envelopes at 128 events, 8 gzip members, 16 levels of
nesting and 8,192 values. Normalized envelopes are capped at 1 MiB. LibFuzzer
also reports slow inputs, large single allocations and high RSS. These are
fuzzing limits, not the server's.

`fuzz/Cargo.lock` is separate from the application lockfile. Update it only when
you change the fuzz crate's dependencies.

## Corpus

The seeds come from the Ruby msgpack fixtures described in
[the fixtures README](../tests/fixtures/forward/README.md). `seed_corpus.py`
checks the known wrapper bytes before it slices out raw entries, gzip payloads,
EventTime values and option seeds. `forward.dict` holds MessagePack markers,
protocol control keys, gzip headers, Docker fields and structured-log content.

Git ignores new corpus files until someone reviews and minimizes them. The
checked-in `.msgpack` and `.bin` seeds stay versioned. Never add production logs,
credentials or client captures with personal information to the corpus.

## CI and crashes

The [Forward fuzz workflow](../.github/workflows/forward-fuzz.yml) runs each
target for 30 seconds on pull requests and pushes to `main` that change the
Forward code, the fuzz crate or the lockfile, and for five minutes every Monday.
A failed run keeps the crash artifacts and logs. Reproduce a crash with
`cargo +nightly-2026-11-01 fuzz run TARGET PATH_TO_ARTIFACT` and minimize it with
`cargo +nightly-2026-11-01 fuzz tmin TARGET PATH_TO_ARTIFACT`. Check in the
minimized input and a focused regression test before you fix the bug.

## Property tests

The shrinking property tests run with the normal workspace tests:

```sh
cargo test --package logbrook --test forward_properties --locked
```

They generate recursive MessagePack values, duplicate and invalid UTF-8 keys,
arbitrary float bits, random fragment boundaries, invalid final records and
constrained untrusted input. They check structural round trips, deterministic
normalization, exact fallback reconstruction, that fragmented and whole framing
agree, and that invalid envelopes are rejected whole. When a property fails,
proptest saves the minimal counterexample; review it and commit it.
