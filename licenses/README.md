# Dependency notices

Logbrook's source is licensed under MIT. Its dependencies retain their own licenses and attribution. The checked-in `THIRD_PARTY_NOTICES.txt` at the repository root accompanies the Linux executable in the Docker image. The base image supplies its own operating-system notices.

## Regenerate after dependency changes

Run these commands from the repository root:

```sh
cargo install cargo-about --version 0.9.2 --locked --features cli
cargo fetch --locked
cargo xtask notices
cargo xtask notices --check
```

Review and commit the resulting notice changes together with `Cargo.lock`. Normal application and Docker builds need no additional tool. CI regenerates the notices and compares their full contents; Docker also rejects a bundle whose lockfile fingerprint is stale. Image inspection checks that the image contains the checked-in bundle.

`about.toml` fixes both supported Linux image targets and the accepted license choices. `cargo-about` resolves the application dependency graph and checks its license expressions. The Rust generator then collects original `LICENSE`, `LICENCE`, `COPYING`, `COPYRIGHT`, and `NOTICE` files, including nested native files. Identical texts are shared without merging different copyright statements. Development-only and build-only dependencies are excluded. Some reproduced files describe alternative licenses or native components that are not linked into every target.

The generation command works offline after `cargo fetch`. A dependency without a license file or an explicitly reviewed supplement fails generation. Do not accept a newly encountered license without reviewing its terms and distribution requirements.

## DuckDB source supplements

The published `duckdb` Rust crate omits its workspace license, and `libduckdb-sys` omits license files from its bundled C++ source archive. `native.json` pins the crate versions, upstream repositories, immutable revisions, and supplemental notice files:

- `duckdb-rs.txt` reproduces the Rust workspace license at the crate's `.cargo_vcs_info.json` revision.
- `duckdb.txt` reproduces the DuckDB license and the native component licenses from the matching DuckDB release. Each text identifies its exact source URL. It covers the bundled source tree for the core, JSON, and Parquet features.

When updating DuckDB, inspect the new crate's `duckdb.tar.gz` and `manifest.json`, identify the matching upstream release, and compare its bundled native directories with the supplemental licenses. Refresh the texts from that immutable upstream revision, retaining their source URLs. Update the reviewed version and revision in `native.json`, then regenerate the root bundle. Generation deliberately rejects a DuckDB version change until its supplement has been reviewed.

Keep upstream license and notice text verbatim. Formatting conventions for Logbrook's own documentation do not apply to reproduced legal text.
