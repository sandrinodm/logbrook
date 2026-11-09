# Dependency notices

Logbrook is MIT-licensed. Its dependencies keep their own licenses and attribution, collected in `THIRD_PARTY_NOTICES.txt` at the repository root. The Docker image ships that file next to the Linux executable. Operating-system notices come with the base image.

## Regenerate after dependency changes

Run these commands from the repository root:

```sh
cargo install cargo-about --version 0.9.3 --locked --features cli
cargo fetch --locked
cargo xtask notices
cargo xtask notices --check
```

Review the notice changes and commit them with `Cargo.lock`. Ordinary builds, including Docker builds, don't need `cargo-about`. CI regenerates the notices and compares the full text, and the Docker build fails if the lockfile fingerprint in `THIRD_PARTY_NOTICES.txt` is stale.

`about.toml` lists the two Linux image targets and the accepted licenses. `cargo xtask notices` runs `cargo-about` over the dependency graph for those targets, then copies each dependency's own `LICENSE`, `LICENCE`, `COPYING`, `COPYRIGHT`, and `NOTICE` files into the bundle. Development and build dependencies are left out. Some copied files describe alternative licenses or native components that aren't linked into every target.

Generation works offline after `cargo fetch`. It fails for a dependency that has no license file and no reviewed supplement. Before adding a new license to `about.toml`, review its terms and distribution requirements.

## DuckDB source supplements

The published `duckdb` crate leaves out its workspace license, and `libduckdb-sys` ships its bundled C++ source without license files. `native.json` records, for each crate, the version, upstream repository, revision, and the supplement file that fills the gap:

- `duckdb-rs.txt` reproduces the Rust workspace license at the crate's `.cargo_vcs_info.json` revision.
- `duckdb.txt` reproduces the DuckDB license and the licenses of the native components from the matching DuckDB release, covering the bundled source for the core, JSON, and Parquet features. Each text names its source URL.

To update DuckDB, inspect the new crate's `duckdb.tar.gz` and `manifest.json`, find the matching upstream release, and compare its bundled native directories with the supplements. Refresh the texts from that upstream revision and keep their source URLs. Update the version and revision in `native.json`, then regenerate the root bundle. Generation fails after a DuckDB version change until `native.json` matches it.

Keep upstream license and notice text verbatim, even where it breaks the style rules for Logbrook's own documentation.
