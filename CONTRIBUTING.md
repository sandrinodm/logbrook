# Contributing to Logbrook

For a substantial feature or API change, [open an issue](https://github.com/sandrinodm/logbrook/issues) first to discuss the use case and approach.

## Report a bug

Include the Logbrook version or commit, operating system, deployment method, relevant configuration, steps to reproduce, and expected and actual behavior. Use a small synthetic log sample when possible. Remove tokens, personal data, and application secrets from logs and configuration before sharing them.

Report suspected security vulnerabilities privately using [the security policy](SECURITY.md).

## Set up a development environment

Follow [build from source](README.md#build-from-source), then the [development checks](README.md#development). The Pino example uses Node.js with npm; CI uses Node.js 26.13.0.

Use a separate data directory for development. Tests and load generators write synthetic events, and some checks kill their own test servers on purpose. Keep them away from production data.

## Code style

Use four spaces for Rust and two for JavaScript, as `.editorconfig` sets them. Rustfmt and Prettier handle wrapping and spacing, and CI checks both. Keep developer-tool dependencies in the `xtask` package and Prettier in the Pino example, so neither ends up in the server image.

Format changed code from the repository root:

```sh
cargo fmt --all
npm --prefix examples/pino-logger run format
```

Separate logical steps with blank lines and give conditions and intermediate values descriptive names. Comments explain invariants, ordering requirements, or tradeoffs. Keep comments about transactions, cancellation, and resource cleanup next to the code they describe, and keep those guarantees intact when you move code around.

## Run the relevant checks

Run commands from the repository root. The [development section](README.md#development) lists the full set of checks CI runs.

For Rust changes:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
```

When changing the HTTP contract, update `src/openapi.json`, [the API reference](docs/API.md), and the affected API tests and examples.

For Forward codec and normalization changes, run the [property tests and libFuzzer targets](fuzz/README.md). The workspace tests include the property tests. The Forward fuzzing workflow runs the fuzz targets under AddressSanitizer, with a longer run every week.

For Forward receiver changes, also run the client interoperability test. It needs Python 3.9+, Docker, and OpenSSL, and it sends logs from Docker's `fluentd` logging driver, Fluentd (including TLS and shared-key authentication), and Fluent Bit to a native server. On macOS with Docker Desktop or OrbStack:

```sh
cargo build --package logbrook --locked
python3 tests/tools/forward_interop.py --binary target/debug/logbrook \
  --host host.docker.internal --network bridge \
  --output artifacts/forward/interop-macos.json
```

On Linux, use `--host 127.0.0.1 --network host` instead, as CI does. The script uses pinned Fluentd and Fluent Bit images, random ports, and temporary data and credentials, and removes its containers when it finishes. `--negative-control fluentd` or `--negative-control fluentbit` corrupts that client's ACKs, and the run must then fail. Run `python3 tests/tools/forward_interop.py --help` for the other options.

For Pino example changes:

```sh
(cd examples/pino-logger && npm ci && npm run format:check && npm test)
```

Run the process integration tests after changes to storage, retention, indexes, or the CLI. They use disposable data, and CI runs them with the workspace tests:

```sh
cargo test --package logbrook --test process --locked
```

For container changes, follow the [container verification steps](docs/CONTAINER.md#local-verification). For performance changes, include the workload, resource limits, record sizes, concurrency, and before and after measurements so someone else can reproduce them. The [developer tools guide](xtask/README.md) describes the load generators. Keep generated measurements in `artifacts/`, which Git ignores.

## Submit a pull request

- Keep changes focused on one problem and explain the resulting behavior.
- Include a regression test for a bug fix when practical. Prefer tests of observable behavior over implementation details.
- Update affected documentation and examples. Use plain language, clear headings, and no em dashes.
- List the checks you ran and anything they didn't cover. For storage or API changes, explain the compatibility and migration effects.
- Keep lockfiles in sync with dependency changes. Exclude credentials, database files, build output, and local experiment results.
- After dependency changes, regenerate and review the [third-party notices](licenses/README.md). CI checks that the distributed licenses match the lockfile.

Logbrook is licensed under [MIT](LICENSE). Contributions are accepted under the same license.

Maintainers publish versions, container images, and GitHub releases with the [release guide](docs/RELEASING.md).
