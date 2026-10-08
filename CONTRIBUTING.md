# Contributing to Logbrook

Contributions to the server, CLI, examples, and documentation are welcome. For a substantial feature or API change, open an issue first to discuss the use case and approach.

## Report a bug

Include the Logbrook version or commit, operating system, deployment method, relevant configuration, steps to reproduce, and expected and actual behavior. Use a small synthetic log sample when possible. Remove tokens, personal data, and application secrets from logs and configuration before sharing them.

Report suspected security vulnerabilities privately using [the security policy](SECURITY.md).

## Set up a development environment

Follow the [build prerequisites and quick start](README.md#quick-start). The server and CLI use the pinned Rust toolchain and a bundled DuckDB build. The Rust workspace includes the integration checks and developer tools; see the [development checks](README.md#development). Only the Pino example needs Node.js 24 with npm.

Use a separate data directory for development. Tests and load generators write synthetic events, and some checks deliberately stop or kill their own test servers. Keep them away from production data.

## Code style

Use four spaces for Rust and two for JavaScript. The repository's `.editorconfig` supplies editor defaults. Rustfmt and Prettier handle wrapping and spacing; CI checks both languages. Developer-tool dependencies stay in the `xtask` package, and Prettier stays in the Pino example. Neither is part of the server image.

Format changed code from the repository root:

```sh
cargo fmt --all
npm --prefix examples/pino-logger run format
```

Separate logical steps with blank lines and give conditions and intermediate values descriptive names. Comments should explain invariants, ordering requirements, or tradeoffs. Keep transaction, cancellation, and resource-cleanup comments beside the code they explain, and preserve those guarantees when reorganizing code.

## Run the relevant checks

Run commands from the repository root. The [development section](README.md#development) lists the complete local check sequence corresponding to CI.

For Rust changes:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
```

When changing the HTTP contract, update `src/openapi.json`, [the API reference](docs/API.md), and the affected API tests and examples.

For Pino example changes:

```sh
(cd examples/pino-logger && npm ci && npm run format:check && npm test)
```

Run the process integration tests after changes to storage, retention, indexes, or the CLI. They create disposable data and are also run by CI as part of the workspace tests:

```sh
cargo test --package logbrook --test process --locked
```

For container changes, use the [container verification instructions](docs/CONTAINER.md#local-verification). Performance changes should include a reproducible workload, resource limits, record sizes, concurrency, and before/after measurements. See the [developer tools guide](xtask/README.md) for the reusable load generators and validation scripts. Keep generated measurements in the ignored `artifacts/` directory.

## Submit a pull request

- Keep changes focused on one problem and explain the resulting behavior.
- Include a regression test for a bug fix when practical. Prefer tests of observable behavior over implementation details.
- Update affected documentation and examples. Use plain language, clear headings, and no em dashes.
- List the checks you ran and any limitations in the results. For storage or API changes, explain compatibility and migration effects.
- Keep lockfiles in sync with dependency changes. Exclude credentials, database files, build output, and local experiment results.
- After dependency changes, regenerate and review the [third-party notices](licenses/README.md). CI checks that the distributed licenses match the lockfile.

Logbrook is licensed under [MIT](LICENSE). Contributions are accepted under the same license.

Maintainers can follow the [release guide](docs/RELEASING.md) to prepare versions and publish verified container images and GitHub releases.
