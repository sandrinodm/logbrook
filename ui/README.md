# logbrook-ui

A web UI for [Logbrook](../README.md): search and page through events, filter by facet, follow a live tail, and manage indexes. It is a TanStack Start app on Nitro. One Node process serves the pages and a small backend-for-frontend (BFF) that talks to the Logbrook HTTP API. Nothing in the Rust server changed.

The browser never sees a Logbrook token. The BFF reads its tokens from its environment and adds the `Authorization` header itself. A BFF rather than direct browser access, because the Logbrook server sends no CORS headers, accepts only bearer tokens and does not compress responses.

## No authentication

**This app has no login, no sessions and no users.** Everyone who can reach it gets the access of the Logbrook tokens it is configured with: they can read everything the read token can read, and, when `LOGBROOK_ADMIN_TOKEN` is set, they can create and delete indexes. Bind it to loopback, or put it behind a reverse proxy that authenticates users (Traefik with forward auth, oauth2-proxy and the like) and publish only the proxy. The proxy authenticates people; it does not stop cross-site requests, which is why the server functions that change indexes keep TanStack Start's same-origin check (see below).

## Run it locally

Needs Node 26 and pnpm 12.1.

```sh
pnpm install
pnpm dev:logbrook
```

`pnpm dev:logbrook` starts a throwaway Logbrook from `../target/debug/logbrook` (build it first with `cargo build` in the repository root), ingests about 1,200 sample events into an index called `itest`, writes one new event every two seconds, and prints its URL, its tokens and the exact environment line to start the UI with. Ctrl-C stops it and deletes the data.

In a second terminal, use the line it printed (the port and the tokens are the ones it shows):

```sh
LOGBROOK_URL=http://127.0.0.1:<port> LOGBROOK_READ_TOKEN=<read token> LOGBROOK_ADMIN_TOKEN=<admin token> pnpm dev
```

Leave `LOGBROOK_ADMIN_TOKEN` out to see the UI as a read-only deployment.

Open <http://localhost:5173>. There is nothing to sign in to. To use your own server instead, point `LOGBROOK_URL` at it and set `LOGBROOK_READ_TOKEN` (and `LOGBROOK_ADMIN_TOKEN` if you want index management).

## Environment

| Variable               | Default                 |                                                                                                                                                                                                                            |
| ---------------------- | ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LOGBROOK_URL`         | `http://127.0.0.1:3100` | Base URL of the Logbrook server, as the UI's server reaches it.                                                                                                                                                            |
| `LOGBROOK_READ_TOKEN`  | none, **required**      | A Logbrook read token. The proxy sends it for every read. The server refuses to start without it, in development too.                                                                                                      |
| `LOGBROOK_ADMIN_TOKEN` | none                    | A Logbrook admin token, different from the read token. Enables creating and deleting indexes and the full index list. Without it those controls are hidden and the server functions answer that management is unavailable. |
| `PORT`                 | `3000`                  | Port of the production server (`pnpm start`). The dev server uses 5173.                                                                                                                                                    |
| `HOST`                 | `127.0.0.1`             | Address the production server listens on. The default is loopback, so only this machine can reach it. Set `0.0.0.0` (the Docker image does) to listen on all interfaces. `NITRO_HOST` takes precedence.                    |
| `ALLOWED_HOSTS`        | none                    | Comma-separated host names the server answers to, ports ignored. Any other `Host` header gets `421`. See "DNS rebinding" below.                                                                                            |

Tokens are 16 to 512 visible ASCII characters without whitespace, as Logbrook requires. A variable that is empty or only whitespace counts as not set. A badly shaped token, or the same value for both tokens, stops the server at boot with a message that names the variable, never the value.

At boot the server also tries both tokens against Logbrook once and logs a warning (it does not stop) if Logbrook is unreachable, rejects a token, or if the read and admin tokens look swapped. Logbrook may simply start later than the UI. A `401` from Logbrook while the UI runs is shown as "the server's Logbrook token was rejected"; the operator has to fix `LOGBROOK_READ_TOKEN` and restart.

`.env.example` lists the variables. `pnpm dev` reads a `.env` file in `ui/` (copy `.env.example`); `pnpm start` and the container do not, so set the variables in the environment there.

## Access and token custody

- The BFF reads the tokens from the environment at start and keeps them in memory. They are never sent to the browser, never logged, and not in any URL. There is no user database and no cookie. Logbrook's own source and index scopes still decide what a token can read.
- Browser requests to `/api/lb/...` go through a proxy that allows `GET` on the read endpoints only (`/indexes`, `/indexes/{index}`, `/logs`, `/logs/facets`, `/logs/histogram`, `/logs/tail`, `/metrics`), never ingestion. It always sends the configured **read** token and ignores any `Authorization` header or cookie the browser sends. The admin token is used only by server functions, for the index list with sizes and for creating and deleting indexes.
- Server functions that change indexes (`createIndexFn`, `deleteIndexFn`) carry TanStack Start's built-in same-origin check: a request with `Sec-Fetch-Site` other than `same-origin`, or an `Origin` of another site, gets `403`. The app has no `src/start.ts`, so Start installs that check by default; `e2e/csrf-browser.e2e.ts` replays a recorded call with cross-site headers and expects `403`.
- What the UI knows about access comes from one small server function (`getAccessFn`): whether an admin token is configured. It reports no token. Whether metrics are allowed is learned from the `/metrics` answer (`403` means the read token is restricted), and whether index sizes are allowed from the shape of the index list.
- **DNS rebinding.** A server on loopback without authentication can be reached by a malicious web page whose host name it resolves to `127.0.0.1`. Every request, static files included, therefore has its `Host` header checked before anything else runs, and a name that is not allowed gets `421 Misdirected Request` with a short plain-text body and no upstream call. With `ALLOWED_HOSTS` set, those names are allowed. Without it, a server bound to a loopback address allows only `localhost`, `127.0.0.1` and `[::1]`, and a server bound anywhere else (the container) allows any host and logs one warning at boot that asks for `ALLOWED_HOSTS`. `localhost`, `127.0.0.1` and `[::1]` are always allowed, because a page on another origin cannot make a browser send them, and the container health check and a local `curl` rely on it. Behind a reverse proxy that does not pass the original `Host` on, set `ALLOWED_HOSTS` to the name the proxy sends.
- Views follow what the tokens can do: a source-restricted read token gets index names but no sizes or metrics, and the create and delete controls need a configured admin token.

## Keyboard

`Ctrl K` or `⌘ K` opens the command palette (views, index, time range, theme). `?` lists the shortcuts. `t` switches between Logs and Live Tail and keeps the index and filters. In the log table: `j`/`k` or the arrow keys move and open the next or previous event, `Enter` opens the focused one, `Esc` closes the panel, `/` searches the message. The histogram is a stop in the tab order: `←` and `→` move a cursor from bucket to bucket (`Home` and `End` jump to the ends), `Shift` with them extends a selection, `Enter` zooms to the selection (or to the cursor's bucket), `Esc` clears it. The header reads out the bucket or the selection, which a screen reader announces. Single keys are ignored while you type in a field or have a dialog open.

Going from Live Tail (or Indexes, or Ingest) back to Logs, through the nav, `t` or the command palette, brings back the time range (`range`, or `from` and `to`), pinned columns, density, time zone and wrap the explorer had when you left it, in the same tab. The filters of the page you come from win, and a relative range gets a new "now". The state lives in memory, so a reload or a link opened directly starts clean, and it never appears in the tail's URL.

The theme follows `prefers-color-scheme`. The theme button cycles system, light and dark and remembers the choice in `localStorage`.

## Tests and checks

```sh
pnpm check              # format, Oxlint with type checking, JSDoc on every declaration
pnpm test               # unit tests (Vitest)
pnpm lint:react         # react-doctor
pnpm build              # production build into .output
pnpm test:e2e           # builds, then runs e2e/ with the e2e runner against a real logbrook binary
```

`pnpm test:e2e` starts `../target/debug/logbrook` (or `LOGBROOK_BIN`) with a temporary data directory, runs the built BFF in front of it, and drives it with plain HTTP and through the [`e2e`](https://e2e.com) runner and its `@e2e-dev/web` Chromium engine. The BFF reads its tokens at boot, so each token setup a test needs (read-only, with an admin token, a source-restricted read token, a wrong token) is its own BFF process. Install the browser once:

```sh
pnpm exec e2e-web install chromium
```

The script sets `E2E_TELEMETRY_DISABLED=1`, which also turns off the update check. Run part of the suite, or watch it, with the binary directly (the script also builds, and the flags below need the build to be current):

```sh
export E2E_TELEMETRY_DISABLED=1              # the CLI reports usage otherwise
pnpm exec e2e run proxy                      # one file, by name
pnpm exec e2e run --grep 'pages through'     # tests by title
pnpm exec e2e run --headed shell-browser     # see the browser
pnpm exec e2e list                           # the 87 tests, without running them
```

A green exit code does not prove every test ran, because skipped and flaky tests also exit 0. `.e2e/report.json` has the counts under `run.summary`, and a failed test leaves `.e2e/results/<test>/trace.md` with its steps and a screenshot. `.e2e/` is not committed; CI uploads it when the job fails.

Every process and temporary directory a test starts is recorded, and removed when the run ends, when it is interrupted (Ctrl+C, `SIGTERM`) or when a worker is killed: each worker removes its own on exit, and `e2e.config.ts` opens a registry in the runner process (`e2e/support/run-registry.ts`) whose reporter stops whatever a dead worker left behind. Each test file's `afterAll` runs all of its stops even when one of them fails. A worker whose event loop is blocked when the run is forced with a second interrupt cannot clean up after itself, so the runner kills it as it exits, and the engine's `chrome-headless-shell` ends with it.

### Performance

`pnpm perf` runs the budget measurements from the plan against a production build and a real Logbrook with 60,000 events. Build first, then pick the measurements:

```sh
pnpm build
pnpm perf size filter scroll tail
```

`size` is gzipped JavaScript per route, `filter` is the time from a filter click to painted rows, `scroll` scrolls 50,000 loaded rows and counts long tasks and dropped frames, `tail` follows 1,000 events per second for 20 seconds. Without arguments it runs all four.

`rerenders` reports which components render when a row is selected. It needs the profiling build, which maps `react-dom/client` to `react-dom/profiling` and skips minification so component names survive:

```sh
pnpm build:profile
pnpm perf rerenders
```

Both builds write to `.output`, so run `pnpm build` again afterwards.

To see what is in the client bundle, build with the stats plugin and read the report. `pnpm build:analyze` writes `bundle-stats.json` (rendered size of every module in every client chunk, before minification), and `pnpm bundle-report` prints it per package and per chunk with the gzip size of each chunk; pass a chunk name fragment to list that chunk's largest modules:

```sh
pnpm build:analyze
pnpm bundle-report
```

## Deploy

Remember that the app has no authentication (see above). The production server listens on `127.0.0.1` unless `HOST` says otherwise; put an authenticating reverse proxy in front before you expose it.

```sh
pnpm build
LOGBROOK_URL=http://127.0.0.1:3100 LOGBROOK_READ_TOKEN=<read token> LOGBROOK_ADMIN_TOKEN=<admin token> pnpm start
```

`pnpm start` runs `node .output/server/index.mjs`, which needs nothing else on disk. Leave `LOGBROOK_ADMIN_TOKEN` out for a read-only deployment. On `SIGTERM` or `SIGINT` the server stops accepting connections, ends the open live-tail streams (the browser reconnects with its last event ID) and exits as soon as the other requests finish, about a second after the signal; Nitro's 5 s grace period (`SERVER_SHUTDOWN_TIMEOUT`) is only the upper bound.

As a container. The image builds on Node 26 and runs on distroless Node 26 (no shell or package manager) as the non-root user `65532`, with only the build output in it, owned by root. Base images are pinned by digest. `HOST=0.0.0.0` is set inside because loopback in a container is unreachable from the host, and the health check calls `/healthz`, which does not call Logbrook. The server writes nothing to disk, so `--read-only` works. The build context is an allowlist in `.dockerignore`, so a new build input must be added there.

```sh
cd ui
docker build --tag logbrook-ui --build-arg VCS_REF="$(git rev-parse HEAD)" .
docker run --read-only --cap-drop ALL --security-opt no-new-privileges \
  --publish 127.0.0.1:3000:3000 \
  --env LOGBROOK_URL=http://host.docker.internal:3100 \
  --env LOGBROOK_READ_TOKEN=<read token> \
  --env ALLOWED_HOSTS=localhost \
  logbrook-ui
```

Set `ALLOWED_HOSTS` to the name your browser uses (the public host name behind a proxy). Without it the container accepts any `Host` header and logs a warning at boot.

[`../examples/logbrook-ui/compose.yaml`](../examples/logbrook-ui/compose.yaml) runs the server and the UI together by including the repository's `compose.yaml` and adding a `logbrook-ui` service. It publishes the UI on `127.0.0.1:3000` only and sets `ALLOWED_HOSTS` to `UI_ALLOWED_HOSTS` (default `localhost`); set `UI_ALLOWED_HOSTS` to the public host name when a proxy sits in front. It reuses `LOGBROOK_READ_TOKEN` and `LOGBROOK_ADMIN_TOKEN` of the server's own configuration.

```sh
LOGBROOK_INGEST_TOKEN=... LOGBROOK_READ_TOKEN=... \
docker compose -f examples/logbrook-ui/compose.yaml up --build
```

To reach the UI from other machines, put a reverse proxy that authenticates users in front of it and publish only the proxy; terminate TLS there too. The proxy must not buffer `/api/lb/.../logs/tail`, which is a server-sent event stream.

## Layout

| Path               | What                                                                                                                                                                                                          |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/routes`       | File routes: the `_app` layout (index list, access info, nav, shortcuts), and the Logs, Tail, Indexes and Ingest views. `routes/api/lb/$.ts` is the proxy and `routes/healthz.ts` the container health check. |
| `src/server`       | Server-only code (`*.server.ts`, kept out of the client bundle by import protection): config and token validation, boot check of the tokens, proxy, allowlist.                                                |
| `src/functions`    | Server functions: access info, index management.                                                                                                                                                              |
| `src/lib/logbrook` | Typed client, query options, tail store and reconnect logic, Prometheus text parser.                                                                                                                          |
| `src/lib/explorer` | URL search params, time ranges, row arithmetic for the virtual table.                                                                                                                                         |
| `src/components`   | Views and the vendored base components in `ui/`.                                                                                                                                                              |
| `e2e`              | Integration and browser tests (`*.e2e.ts`, run by `e2e`), the throwaway server (`dev-logbrook.ts`) and `perf.ts`.                                                                                             |
