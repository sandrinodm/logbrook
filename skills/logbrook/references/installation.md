# Install and configure Logbrook

## Select an image

Use a version or digest from the [GHCR package](https://github.com/sandrinodm/logbrook/pkgs/container/logbrook). Replace `<version>` below with a completed release's tag, without its Git `v` prefix. `latest` exists only when a release explicitly publishes it.

```sh
export LOGBROOK_IMAGE='ghcr.io/sandrinodm/logbrook:<version>'
docker pull "$LOGBROOK_IMAGE"
```

If that release is still running, wait for publication before diagnosing a missing image. Once publication completes, check the selected tag and package visibility if anonymous pulling fails. Authenticate only when the user intends to use a private package.

For an optional source build, run this from a Logbrook checkout:

```sh
export LOGBROOK_IMAGE=logbrook:local
docker build --tag "$LOGBROOK_IMAGE" .
```

Choose either Docker or Compose below for the instance. Both examples use host port 3100; running both unchanged would conflict.

## Create configuration for a new instance

In a dedicated deployment directory, create `.env` once. Reuse existing credentials for an existing instance. This command validates the image variable and generates both credentials before creating the file. It requires OpenSSL and refuses to overwrite an existing file:

```sh
(
  set -eu
  : "${LOGBROOK_IMAGE:?Choose an image first}"
  command -v openssl > /dev/null
  ingest_token=$(openssl rand -hex 32)
  read_token=$(openssl rand -hex 32)
  test "${#ingest_token}" -eq 64
  test "${#read_token}" -eq 64

  umask 077
  set -C
  cat > .env <<EOF
LOGBROOK_IMAGE=$LOGBROOK_IMAGE
LOGBROOK_INGEST_TOKEN=$ingest_token
LOGBROOK_READ_TOKEN=$read_token
LOGBROOK_BIND=0.0.0.0:3100
LOGBROOK_DATA_DIR=/data
LOGBROOK_SOURCE=apps
LOGBROOK_INDEXES=apps
LOGBROOK_MAX_INDEXES=4
LOGBROOK_RETENTION_DAYS=7
LOGBROOK_MAX_SIZE_GB=20
LOGBROOK_ARCHIVE_AFTER_MS=86400000
LOGBROOK_MAINTENANCE_INTERVAL_SECS=60
EOF
)
```

Keep `.env` out of version control. The ingest and read tokens are separate roles, not interchangeable credentials. Add an admin token only when index administration requires one; it is unnecessary for ordinary ingestion and queries.

For administration, generate another credential with `openssl rand -hex 32` and store it as `LOGBROOK_ADMIN_TOKEN` in `.env`. Docker's `--env-file` forwards it automatically. With Compose, also uncomment the admin-token mapping in the bundled template, then recreate the service. Compose uses `.env` for interpolation and forwards only explicitly mapped variables; adding the token to `.env` alone does not enable administration. Leave the mapping commented when no admin token is configured, since an empty token is invalid.

## Docker

In the deployment directory, load the generated environment and run the image:

```sh
set -a
. ./.env
set +a

docker run -d --name logbrook \
  --restart unless-stopped --stop-timeout 35 \
  --publish 127.0.0.1:3100:3100 \
  --env-file .env \
  --mount type=volume,src=logbrook-data,dst=/data \
  --read-only --tmpfs /tmp:rw,noexec,nosuid,size=256m,mode=1777 \
  --cap-drop ALL --security-opt no-new-privileges:true \
  "$LOGBROOK_IMAGE"

docker exec logbrook logbrook healthcheck
docker exec logbrook logbrook indexes list
```

Allow startup to finish before the health check succeeds; inspect `docker logs logbrook` on failure. The image runs as UID/GID `10001:10001` and initializes its `/data` ownership for a fresh named volume. For an existing volume or a host bind mount, arrange writable ownership for that UID/GID. The image has no shell; execute `logbrook` directly, not `sh -c`.

## Docker Compose

Copy [the bundled template](../assets/compose.yaml) into the deployment directory as `compose.yaml`, alongside `.env`. Resolve the template path from the installed skill directory. Set a stable project name so later commands reuse the same volume:

```sh
export COMPOSE_PROJECT_NAME=logbrook
docker compose config --quiet
docker compose up -d --wait --wait-timeout 120
docker compose exec -T logbrook logbrook indexes list
```

The template uses the image's built-in health check. `config --quiet` validates without printing expanded credentials. Shell variables take precedence over `.env`; unset stale overrides or intentionally update them before recreating the service. To use the repository's own root `compose.yaml` instead, run `docker compose up --build -d --wait` from the checkout; that file builds from source.

## Connect applications and verify storage

| Client location | Logbrook base URL |
| --- | --- |
| Host running Docker | `http://127.0.0.1:3100` |
| Another service on the same Compose network | `http://logbrook:3100` |
| Another machine | The deployment's reachable HTTPS reverse-proxy URL |

Inside another container, `127.0.0.1` refers to that container. Join the same Docker network to use service DNS. The default loopback host binding is not reachable from another machine; configure the chosen ingress/proxy explicitly. Logbrook serves HTTP; TLS terminates at the proxy.

Use [the ingestion probe](application-logging.md#verify-ingestion) to write a uniquely marked event, then query it using the read credential. Restart with `docker restart logbrook` or `docker compose restart logbrook`, wait for readiness, and query the same event again. A successful setup retains that event in the named volume across restart. `docker compose down` retains the volume; `down --volumes` deletes it.

## Retention, indexes, and TOML

The supplied configuration retains logs for seven days and targets 20 decimal GB **per index**. Each index lives under `/data/indexes/<name>/`. `default` always counts toward the four index slots. Index names allow 1–63 lowercase letters, digits, `_`, or `-`, starting with a letter or digit.

- Retention must exceed the archival age. For one-day retention, reduce `LOGBROOK_ARCHIVE_AFTER_MS` below one day as well.
- Size pressure may expire events before their age limit. The size target is not a hard quota; writes, spill, and pinned files can exceed it. `507` means the index still cannot accept ingestion at its target.
- DuckDB memory, spill, and queue budgets divide across `max_indexes`, including unused slots. Raising the slot count alone reduces each index's share.
- Environment changes require container recreation: use `docker compose up -d --wait` after changing `.env`. A plain container restart does not reload its environment.

For distinct ingestion sources, scoped readers, or per-index policies, mount a TOML file read-only at `/etc/logbrook/config.toml` and run `--config /etc/logbrook/config.toml serve`. In Compose, set `command: ["--config", "/etc/logbrook/config.toml", "serve"]` and add the file mount beside the data volume. In Docker, add the bind mount and put those command arguments after the image name. Match file permissions to UID/GID `10001:10001`.

Use the fully commented [configuration example](https://github.com/sandrinodm/logbrook/blob/main/logbrook.example.toml) for exact keys. Replace every example credential before validation. Settings resolve as defaults, TOML, then supported environment overrides, with explicit per-index policies taking precedence over inherited global policies. When moving to multiple TOML ingestion tokens, remove the single-token environment setting: `LOGBROOK_INGEST_TOKEN` replaces the entire ingestion map. Read-token environment overrides add entries rather than clearing the TOML map; remove placeholders and explicitly configure source scopes.

After editing the Compose mounts, environment, and TOML file, validate the proposed configuration in a fresh one-off container before recreating the server:

```sh
docker compose config --quiet
docker compose run --rm --no-deps -T logbrook \
  --config /etc/logbrook/config.toml check-config
```

This uses the proposed mounts and environment, including changes the running container cannot see. `check-config` validates without opening the data directory. Once it succeeds, apply the configuration with `docker compose up -d --wait`. Shortening retention can delete history on the next maintenance pass; increasing it cannot recover expired data.

Docker references: [volumes](https://docs.docker.com/engine/storage/volumes/), [Compose interpolation](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/), and [Compose networking](https://docs.docker.com/compose/how-tos/networking/).
