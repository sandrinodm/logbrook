# Install and configure Logbrook

## Select an image

Use a version or digest from the [GHCR package](https://github.com/sandrinodm/logbrook/pkgs/container/logbrook). Replace `<version>` below with a completed release's tag, without its Git `v` prefix. `latest` exists only when a release explicitly publishes it.

```sh
export LOGBROOK_IMAGE='ghcr.io/sandrinodm/logbrook:<version>'
docker pull "$LOGBROOK_IMAGE"
```

If the release workflow is still running, wait for it to publish before diagnosing a missing image. If an anonymous pull fails after publication, check the tag and the package visibility. Authenticate only when the user intends to use a private package.

To build the image from source instead, run this from a Logbrook checkout:

```sh
export LOGBROOK_IMAGE=logbrook:local
docker build --tag "$LOGBROOK_IMAGE" .
```

Use either Docker or Compose below, not both: both publish host port 3100.

## Create configuration for a new instance

In a dedicated deployment directory, create `.env` once; for an existing instance, reuse its credentials. This command checks the image variable and generates both credentials before it creates the file. It needs OpenSSL and refuses to overwrite an existing file:

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

Keep `.env` out of version control. The ingestion token can only write and the read token can only query; don't swap them. Add an admin token only when the user needs index administration. Ingestion and queries don't need one.

For administration, generate another credential with `openssl rand -hex 32` and store it as `LOGBROOK_ADMIN_TOKEN` in `.env`. Docker's `--env-file` passes it through. With Compose, also uncomment the admin-token mapping in the bundled template and recreate the service: Compose uses `.env` only for interpolation and passes only mapped variables, so the token in `.env` alone does nothing. Leave the mapping commented out when there is no admin token, because an empty token is invalid.

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

The health check fails until startup finishes; read `docker logs logbrook` if it keeps failing. The image runs as UID/GID `10001:10001` and sets the ownership of `/data` on a new named volume. For an existing volume or a host bind mount, make it writable by that UID/GID. The image has no shell, so run `logbrook` directly, not through `sh -c`.

## Docker Compose

Copy [the bundled template](../assets/compose.yaml) into the deployment directory as `compose.yaml`, next to `.env`. Resolve the template path from the installed skill directory. Set a fixed project name so later commands reuse the same volume:

```sh
export COMPOSE_PROJECT_NAME=logbrook
docker compose config --quiet
docker compose up -d --wait --wait-timeout 120
docker compose exec -T logbrook logbrook indexes list
```

The template relies on the image's built-in health check. `config --quiet` validates the file without printing the expanded credentials. Shell variables override `.env`, so unset stale ones or update them on purpose before recreating the service. To use the repository's root `compose.yaml` instead, which builds from source, run `docker compose up --build -d --wait` in the checkout.

## Connect applications and verify storage

| Client location | Logbrook base URL |
| --- | --- |
| Host running Docker | `http://127.0.0.1:3100` |
| Another service on the same Compose network | `http://logbrook:3100` |
| Another machine | The deployment's reachable HTTPS reverse-proxy URL |

Inside another container, `127.0.0.1` is that container. Join the same Docker network to use the service name. The default host binding is loopback only, so another machine can't reach it until you set up an ingress or reverse proxy. Logbrook serves plain HTTP; terminate TLS at the proxy.

Use [the ingestion probe](application-logging.md#verify-ingestion) to write a uniquely marked event and query it with the read credential. Restart with `docker restart logbrook` or `docker compose restart logbrook`, wait until it is ready, and query the same event again. If the setup is right, the event is still there. `docker compose down` keeps the volume; `down --volumes` deletes it.

## Retention, indexes, and TOML

The supplied configuration keeps logs for seven days and targets 20 decimal GB **per index**. Each index lives under `/data/indexes/<name>/`. `default` always takes one of the four index slots. Index names are 1 to 63 lowercase letters, digits, `_` or `-`, starting with a letter or digit.

- Retention must be longer than the archive age. For one-day retention, also lower `LOGBROOK_ARCHIVE_AFTER_MS` below one day.
- When an index is over its size target, events can expire before their age limit. The target is not a hard quota: writes, spill and pinned files can go over it. `507` means the index is still over its target and refuses ingestion.
- DuckDB memory, spill and queue budgets are divided by `max_indexes`, unused slots included, so raising the slot count alone shrinks each index's share.
- After changing `.env`, recreate the container with `docker compose up -d --wait`. A plain restart doesn't reload the environment.

For separate ingestion sources, scoped readers or per-index policies, mount a TOML file read-only at `/etc/logbrook/config.toml` and run `--config /etc/logbrook/config.toml serve`. In Compose, set `command: ["--config", "/etc/logbrook/config.toml", "serve"]` and add the file mount beside the data volume. In Docker, add the bind mount and put those command arguments after the image name. Match file permissions to UID/GID `10001:10001`.

Take exact keys from the commented [configuration example](https://github.com/sandrinodm/logbrook/blob/main/logbrook.example.toml), and replace every example credential before validating. Defaults are overridden by TOML, and TOML by the supported environment variables; a per-index policy wins over the global one. When moving to several TOML ingestion tokens, remove `LOGBROOK_INGEST_TOKEN`, because it replaces the whole ingestion map. `LOGBROOK_READ_TOKEN` adds one entry to the TOML read tokens instead of replacing them; if that token isn't already in the TOML file, also set `LOGBROOK_READ_SOURCES` to its sources (empty means all). Remove placeholder tokens from the file.

After editing the Compose mounts, environment, and TOML file, validate the proposed configuration in a fresh one-off container before recreating the server:

```sh
docker compose config --quiet
docker compose run --rm --no-deps -T logbrook \
  --config /etc/logbrook/config.toml check-config
```

The one-off container sees the new mounts and environment, which the running container can't. `check-config` doesn't open the data directory. Once it passes, apply the configuration with `docker compose up -d --wait`. Shortening retention can delete history on the next maintenance pass, and lengthening it doesn't bring expired data back.

Docker references: [volumes](https://docs.docker.com/engine/storage/volumes/), [Compose interpolation](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/), and [Compose networking](https://docs.docker.com/compose/how-tos/networking/).
