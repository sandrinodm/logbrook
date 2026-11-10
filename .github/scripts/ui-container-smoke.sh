#!/usr/bin/env bash
# Smoke-tests a loaded Logbrook UI image: usage is ui-container-smoke.sh IMAGE.
# UI_SMOKE_PORT picks the loopback port to publish (default 3000).
set -euo pipefail
image=${1:?usage: ui-container-smoke.sh IMAGE}
port=${UI_SMOKE_PORT:-3000}
name=logbrook-ui-smoke-$$
token=ci-read-token-0123456789
url=http://127.0.0.1:$port

cleanup() {
  docker rm --force "$name" > /dev/null 2>&1 || true
}
trap cleanup EXIT

# The same hardening the documentation recommends.
start() {
  docker run --detach --name "$name" --read-only --cap-drop ALL --security-opt no-new-privileges \
    --publish "127.0.0.1:$port:3000" --env LOGBROOK_READ_TOKEN="$token" "$@" "$image" > /dev/null
}

wait_healthy() {
  for _ in $(seq 1 30); do
    if curl --fail --silent --output /dev/null "$url/healthz"; then
      return 0
    fi
    sleep 1
  done
  docker logs "$name" >&2
  echo 'The UI container never answered /healthz' >&2
  return 1
}

status() {
  curl --silent --output /dev/null --write-out '%{http_code}' --header "Host: $1" "$url/healthz"
}

echo 'The container refuses to start without a read token'
if output=$(docker run --rm "$image" 2>&1); then
  echo "$output"
  echo 'The container started without LOGBROOK_READ_TOKEN' >&2
  exit 1
fi
echo "$output"
grep --fixed-strings --quiet 'LOGBROOK_READ_TOKEN is required' <<< "$output"

echo 'The container answers /healthz, warns about ALLOWED_HOSTS and runs unprivileged'
start
wait_healthy
docker logs "$name" 2>&1 | grep --fixed-strings --quiet 'ALLOWED_HOSTS is not set'
test "$(docker inspect --format '{{.Config.User}}' "$image")" = 65532:65532
test "$(docker exec "$name" /nodejs/bin/node -p 'process.getuid()')" = 65532
cleanup

echo 'The container enforces ALLOWED_HOSTS'
start --env ALLOWED_HOSTS=ui.example.test
wait_healthy
test "$(status ui.example.test)" = 200
test "$(status evil.example)" = 421
