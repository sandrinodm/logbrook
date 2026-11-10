# syntax=docker/dockerfile:1.28.0@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b

# Build: install with the lockfile, then `pnpm build` writes the self-contained Nitro server to
# .output (the server entry, its traced node_modules and the client assets). Debian 13 matches the
# runtime image, so anything the build resolves for the platform also runs there.
FROM node:26.13.0-trixie-slim@sha256:5adad64f9a1fca2d51972caa65983634c8a774eb300d72b20f7f2a0f60368072 AS build
ENV CI=true
RUN npm install --global --no-fund --no-audit pnpm@12.1.0
WORKDIR /app
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN --mount=type=cache,id=logbrook-ui-pnpm,target=/pnpm/store,sharing=locked \
    pnpm install --frozen-lockfile --store-dir=/pnpm/store
COPY tsconfig.json vite.config.ts ./
COPY src/ ./src/
RUN pnpm build \
    && test -f .output/server/index.mjs \
    && test -d .output/public

# Run: distroless Node, which has no shell, package manager or other tools, as the image's
# unprivileged user. The build output is owned by root, so the server cannot modify its own code,
# and it writes nothing at run time, so the container can run with a read-only root filesystem.
FROM gcr.io/distroless/nodejs26-debian13:nonroot@sha256:33c86a63859efc2ebd88ea0a355863582ec6f1220d048de62baffb2584e709b3 AS runtime
ARG VCS_REF=unknown
ARG VERSION=0.1.0
LABEL org.opencontainers.image.title="logbrook-ui" \
      org.opencontainers.image.description="Web UI and backend-for-frontend for Logbrook" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.revision=$VCS_REF \
      org.opencontainers.image.version=$VERSION
# Required at run time: LOGBROOK_READ_TOKEN (the server refuses to start without it). Optional:
# LOGBROOK_URL (default http://127.0.0.1:3100) and LOGBROOK_ADMIN_TOKEN (enables creating and
# deleting indexes). The app has no authentication of its own: run it behind an authenticating
# reverse proxy or publish its port on loopback only (`-p 127.0.0.1:3000:3000`).
#
# Outside a container the app listens on 127.0.0.1 unless HOST says otherwise. Inside one that
# address is unreachable from the host, so the image sets HOST=0.0.0.0; what is reachable is then
# decided by how the port is published. With HOST at a non-loopback address the app logs a warning
# until ALLOWED_HOSTS names the hostname the browser uses (DNS rebinding protection; localhost,
# 127.0.0.1 and [::1] are always accepted, which the health check relies on).
ENV NODE_ENV=production \
    HOST=0.0.0.0 \
    PORT=3000
WORKDIR /app
COPY --from=build /app/.output ./.output
USER 65532:65532
EXPOSE 3000
# The server ends open live-tail streams on SIGTERM and exits within about a second.
STOPSIGNAL SIGTERM
# /healthz needs no Logbrook, so it answers whenever the process is up.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/nodejs/bin/node", "-e", "fetch('http://127.0.0.1:' + process.env.PORT + '/healthz').then((r) => process.exit(r.ok ? 0 : 1), () => process.exit(1))"]
ENTRYPOINT ["/nodejs/bin/node"]
CMD [".output/server/index.mjs"]
