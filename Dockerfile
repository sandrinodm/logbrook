# syntax=docker/dockerfile:1.28.0@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b
FROM rust:1.99.1-bookworm@sha256:9181b56c2a21ab1d5e73cffe04fc762426d30aa7fac3c91141dd033f49a7fc8b AS backend
ARG TARGETARCH
WORKDIR /build
COPY Cargo.toml Cargo.lock rust-toolchain.toml clippy.toml ./
COPY xtask/Cargo.toml ./xtask/Cargo.toml
COPY src/ ./src/
COPY migrations/ ./migrations/
RUN --mount=type=cache,id=logbrook-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=logbrook-target-${TARGETARCH},target=/build/target,sharing=locked \
    touch src/main.rs src/lib.rs \
    && cargo build --locked --release --package logbrook --jobs 2 && cp target/release/logbrook /usr/local/bin/logbrook

# A dependency update must regenerate the checked-in distribution notices.
COPY THIRD_PARTY_NOTICES.txt ./
RUN grep -Fx "Cargo.lock SHA-256: $(sha256sum Cargo.lock | cut -d ' ' -f 1)" THIRD_PARTY_NOTICES.txt > /dev/null

# Verify the actual release executable with build tools, never runtime packages.
FROM backend AS verified
RUN strip --strip-unneeded /usr/local/bin/logbrook \
    && /usr/local/bin/logbrook --version \
    && ldd /usr/local/bin/logbrook > /tmp/logbrook-linkage \
    && ! grep -q 'not found' /tmp/logbrook-linkage \
    && mkdir /runtime-data && chown 10001:10001 /runtime-data

# cc supplies the glibc/C++ runtime required by bundled DuckDB, plus CA roots.
FROM gcr.io/distroless/cc-debian13:nonroot@sha256:08476c244888bfb425bae1ae4dd7c17f093e972aab8426883d97f3c6f280f7c1 AS runtime
ARG VCS_REF=unknown
ARG VERSION=0.1.0
LABEL org.opencontainers.image.title="logbrook" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.description="Self-hosted log search with DuckDB and Parquet" \
      org.opencontainers.image.revision=$VCS_REF \
      org.opencontainers.image.version=$VERSION
COPY --from=verified /usr/local/bin/logbrook /usr/local/bin/logbrook
COPY LICENSE /usr/share/licenses/logbrook/LICENSE
COPY --from=backend /build/THIRD_PARTY_NOTICES.txt /usr/share/licenses/logbrook/THIRD_PARTY_NOTICES.txt
COPY --from=verified --chown=10001:10001 /runtime-data /data
USER 10001:10001
ENV LOGBROOK_BIND=0.0.0.0:3100 LOGBROOK_DATA_DIR=/data
EXPOSE 3100 24224/tcp 24224/udp
VOLUME ["/data"]
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 CMD ["/usr/local/bin/logbrook", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/logbrook"]
CMD ["serve"]
