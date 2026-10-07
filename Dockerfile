# syntax=docker/dockerfile:1
FROM rust:1.99.0-bookworm@sha256:114c7a4425406451c2866b6aafe69fe29b1b298832db1277d411ac73c82d04d6 AS backend
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
FROM gcr.io/distroless/cc-debian13:nonroot@sha256:e792ab3d241a468a4fd7519ddbbebe66b49b5f365771716ea688ad40b6c6f1c2 AS runtime
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
EXPOSE 3100
VOLUME ["/data"]
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 CMD ["/usr/local/bin/logbrook", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/logbrook"]
CMD ["serve"]
