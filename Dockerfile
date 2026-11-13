# syntax=docker/dockerfile:1.7
FROM rust:1.95-bookworm@sha256:6258907abe69656e41cd992e0b705cdcfabcbbe3db374f92ed2d47121282d4a1 AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang libclang-dev cmake pkg-config python3 perl make g++ \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build/nacket
COPY ci/build-image.py ci/license-inventory.py ci/source-manifest.py ./ci/
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY LICENSE-MIT LICENSE-APACHE ./
COPY src ./src
COPY README.md README.en.md SECURITY.md SECURITY.en.md CONTRIBUTING.md CONTRIBUTING.en.md ./
COPY docs ./docs
COPY deploy ./deploy
COPY docker ./docker
COPY Dockerfile .dockerignore THIRD-PARTY.md THIRD-PARTY.en.md ./
COPY zcf ./zcf
COPY contracts ./contracts
COPY monitoring ./monitoring
RUN python3 ci/source-manifest.py --product . --output /build/input-manifest.json
ARG BUILD_JOBS=2
ARG TARGETARCH
# 缓存产物的时间可能晚于新输入源码；刷新本地源码时间，让 Cargo 重新编译当前内容，registry 依赖仍复用缓存。
RUN --mount=type=cache,id=nacket-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=nacket-target-${TARGETARCH},target=/build/nacket/target \
    find /build/nacket/src -type f -name '*.rs' -exec touch '{}' + \
    && cargo build --locked --release --features schema --bins -j ${BUILD_JOBS} \
    && mkdir /out \
    && cp target/release/nacket target/release/nacket-schema /out/ \
    && python3 ci/source-manifest.py --product . --output /build/input-manifest.json --binary-dir /out \
    && cp /build/input-manifest.json /out/build-manifest.json \
    && ldd /out/nacket \
    && cargo metadata --locked --features schema --format-version 1 > /out/dependencies.json \
    && python3 ci/license-inventory.py /out/dependencies.json /out/licenses

FROM debian:bookworm-slim@sha256:abd67ffcfa541b485a3dff59865ab629aa048a6c613e639d36e7456b0b229241
ARG APP_VERSION=1.0.0
ARG SOURCE_REVISION=unknown
LABEL org.opencontainers.image.title="nacket" \
    org.opencontainers.image.description="Persistent connections, dynamic routes and trusted Redis/Kafka delivery with ProtocolBytes" \
    org.opencontainers.image.source="https://github.com/nasa-runtime/nacket" \
    org.opencontainers.image.url="https://github.com/nasa-runtime/nacket" \
    org.opencontainers.image.licenses="MIT OR Apache-2.0" \
    org.opencontainers.image.version="${APP_VERSION}" \
    org.opencontainers.image.revision="${SOURCE_REVISION}"
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl libgcc-s1 libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 nacket \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin nacket \
    && mkdir -p /app/data /app/zcf /usr/local/logs/nacket \
    && chown 10001:10001 /app/data /usr/local/logs/nacket
WORKDIR /app
COPY --from=builder /out/nacket /out/nacket-schema /usr/local/bin/
COPY --from=builder /out/dependencies.json /usr/share/nacket/dependencies.json
COPY --from=builder /out/build-manifest.json /usr/share/nacket/build-manifest.json
COPY --from=builder /out/licenses /usr/share/licenses/nacket/dependencies
COPY zcf/application.yml ./zcf/application.yml
COPY contracts /usr/share/nacket/contracts
COPY monitoring /usr/share/nacket/monitoring
COPY monitoring /usr/share/doc/nacket/monitoring
COPY README.md README.en.md SECURITY.md SECURITY.en.md CONTRIBUTING.md CONTRIBUTING.en.md /usr/share/doc/nacket/
COPY docs /usr/share/doc/nacket/docs
COPY deploy /usr/share/doc/nacket/deploy
COPY docker /usr/share/doc/nacket/docker
COPY contracts /usr/share/doc/nacket/contracts
COPY LICENSE-MIT LICENSE-APACHE THIRD-PARTY.md THIRD-PARTY.en.md /usr/share/doc/nacket/
COPY LICENSE-MIT LICENSE-APACHE THIRD-PARTY.md THIRD-PARTY.en.md /usr/share/licenses/nacket/
USER 10001:10001
EXPOSE 3030 3031 9464
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=15s --timeout=3s --start-period=60s --retries=3 \
    CMD curl --fail --silent --max-time 2 http://127.0.0.1:3031/readyz > /dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/nacket"]
