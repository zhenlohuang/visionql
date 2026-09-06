ARG RUST_VERSION=1.91.1
ARG DEBIAN_SUITE=trixie

FROM rust:${RUST_VERSION}-${DEBIAN_SUITE} AS builder

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        cmake \
        libavcodec-dev \
        libavdevice-dev \
        libavfilter-dev \
        libavformat-dev \
        libavutil-dev \
        libclang-dev \
        libswresample-dev \
        libswscale-dev \
        perl \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/visionql
COPY . .

RUN cargo build \
        --locked \
        --release \
        --package vql-cli \
        --package vql-server

FROM debian:${DEBIAN_SUITE}-slim AS runtime

LABEL org.opencontainers.image.source="https://github.com/zhenlohuang/visionql" \
      org.opencontainers.image.licenses="Apache-2.0"

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates \
        curl \
        ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && useradd \
        --system \
        --uid 10001 \
        --home-dir /var/lib/visionql \
        --create-home \
        --shell /usr/sbin/nologin \
        visionql

COPY --from=builder /usr/src/visionql/target/release/vql /usr/local/bin/vql
COPY --from=builder /usr/src/visionql/target/release/vqld /usr/local/bin/vqld
COPY --from=builder /usr/src/visionql/LICENSE /usr/share/doc/visionql/LICENSE

ENV HOME=/var/lib/visionql \
    VQL_HOME=/var/lib/visionql

VOLUME ["/var/lib/visionql"]
EXPOSE 6031

USER visionql

STOPSIGNAL SIGINT
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD curl --fail --silent --show-error http://127.0.0.1:6032/health/ready || exit 1

CMD ["vqld"]
