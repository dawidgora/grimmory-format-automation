# syntax=docker/dockerfile:1

FROM golang:1.23-bookworm AS development
ARG AIR_VERSION=v1.61.0
WORKDIR /workspace
COPY go.mod go.sum ./
RUN go mod download \
    && GOBIN=/usr/local/bin go install github.com/air-verse/air@${AIR_VERSION} \
    && apt-get update \
    && apt-get install -y --no-install-recommends calibre ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
# Keep Air builds from rewriting module metadata inside the bind-mounted source.
ENV GOFLAGS=-mod=readonly

FROM golang:1.23-bookworm AS build
WORKDIR /src
COPY go.mod ./
COPY . ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/converter ./cmd/server

# Keep the runtime tied to Debian's Calibre package rather than a GUI-oriented
# Calibre image. The Go binary remains statically linked; Debian is needed for
# ebook-convert and its native runtime dependencies.
FROM debian:bookworm-slim
ARG VERSION=dev
ARG VCS_REF=unknown
LABEL org.opencontainers.image.title="Grimmory Format Automation" \
      org.opencontainers.image.description="Authenticated Grimmory ebook format reconciliation service backed by Calibre" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.source="https://github.com/dawidgora/grimmory-format-automation" \
      org.opencontainers.image.licenses="MIT AND GPL-3.0-only"

RUN apt-get update \
    && apt-get install -y --no-install-recommends calibre ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 converter \
    && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin converter \
    && mkdir -p /data \
    && chown -R converter:converter /data

COPY --from=build /out/converter /usr/local/bin/converter
ENV DATA_DIR=/data \
    PORT=8080 \
    HOME=/home/converter
VOLUME ["/data"]
USER converter
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl --fail --silent http://127.0.0.1:8080/health || exit 1
ENTRYPOINT ["/usr/local/bin/converter"]
