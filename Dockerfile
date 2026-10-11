# Build the web UI, then the binary with it embedded, then a slim image.
# Stages: `source` (default, compiles here) and `prebuilt` (release.yml:
# `--target prebuilt` copies docker-bin/<amd64|arm64>/respan-gateway, built
# natively per architecture, so nothing compiles under emulation).
FROM node:22-slim AS ui
RUN corepack enable
WORKDIR /src/apps/ui
COPY apps/ui/package.json apps/ui/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY apps/ui ./
RUN pnpm build

FROM rust:1-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
COPY --from=ui /src/apps/ui/dist apps/ui/dist
RUN cargo build --release -p respan-gateway

FROM debian:stable-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 gateway
WORKDIR /home/gateway

# The image listens on every address (port $PORT, 8080 by default; RESPAN_LISTEN overrides), so the gateway
# refuses to start without a client key:
#   docker run -p 8080:8080 -e OPENAI_API_KEY -e RESPAN_CLIENT_KEYS=sk-gw-change-me IMAGE
# or `client_keys:` in a mounted config (-v $PWD/gateway.yaml:/home/gateway/gateway.yaml:ro).
# Provider keys come from the environment (-e OPENAI_API_KEY ...) or the config.
ENV PORT=8080
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
    CMD sh -c 'curl -fsS "http://127.0.0.1:${PORT:-8080}/healthz" >/dev/null || exit 1'
ENTRYPOINT ["respan-gateway"]

FROM runtime AS prebuilt
ARG TARGETARCH
COPY docker-bin/${TARGETARCH}/respan-gateway /usr/local/bin/respan-gateway
USER gateway

FROM runtime AS source
COPY --from=build /src/target/release/respan-gateway /usr/local/bin/respan-gateway
USER gateway
