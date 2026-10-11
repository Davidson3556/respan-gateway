# Build the web UI, then the binary with it embedded, then a slim image.
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

FROM debian:stable-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/respan-gateway /usr/local/bin/respan-gateway
EXPOSE 8080
# Reachable from outside the container: set `keys` in a mounted gateway.yaml
# (-v $PWD/gateway.yaml:/gateway.yaml), or publish the port on localhost only
# (-p 127.0.0.1:8080:8080).
WORKDIR /
ENTRYPOINT ["respan-gateway", "--listen", "0.0.0.0:8080"]
