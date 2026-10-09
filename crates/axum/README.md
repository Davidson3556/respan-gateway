# respan-gateway-axum

Axum hosting for a `Gateway`: each request goes through the HTTP adapter
([`respan_gateway_http::handle`](../http/README.md)), and formats decide
what it is.

- `router(gateway)`: every request, its body read whole up to 32 MiB
  (413 `too_large` past it). It switches no protocols.
- `router_with_limit(gateway, |head| bytes)`: the same, with a body limit
  chosen per request from its head.
- `router_with_ws(gateway, |head| bytes, limits, stop)`, behind the
  optional `ws` feature: the same, but a WebSocket upgrade is a session,
  pumped frame by frame through the gateway by
  [`respan-gateway-ws`](../ws/README.md)'s `serve`, which documents how each
  end reaches the client and the provider. `stop` closes every open session 1001, for a graceful shutdown.

Used by: the [binary](../../apps/gateway/src/main.rs) and the
[hosting examples](../../examples/hosting).
