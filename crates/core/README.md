# respan-gateway-core

The neutral kernel. It runs each request through the registered plugins and
streams the response back, and knows nothing of HTTP, providers,
credentials or message kinds. Plugin authors use it through
[`respan-gateway-sdk`](../sdk/README.md), which re-exports these types as
they are.

- `Gateway`: built by value (`Gateway::new().provider(..)?.client(..)?
  .plugin(..).target(..)?`) and handed one transport at a time
  (`handle(io, extensions)`); `idle()` waits for every request and end hook.
- `Format<T, R, W>`: a protocol bound to an owned transport `T`; `bind`
  returns a `Connection`, the receive future of `R` and the `EventSink` of
  `W`. Registration decides the edge: a client format receives a kind's
  `Input` and sends its `Output`, a provider format the reverse.
- `Plugin`: `on_client` and `on_client_end` once per request,
  `on_provider` and `on_provider_end` once per call, in registration order.
- `Filter<T>`: edits a kind's inputs or outputs as they stream; its type
  says which. `ClientContext::wrap` is for work that awaits.
- `Call` / `Response`: a plugin's call to a provider (`cx.call`), and what
  a call or `Response::stream` gives back: streamed, read or held.
- `Fault`: the one error, with typed facts (`fault.with(..)`,
  `fault.get::<T>()`).

See [architecture](../../docs/ARCHITECTURE.md) for how the crates fit.
