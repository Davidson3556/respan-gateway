# 0013. No HTTP in the core

- Status: accepted, amended by [0019](0019-status-out-of-the-kernel.md) (`Fault` has no status) and [0020](0020-symmetric-edges.md) (a wire is a format's `Wire` and `Dialer`, not a pair of head types; `Exchange`, `Transport` and the codec traits live in `crates/http`)
- Date: 2026-09-28

## Context

After 0012 the kernel read no HTTP but still carried `http` types: `Format`
and `Transport` took `http::Request`/`http::Response`, a `Fault` held an
HTTP status, and the head hooks were typed as HTTP. A second wire
(stdio MCP, WebSocket realtime, an embedder's own) had to pretend to be
HTTP.

## Decision

1. **A wire is a format's `Wire` and `Dialer` types** (0020), which the core
   carries and never opens. `Gateway::handle<W>` takes the caller's wire;
   `Gateway::provider` takes `impl Into<F::Dialer>`, so a dialer of another
   kind does not build. Formats of one name share a kind, wire and dialer,
   checked as they are given. HTTP's wire is `http::Accepted` and its dialer
   `http::Dialer`, made of any `http::Transport`.
2. **`Exchange<H>` is HTTP's**, in `crates/http`: `head`, `body` (`Frames`, a
   stream of `Result<Bytes, Fault>`, one item per data frame; a whole call
   is one frame), `extensions` and the flags `relayed` and `streams`,
   which a `Transport` sends and gets back.
3. **The encoder writes the head.** `http::Encoder<T, H>::head(first)` is
   asked once before any bytes; `first` is a call's first input when
   written from its value. A `Codec`'s factories get the heads they may
   start from: `request_encoder(client, body, streams)`,
   `response_decoder(answer, call, sent)`,
   `response_encoder(answer, ask, body, refusal)`.
4. **`Extensions` are the core's own**, a clonable type map. The HTTP
   adapter takes an embedder's values from an `Extensions` it finds in the
   request's `http::Extensions`.
5. **HTTP lives in `crates/http`**: `Ask`/`Answer` (named `RequestHead`
   and `ResponseHead` since 0020; `http::request::Parts`,
   `http::response::Parts`), the adapter, the
   body, the codec traits, the driver and conversions (`ask`, `answer`,
   `call`). A fault's HTTP status is a hint defined there (0019).
6. **No plugin touches a head.** The head hooks `edit_call`,
   `edit_response_head` and `relay` were generic over the wire's head
   type; 0020 removed them.
7. stdio MCP stays on the HTTP wire for now.

## Consequences

- The core depends on no `http` crate.
- An HTTP format implements `Codec` and `impl_format!(Type)`; an HTTP
  transport implements `http::Transport`, with the heads
  `http::Ask` and `http::Answer`.
- Body-only encoders are `Encoder<T, ()>`, given a head by
  `formats::with_head` or `formats::headed`.
- A wire-neutral plugin cannot edit heads. `ProviderContext::is_sent`
  says the call was handed to its dialer.
- Trailers never reach the kernel.

## History

- 0013 first typed `Format` and `Transport` by `type Ask; type Answer;`, a
  pair of head types the core carried; 0017 made `handle` untyped over
  them, and 0020 replaced the pair with `Wire` and `Dialer`.
- 0015: `ProviderContext::is_refused` sits beside `is_sent`; `relayed`
  and `streams` are fields of `Exchange`.
