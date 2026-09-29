# 0013. No HTTP in the core

- Status: accepted, amended by [0017](0017-untyped-handle.md) (`handle` is untyped)
- Date: 2026-09-28

## Context

After 0012 the kernel read no HTTP but still carried `http` types: `Format`
and `Transport` took `http::Request`/`http::Response`, `Fault::status` was
a `StatusCode`, and the head hooks were typed as HTTP. A second wire
(stdio MCP, WebSocket realtime, an embedder's own) had to pretend to be
HTTP.

## Decision

1. **A wire is a pair of head types.** `Format` and `Transport` each carry
   `type Ask; type Answer;`. `Gateway::provider` requires
   `T::Ask = F::Ask` and `T::Answer = F::Answer`, so a mismatch does not
   build. Formats of one name share a wire, checked as they are given.
   `Gateway::handle` serves the formats, its caller typing the wire (0017).
2. **`Exchange<H>`**: `head`, `body` and `extensions`, which
   `Gateway::handle` takes and gives and a `Transport` sends and gets
   back. `Body` is a stream of `Result<Bytes, Fault>`, one item per data
   frame; a whole call is one frame.
3. **The encoder writes the head.** `Encoder<T, H>::head(first)` is asked
   once before any bytes; `first` is a call's first input when written
   from its value. Factories get the heads they may start from:
   `request_encoder(client, body, streams)`,
   `response_decoder(answer, call, sent)`,
   `response_encoder(answer, ask, body, refusal)`.
4. **`Extensions` are the core's own**, a clonable type map. The HTTP
   adapter takes an embedder's values from an `Extensions` it finds in the
   request's `http::Extensions`.
5. **HTTP lives in `crates/http`**: `Ask`/`Answer`
   (`http::request::Parts`, `http::response::Parts`), the adapter, the
   body, and conversions (`ask`, `answer`, `call`). `Fault::status` is a
   `u16` in HTTP's numbering; `Fault::new` takes `impl Into<u16>`.
6. **Head hooks are generic**: `edit_call::<H>`, `edit_response_head::<H>`
   and `relay::<H>` fail `INTERNAL` for a head of another wire.
7. stdio MCP stays on the HTTP wire for now.

## Consequences

- The core depends on no `http` crate. 16 public types (14 since 0015).
- HTTP formats and transports write
  `type Ask = respan_gateway_http::Ask; type Answer = respan_gateway_http::Answer;`.
- Body-only encoders are `Encoder<T, ()>`, given a head by
  `formats::with_head` or `formats::headed`.
- A wire-neutral plugin cannot edit heads. Whether a call was handed to
  its transport is `ProviderContext::is_sent`, which the ledger reads.
- Trailers never reach the kernel.

## History

- 0015: 14 public types; `ProviderContext::is_head_only` sits beside
  `is_sent`.
