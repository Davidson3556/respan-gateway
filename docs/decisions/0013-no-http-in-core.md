# 0013. No HTTP in the core

- Status: accepted
- Date: 2026-09-28

## Context

After 0012 the kernel read no HTTP but still carried `http` types as an
envelope: `Format` and `Transport` took `http::Request`/`http::Response`,
the body was an `http_body::Body`, `Fault::status` a `StatusCode`, and
`edit_call`, `edit_response_head` and `relay` were typed as HTTP. A second
wire (stdio MCP, WebSocket realtime, an embedder's own) had to pretend to
be HTTP. This is "Stage B" of `target/review/DESIGN-heads.md`, trimmed: no
`Wire` trait and no `Out` type. It amends 0003 to 0012.

## Decision

1. **A wire is a pair of head types.** `Format` and `Transport` each carry
   `type Ask; type Answer;`. `Gateway::provider` requires
   `T::Ask = F::Ask` and `T::Answer = F::Answer`, so a mismatch does not
   build. Formats of one name share a wire, checked as they are given.
   `Gateway::handle::<Ask, Answer>` serves the formats on that wire.
2. **One new type, `Exchange<H>`**: `head`, `body` and `extensions`, which
   `Gateway::handle` takes and gives and a `Transport` sends and gets
   back. `Body` is a `BoxStream` of `Result<Bytes, Fault>`, one item per
   data frame; a whole call is one frame.
3. **The encoder writes the head.** `Encoder<T, H>` has
   `head(&mut self, first: Option<&T>) -> H`, asked once before any
   bytes: `first` is a call's first input when written from its value.
   Factories get the heads they may start from: `request_encoder(client:
   Option<&Ask>, body: &mut Bytes, streams)`, `response_decoder(answer,
   call, sent)`, `response_encoder(answer: Option<&Answer>, ask, body,
   refusal)`. Whether a body exists stays the format's (`Encoder::done`).
4. **Extensions are the core's own**: a minimal clonable type map, since
   `http::Extensions` would keep the `http` dependency. The HTTP adapter
   takes an embedder's values from an `Extensions` it finds in the
   request's `http::Extensions`.
5. **HTTP lives in `crates/http`**: the `Ask`/`Answer` head types
   (`http::request::Parts`, `http::response::Parts`), the adapter, `Body`
   as an HTTP body, and conversions (`ask`, `answer`, `call`). The HTTP
   formats and `HttpTransport` use them. `Fault::status` is a `u16` in
   HTTP's numbering; `Fault::new` takes `impl Into<u16>`, so a
   `StatusCode` still works.
6. **Head hooks are generic**: `edit_call::<H>(|head, first| ..)`,
   `edit_response_head::<H>(|head| ..)` and `relay::<H>(Exchange<H>)` fail
   with `internal` for a head of another wire.
7. stdio MCP stays on the HTTP wire for now; a line wire of its own is
   possible without a kernel change.

## Consequences

- The core depends on no `http`, `http-body` or `http-body-util`. 16
  public types: 0012's 13, `Relayed`, `Exchange` and `Extensions`.
- Every format and transport names its wire; HTTP ones write
  `type Ask = respan_gateway_http::Ask; type Answer = respan_gateway_http::Answer;`.
- Body-only encoders are `Encoder<T, ()>`, given a head by
  `formats::with_head` or `formats::headed`.
- A wire-neutral plugin cannot edit heads; `edit_call` of HTTP's head on
  another wire fails the call. Whether a call was handed to its transport
  is a neutral fact, `ProviderContext::is_sent`, which the ledger reads.
- Trailers never reach the kernel.
