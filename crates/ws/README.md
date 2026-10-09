# respan-gateway-ws

WebSocket sessions on the gateway, as neutral building blocks. A WebSocket is
an HTTP Upgrade exchange on the HTTP wire: the call's head is the upgrade
request, the answer's head is the `101`, and the two bodies are streams of
frames, **one body item per text frame**. Nothing here reads a provider's
events; OpenAI Realtime, Responses over WebSocket and Gemini Live run
through the same code, each a preset naming only its endpoint, subprotocol
and framing.

The `Input` / `Output` pair lives in
[`respan-gateway-realtime`](../realtime/README.md). Import models from
that crate and codecs/transports from this one.

## API

- `WsTransport::new(base_url)`: an outbound native HTTP sender to a `ws://` or
  `wss://` provider. It prepares a call as `HttpClient` does
  (`respan_gateway_http::prepare_call`), so a client's key passes only where
  a plugin put it back. `.with_headers(map)` sets the transport's own keys,
  `.with_limits(limits)` its `Limits`, `.with_handshake_timeout(d)` how long
  the provider has to answer the upgrade (default 10 s), and
  `.with_client(client)` a client in place of the shared one.
- `.with_binary_text(true)` accepts a provider's binary messages containing
  UTF-8 text, such as Gemini Live's JSON. These become text messages toward
  the client; invalid UTF-8 is a fault. Disabled by default.
- `JsonEvents::{OPENAI_REALTIME, RESPONSES_WS, GEMINI_LIVE}`: the vendors'
  presets, named `openai-realtime`, `responses-ws` and `gemini-live`, each
  setting documented with the vendor's reason. Each is served at, and sends
  to, the path its vendor's SDK adds to its base URL (`/realtime` and
  `/responses` after `wss://api.openai.com/v1`; Gemini's
  `/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent`
  after `wss://generativelanguage.googleapis.com`). OpenAI Realtime speaks
  the subprotocol `realtime` its browser clients offer.
  `format.transport(base_url)` is a `WsTransport` to its provider, taking
  binary text for Gemini Live, whose server sends JSON in binary frames.
  The key is the transport's (`with_headers`: OpenAI's
  `Authorization: Bearer`, Gemini's `x-goog-api-key`) or the client's.
- `JsonEvents::new(name, path)`: incoming and outgoing `Format` bindings
  whose kind is one JSON event per frame, `Input` in and
  `Output` out (each a format-tagged `NativeValue`, read with `json()`). Every event is written from
  its JSON, so a relay to a format of the same name keeps what an event says,
  not its exact bytes. Plugins filter events with
  `cx.filter(|e: &mut Output| ..)` (client edge) or `px.filter(..)`
  (provider edge), and drop one with a `Filter` that does not push it.
  `.protocol(name)` makes it speak a subprotocol: offered to the provider,
  and given back to a client that offered it. A handshake it refuses is
  answered with the fault's error code, never its message.
- `CloseCode(u16)`: a fact on a `Fault` that picks the code the client is
  closed with, in place of 1011. A code no Close may carry (1004 to 1006,
  1015, outside 1000 to 4999) is sent as 1011 (`CloseCode::sendable`).
- `ProviderClose(u16)`: the code a provider closed with, other than 1000, on
  the `upstream` fault the transport ends the answer with. The client is told
  it only as a `CloseCode`, which the transport adds on a call relayed with
  `RawErrors` (`relay::Relay`), or a plugin sets.
- `Limits { max_message, close_wait, close_send, ping_after }`: what an edge
  takes, with defaults and their reasons on the type: 16 MiB messages, 5 s
  for the other side's part of a close, 1 s for a Close to leave, a ping
  after 30 s of quiet.
- `serve(gateway, head, upgraded, limits, stop)`: a client's upgrade through
  the gateway, for an adapter that hosts sessions; `respan-gateway-axum`'s
  `router_with_ws` is one, behind its optional `ws` feature. `serve`
  documents how each end of a session reaches the client and the provider.

Both edges pump a session with the same code. Any other format on the HTTP
wire works with the transport and the adapter: the tests relay through
`formats::opaque` too.

## The transport

- The call's body ends clean: the provider is sent Close 1000, and its end,
  however it comes within `close_wait`, ends the answer clean.
- The call's body ends `Err`: the provider is sent Close (1001 for a client
  that left, else the fault's `CloseCode` or 1011) within `close_send`, and
  the answer ends with the fault. The answer's body dropped: Close 1001.
- The provider's Close 1000 (or no code) ends the answer clean; another code
  is an `upstream` fault with its `ProviderClose`. Its reason text is not
  carried.
- The provider dropping with no Close, or silent through a ping, is
  `truncated`; a rejected binary frame or a message over `max_message` is
  `upstream`, and the provider is sent Close 1003 or 1009.
- A provider that cannot be reached, or does not answer the upgrade within
  the handshake timeout (a refusal's body included), is `upstream`. A refusal
  of the upgrade is an answer with its body, which the format reads whole,
  up to `max_message`, as any error answer; a body that breaks off or is
  malformed is `truncated`.

## Limits

- **Text messages by default.** A provider's binary frame is a fault unless
  `with_binary_text(true)` enables UTF-8 normalization, as
  `JsonEvents::GEMINI_LIVE.transport(..)` does for Gemini Live's JSON in
  binary frames. A client's binary frame is `bad_request`, closed 1003.
  Ping and pong are answered by each hop's library and never reach a
  plugin.
- **HTTP/1.1 upgrades only** (no WebSockets over HTTP/2), no
  `permessage-deflate` (the extension header is not passed on or negotiated).
- **Backpressure holds.** A frame moves from a socket to the next only as the
  next side takes it: one frame in flight per hop, plus the sockets' own
  buffers. A client that does not read stalls the provider after about 6 MiB,
  and a provider that does not read stalls the client after about 7 MiB
  (`crates/axum/tests/backpressure.rs`).
- **Keep-alive, no idle limit**: a peer quiet for `ping_after` while read is
  pinged, and quiet that long again is gone, on both edges. A quiet session
  whose peers answer pings lasts; a limit on its length is a plugin's, and
  shutdown is the adapter's `stop`.
- **`wss` uses the system's root certificates** and the `aws-lc-rs` provider,
  named in the code because the workspace's dependencies enable both `ring`
  and `aws-lc-rs`, which makes rustls's own choice panic. Proxies are the
  environment's, as for `HttpClient`. The opt-in
  `validation/tests/live_ws.rs` checks TLS sessions against Gemini Live
  and OpenAI Realtime; each requires a valid key with model access.
- No reconnection or session resumption.

## Design notes

The kernel routes events; this integration binds owned `Incoming` and
`Outgoing` values to native `Request<Body>` and `Response<Body>`, so a frame
is a body item. `WsTransport::connect()` supplies the per-call provider
factory. Its `send` returns a separate completion future: response EOF alone
cannot establish outgoing delivery success. Incoming sessions explicitly
select live input; a streamed HTTP body alone does not turn a finite request
into a session. Where that shows:

- **Frame kinds.** A body item carries text bytes, so its original frame kind
  is lost. Opt-in binary UTF-8 normalization supports JSON protocols; raw
  binary audio would require a codec that carried each event's frame kind.
- **Close mapping.** A close is only a clean end, or an `Err` with typed facts
  (`ProviderClose`, `CloseCode`) on the fault. The client's close code and
  reason, and the provider's reason text, cannot cross.
- **The Upgrade dance.** A call keeps `Upgrade`, `prepare_call` gives it
  `Connection: upgrade`, and the transport leaves the handshake's own
  `Sec-WebSocket-Key`, `-Version` and `-Extensions` to the WebSocket library.
  The adapter answers with the `101`'s other headers, and its subprotocol if
  the client offered it and it is no key.

Used by: the [binary](../../apps/gateway/src/main.rs), and the
[realtime examples](../../examples/realtime).
