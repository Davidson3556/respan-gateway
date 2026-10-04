# respan-gateway-ws

WebSocket sessions on the gateway, as neutral building blocks. A WebSocket is
an HTTP Upgrade exchange on the HTTP wire: the call's head is the upgrade
request, the answer's head is the `101`, and the two bodies are streams of
frames, **one body item per text frame**. Nothing here knows a provider's
events; OpenAI Realtime and Gemini Live run through the same code.

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
- `JsonEvents::new(name, path)`: incoming and outgoing `Format` bindings
  whose kind is one JSON event per frame, `ClientEvent` in and
  `ProviderEvent` out (each has `json: Value`). Every event is written from
  its JSON, so a relay to a format of the same name keeps what an event says,
  not its exact bytes. Plugins filter events with
  `cx.filter(|e: &mut ProviderEvent| ..)` (client edge) or `px.filter(..)`
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
- `Limits { max_message, close_wait, close_send }`: what an edge takes, with
  defaults and their reasons on the type: 16 MiB messages, 5 s for the other
  side's part of a close, 1 s for a Close to leave.

Hosting a client's session is the `respan-gateway-axum` adapter's, behind its
optional `ws` feature (`respan_gateway_axum::ws::{router, serve_ws,
serve_ws_until}`); its module documents how each end of a session reaches the
client and the provider. Any other format on the HTTP wire works with the
transport and the adapter: the tests relay through `formats::opaque` too.

## The transport

- The call's body ends clean: the provider is sent Close 1000, and its end,
  however it comes within `close_wait`, ends the answer clean.
- The call's body ends `Err`: the provider is sent Close (1001 for a client
  that left, else the fault's `CloseCode` or 1011) within `close_send`, and
  the answer ends with the fault. The answer's body dropped: Close 1001.
- The provider's Close 1000 (or no code) ends the answer clean; another code
  is an `upstream` fault with its `ProviderClose`. Its reason text is not
  carried.
- The provider dropping with no Close is `truncated`; a rejected binary
  frame or a message over `max_message` is `upstream`.
- A provider that cannot be reached, or does not answer the upgrade within
  the handshake timeout (a refusal's body included), is `upstream`. A refusal
  of the upgrade is an answer with its body, which the format reads whole,
  up to `max_message`, as any error answer; a body that breaks off or is
  malformed is `truncated`.

## Limits

- **Text messages by default.** A provider's binary frame is a fault unless
  `with_binary_text(true)` enables UTF-8 normalization. Gemini Live sends
  JSON in binary frames; its example enables this. Client binary frames
  remain unsupported. Ping and pong are answered by each hop's library
  and never reach a plugin.
- **HTTP/1.1 upgrades only** (no WebSockets over HTTP/2), no
  `permessage-deflate` (the extension header is not passed on or negotiated).
- **Backpressure holds.** A frame moves from a socket to the next only as the
  next side takes it: one frame in flight per hop, plus the sockets' own
  buffers. A client that does not read stalls the provider after about 6 MiB,
  and a provider that does not read stalls the client after about 7 MiB
  (`crates/axum/tests/backpressure.rs`).
- **No timeouts on a live session**: no idle limit, no keep-alive pings, no
  maximum length. A limit on long sessions is a plugin's, or `serve_ws_until`'s.
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

Used by: [realtime](../../examples/realtime/realtime.rs).
