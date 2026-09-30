# respan-gateway-ws

WebSocket sessions on the gateway, as neutral building blocks. A WebSocket is
an HTTP Upgrade exchange on the HTTP wire: the call's head is the upgrade
request, the answer's head is the `101`, and the two bodies are streams of
frames, **one body item per text frame**. Nothing here knows a provider's
events; OpenAI Realtime and Gemini Live run through the same code.

## API

- `WsTransport::new(base_url)`: an outbound `http::Transport` to a `ws://` or `wss://`
  provider. It strips and keys a call as `HttpTransport` does (`proxy_call`,
  `strip_client_origin_headers`, `credentials`, `CallHeaders`), so a client's
  key passes only on a same-dialect relay. `.with_headers(map)` sets the
  transport's own keys; `.max_message(bytes)` (default 16 MiB).
- `serve_ws(&gateway, ws, req)` and `router(gateway)`: the axum adapter. It
  accepts the client's `WebSocketUpgrade`, calls `respan_gateway_http::handle(..,
  true)`, and pumps frames both ways. A request the gateway refuses (no format,
  a plugin, an unreachable provider, a provider that refuses the upgrade) is an
  ordinary HTTP answer to the handshake, before any upgrade. To mix sessions
  with plain HTTP, route the sessions to `serve_ws` on top of
  `respan_gateway_server::router`.
- `JsonEvents::new(name, path)`: a `Codec` (made a `Format` by `impl_format!`) whose kind is one JSON event per
  frame, `ClientEvent` in and `ProviderEvent` out (each has `json: Value`).
  A relay to a format of the same name is byte for byte while an event is
  unedited; an edited one is written from its JSON. Plugins filter events with
  `cx.filter(|e: &mut ProviderEvent| ..)` (client edge) or `px.filter(..)`
  (provider edge), and drop one with a `Filter` that does not push it.
- `CloseCode(u16)`: a hint on a `Fault` that picks the code the client is
  closed with.

Any other format on the HTTP wire works with the transport and the adapter: the
tests relay through `formats::opaque` too.

## How a session ends

| The end | Input (client to provider) | Output (provider to client) |
|---|---|---|
| Client sends Close | ends clean | provider is sent Close 1000; its end, however it comes within 5 s, is clean |
| Client drops, no Close | ends `Err(abandoned)` | dropped: request and calls end `abandoned` |
| Client sends a binary frame | ends `Err(abandoned)` | client is sent Close 1003; ends `abandoned` |
| Provider sends Close 1000 (or no code) | dropped | ends clean; client is sent Close 1000 |
| Provider sends Close with another code | dropped | `Err(upstream)` with that `CloseCode`; client is sent that code |
| Provider drops, no Close | dropped | `Err(truncated)`; client is sent 1011 `truncated` |
| Provider sends a binary frame or a message over the limit | dropped | `Err(upstream)`; client is sent 1011 |
| Provider is unreachable | (no session) | the client's handshake is answered 502, text/plain |
| A fault ends the response | cut, `abandoned` | client is sent 1011, or its `CloseCode`, with the fault's **code** as the reason, never its message |

A `CloseCode` that no Close may carry (1004 to 1006, 1015, outside 1000 to
4999) is sent as 1011. A provider's own close reason text is not relayed.

## Limits

- **Text frames only.** A binary frame is a fault (table above). Both OpenAI
  Realtime and Gemini Live use JSON text frames. Ping and pong are answered by
  each hop's library and never reach a plugin.
- **HTTP/1.1 upgrades only** (no WebSockets over HTTP/2), no
  `permessage-deflate` (the extension header is not passed on or negotiated).
- **Messages up to 16 MiB** (`MAX_MESSAGE`), each way.
- **Backpressure holds.** A frame moves from a socket to the next only as the
  next side takes it: one frame in flight per hop, plus the sockets' own
  buffers. A client that does not read stalls the provider after about 6 MiB,
  and a provider that does not read stalls the client after about 7 MiB
  (`tests/backpressure.rs`). The kernel buffers nothing beyond its one batch.
- **No timeouts on a live session**: no idle limit, no keep-alive pings, no
  maximum length. The bounds are 10 s to connect and 5 s (`CLOSE_WAIT`) for
  the other side's part of a close. A limit on long sessions is a plugin's.
- **`wss` uses the system's root certificates** and the `aws-lc-rs` provider,
  named in the code because the workspace's dependencies enable both `ring`
  and `aws-lc-rs`, which makes rustls's own choice panic. It is tested for
  building and failing cleanly, not against a real TLS server.
- A provider's refusal of the upgrade (a 401) is a complete answer: the
  provider edge reads its body before the gateway answers, and on a relay the
  client gets the provider's status and body as they came. `serve_ws` writes
  it whole and sized, not chunked.
- No reconnection or session resumption.

## Design notes

The kernel routes events; this SDK stays on the HTTP wire, a `Codec` and an
`http::Transport`, so a frame is a body item. Where that shows:

- **Frame kinds.** A body item is bytes, so its frame kind is lost: text only
  in v1, a binary frame is a fault. A codec that carried a kind on each event
  (text, binary) could pass audio.
- **Close mapping.** A close is only a clean end, or an `Err` with a typed
  `CloseCode` hint on the fault. The client's close code and reason, and the
  provider's reason text, cannot cross.
- **The Upgrade dance.** The adapter keeps `Upgrade`, `proxy_call` sets
  `Connection: upgrade`, the transport strips `Sec-WebSocket-*`, `serve_ws`
  copies back the subprotocol and the provider's other headers.
- **End of input versus end of session.** A client's Close ends the input
  clean, then the response is read to its end and dropped unsent, because a
  connection that is closing cannot send.

Used by: [realtime](../../examples/realtime.rs).
