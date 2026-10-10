# Realtime events

`respan-gateway-realtime` is the realtime kind: `Input` in (client to
provider) and `Output` out (provider to client), each one JSON event, and
its format on its wire, a WebSocket session
([`respan-gateway-ws`](../../wires/ws/README.md)).

Each direction contains `event: NativeValue`, with a mandatory wire format ID
and JSON value. `json()` reads the payload; `json_mut()` edits it using copy on
write. The format rejects mismatched format IDs, so moving an event between
vendor protocols requires an explicit translation.

The format is behind the default `ws` feature. With `default-features =
false` the crate is the kind's types alone, depending only on
`respan-gateway-core` and `serde_json`.

## The format

Nothing here reads a provider's events: OpenAI Realtime, Responses over
WebSocket and Gemini Live run through the same code, each a preset naming
only its endpoint, subprotocol and framing.

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
- `JsonEvents::new(name, path)`: client and provider `Format` bindings
  whose kind is one JSON event per frame, `Input` in and
  `Output` out (each a format-tagged `NativeValue`, read with `json()`). Every event is written from
  its JSON, so a relay to a format of the same name keeps what an event says,
  not its exact bytes. Plugins filter events with
  `cx.filter(|e: &mut Output| ..)` (client edge) or `px.filter(..)`
  (provider edge), and drop one with a `Filter` that does not push it.
  `.protocol(name)` makes it speak a subprotocol: offered to the provider,
  and given back to a client that offered it. A handshake it refuses is
  answered with the fault's error code, never its message.

Used by: the [binary](../../../apps/gateway/src/main.rs), and the
[realtime examples](../../../examples/realtime).
