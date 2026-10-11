# Realtime over WebSocket

`respan-gateway-realtime-ws` is the [realtime kind](../../kinds/realtime/README.md)
over a WebSocket session ([`respan-gateway-ws`](../../wires/ws/README.md)),
a format per spec:

- `OpenAiRealtime`, `openai-realtime`: at `/realtime` after
  `wss://api.openai.com/v1`, in the subprotocol `realtime` its browser
  clients offer.
- `GeminiLive`, `gemini-live`: at
  `/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent`
  after `wss://generativelanguage.googleapis.com`. Its server sends JSON in
  binary frames, which its transport takes as text.
- `ResponsesWs`, `responses-ws`: OpenAI's Responses over WebSocket, at
  `/responses` after `wss://api.openai.com/v1`. It is not realtime: it
  streams Responses over a socket, and uses the kind's types only as native
  events.

Each is served at a path ending in its own, and sends to it;
`format.transport(base_url)` is a `WsTransport` to its provider, set as its
frames need. The key is the transport's (`with_headers`: OpenAI's
`Authorization: Bearer`, Gemini's `x-goog-api-key`) or the client's. A
format that speaks a subprotocol offers it to the provider beside the
client's own offers, and gives it back to a client that offered it. They
share their session's handshake, and differ in their spec and their codec.

For now each decodes every frame, one JSON event, to the kind's
`Input::Native` or `Output::Native`, tagged with its name, and writes only
its own native events: it translates nothing yet. Every event is written
from its JSON, so a relay to the same format keeps what an event says, not
its exact bytes. Plugins filter events with `cx.filter(|e: &mut Output| ..)`
(client edge) or `px.filter(..)` (provider edge), and drop one with a
`Filter` that does not push it. A handshake one refuses is answered with the
fault's error code, never its message.

Used by: the [binary](../../../apps/gateway/src/main.rs), and the
[realtime examples](../../../examples/realtime).
