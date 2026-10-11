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

`OpenAiRealtime` decodes each frame into the kind's events: a session's
configuration, audio (its bytes, in the session's format), messages, tools'
calls and answers, a response's start, content, usage and end, errors.
What the kind has no place for stays OpenAI's: an event's other fields in
its `native`, as the frame has them, and events of no form in the kind,
such as its items and content parts, as `Output::Native`. A field is taken
as the kind's only where it writes back as it came, so a relay to OpenAI
keeps what every event says, not its exact bytes (audio is base64 anew,
padded).

`GeminiLive` does the same for the Live API's messages: a client's `setup`
(first and once, its model without `models/`; a message of more than one
field closes the session 1007), `clientContent` turns, `realtimeInput`
audio and text, `activityEnd`, `toolResponse`; the server's
`setupComplete`, model turns of audio and text, transcripts, `toolCall`,
`interrupted`, `voiceActivity` (its `type` and `audioOffset`),
`turnComplete` and `usageMetadata` (each turn's own counts). A
message that says several things is several events: what it says beyond
them stays on the first its client writes, so it is written back as it
came, unless its events are written as several messages, such as a turn
of two parts. Messages and tools' answers are held until what answers
them, so `clientContent` and `toolResponse` are written whole.
`activityStart`, `audioStreamEnd`, `goAway`, `toolCallCancellation` and
`sessionResumptionUpdate` stay native.

`OpenAiRealtime` and `GeminiLive` write each other's events too: see
[Translation](#translation). `ResponsesWs` decodes every frame to the
kind's `Input::Native` or `Output::Native`, tagged with its name, and
writes no other format's.

Plugins filter events with `cx.filter(|e: &mut Output| ..)` (client edge)
or `px.filter(..)` (provider edge), and drop one with a `Filter` that does
not push it. A handshake one refuses is answered with the fault's error
code, never its message.

## Translation

The target format's writer converts audio its spec cannot carry: G.711 is
decoded for Gemini, which takes PCM at any rate it is told; Gemini's
clients' audio is resampled to OpenAI's 24 kHz; Gemini's 24 kHz audio is
encoded as the G.711 an OpenAI client asked for. A response from Gemini
reaches an OpenAI client with the items, content parts and ids OpenAI
would send, its usage in `response.done`.

What one spec has and the other has not is emulated, dropped or refused:

- To Gemini: the session is created before its setup; an update that
  changes nothing is acknowledged, one that changes anything closes the
  session 1008 (Gemini fixes its setup); `response.cancel` ends the
  response at once and drops the rest of its turn; with manual turns the
  user's audio opens an activity and its commit ends it. The usage of a
  response that ended in calls or an interruption, which Gemini reports
  after, at its turn's end, is in the next `response.done`.
  `conversation.item.truncate` is dropped, as Gemini keeps only what it
  sent; so are ids, tracing, noise reduction, transcription settings and
  VAD tuning. A message is taken only as the user's text answered at once;
  history, item edits, response overrides, a commit under server VAD, a
  tool choice other than `auto` and `create_response: false` are refused.
- To OpenAI: Gemini's client routes by `?model=` on its URL (OpenAI takes
  its model at connect), and its `activityStart` while the model speaks is
  `response.cancel` (one that reaches OpenAI as the response ends is refused
  there for want of a response, and that refusal is dropped, not a close);
  its tools' schemas name their types as JSON Schema
  does (`object` for `OBJECT`); a response that calls tools, or is
  interrupted, ends its turn with `turnComplete`, as the Live API's do;
  context compression, VAD sensitivities and
  `audioStreamEnd` are dropped; sampling, resumption, proactivity, Google
  Search, non-blocking functions and video are refused. An OpenAI `error`
  closes a Gemini client, which knows a refusal only as a close.

A refused input is `bad_request`, closing the client 1008; a refused
output `upstream`. Another format's events and fields a provider sends are
dropped at the client.

A session's first input arrives only once it is routed, so routing reads
its model from the handshake: `requested_model(&Wire)` is `?model=`,
OpenAI's and, by the gateway's convention, a Gemini client's. The model
routing chose goes in the call as `SessionModel`, which OpenAI's provider
dials (else the client's `?model=`; with neither the session is refused),
and into the opening `Session` by `realtime::set_model`, which Gemini's
setup names. Each provider writes its own query, never the client's.

Used by: the [binary](../../../apps/gateway/src/main.rs), and the
[realtime examples](../../../examples/realtime).
