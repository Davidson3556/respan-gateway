# Realtime events

`respan-gateway-realtime` is the realtime kind: a live session with a model,
`Input` in (client to provider) and `Output` out (provider to client), one
event each. Its formats, over WebSocket, are
[`respan-gateway-realtime-ws`](../../formats/realtime-ws/README.md)'s.

- `Input`: a `Session` configuration (its model, instructions, voice, audio
  formats, turn detection, transcription, tools), the user's `Audio`, a
  `Commit` of the user's turn, a whole `Message`, a `ToolResult`, `Respond`
  and `Cancel`.
- `Output`: the session `Opened` and `Updated`, `SpeechStarted` and
  `SpeechStopped` (barge-in), `InputTranscript`, then a response:
  `ResponseStart`, its `Audio`, `Text`, `Transcript` and `ToolCall`s,
  `Usage`, and `ResponseDone` with its `Status`; and `Error`.
- `Audio` is decoded bytes in an `AudioFormat` (16-bit PCM at a rate, or
  G.711 μ-law or A-law), never base64; `duration()` says how long it plays.

What the kind has no form for stays as its format has it: `NativeFields` on
an event, or a whole `Input::Native` / `Output::Native` event, sent only in
that format. Messages, tools and token usage are the conversation kind's.

The crate is the kind's types alone, depending on `respan-gateway-core`,
`respan-gateway-conversation`, `bytes` and `serde_json`.
