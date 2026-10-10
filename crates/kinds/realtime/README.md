# Realtime events

`respan-gateway-realtime` is the realtime kind: `Input` in (client to
provider) and `Output` out (provider to client), each one JSON event. Its
format, a WebSocket session, is
[`respan-gateway-realtime-ws`](../../formats/realtime-ws/README.md)'s.

Each direction contains `event: NativeValue`, with a mandatory wire format ID
and JSON value. `json()` reads the payload; `json_mut()` edits it using copy on
write. The format rejects mismatched format IDs, so moving an event between
vendor protocols requires an explicit translation.

The crate is the kind's types alone, depending only on
`respan-gateway-core` and `serde_json`.
