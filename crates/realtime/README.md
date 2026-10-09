# Realtime events

`respan-gateway-realtime` owns the complete `Input` / `Output` input/output
pair: realtime JSON event payloads. Both directions stay in this crate.

This is a data-model crate. It depends on SDK native values and serde_json; it has no gateway
transport or codec dependency. Protocol implementations live in
[`respan-gateway-ws`](../ws/README.md).

Import the pair directly from `respan_gateway_realtime`.


Each direction contains `event: NativeValue`, with a mandatory wire format ID
and JSON value. `json()` reads the payload; `json_mut()` edits it using copy on
write. The WS codec rejects mismatched format IDs, so moving an event between
vendor protocols requires an explicit translation.
