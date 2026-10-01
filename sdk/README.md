# Plugin SDK

`respan-gateway-sdk` is the single plugin-author entry point. It reexports
core contracts (`Plugin`, `ClientContext`, `ProviderContext`, `Filter`, `Fault`,
`EventStream`, `EventSink`, and the gateway/call/response APIs) and owns shared
LLM vocabulary: `conversation::{Conversation, Reply, Message, …}`, `Usage` and
`Details`. The reexports are the original core types, not wrappers.

Tools are typed too: a request's `Tool`s and `ToolChoice`, an assistant's
`Content::ToolCall` and its `Content::ToolResult`, and a response's
`Piece::ToolCall`, whose arguments stream as JSON text numbered by call.
`Message::text` and `Reply::texts`/`texts_mut` expose typed text, explicitly
skipping tool calls and other content; `Message::is_plain` checks whether all
content is known text. Holdback, JSON-text rewriting and placeholders remain
private example support, not SDK promises.

The SDK depends on core, `bytes` and `serde_json`, not HTTP. Decoded values still
retain their original bytes and snapshot through transport-neutral provenance
for byte-exact relay and fault attribution. That representation stays here to
avoid a cycle with the HTTP crate; it is not HTTP/provider-author machinery.

Protocol authors use `respan_gateway_http::http` for codecs and transports
and `respan_gateway_http::formats` for JSON/SSE framing, error encoding and
opaque passthrough. Routing mechanisms live in `respan_gateway_plugins::routing`;
MCP and WebSockets are separate optional integrations. No old SDK facade
packages are retained.
