# Plugin SDK

`respan-gateway-sdk` is the single plugin-author entry point. It reexports
core contracts (`Plugin`, `ClientContext`, `ProviderContext`, `Filter`, `Fault`,
`EventStream`, `EventSink`, and the gateway/call/response APIs) and owns shared
LLM vocabulary: `conversation::{Conversation, Reply, Message, …}`, `Usage` and
`Details`. The reexports are the original core types, not wrappers.

A `Fault` carries typed facts that each edge renders as it knows: `BadRequest`,
`Upstream` and `Truncated` (each with a `fault` constructor), `RetryAfter` and
`ErrorCode`. Core adds only its own markers (`Abandoned`, `Internal`, `Config`,
`NotFound`).

A response is a sequence of events, the same for a stream and a whole body:
`Reply::Start`, then for each content block a `BlockStart`, its `Delta`s and a
`BlockEnd`, with `Usage` increments, a `Finish` and `Other` (an event the kind
has no form for, in its format's own JSON) among them. An encoder collects the
events into one body or writes them as a stream, so a plugin handles one shape;
`Assembled` folds them into the whole answer for plugins that need it (a
judge, a cache). `Reply::is_content` is true of a start, a block event or a
finish, not of usage or `Other`.

Tools are typed too: a request's `Tool`s and `ToolChoice`, an assistant's
`Content::ToolCall` and its `Content::ToolResult`, and a response's
`Block::ToolCall`, whose arguments stream as `Delta`s of JSON text.
`Message::text` exposes typed text, explicitly skipping tool calls and other
content; `Message::is_plain` checks whether all content is known text and the
message has no `Extra`. Holdback, JSON-text rewriting and placeholders remain
private example support, not SDK promises.

What the kind has no form for stays as the format that decoded it has it, in an
`Extra` (a format name and its fields) on a `Conversation`, `Message`, `Tool`
or `Start`, or in a `Content::Other` or `Reply::Other`; only that format writes
it, and translating it is refused if it could change the answer.

Conversation values are plain data: they keep nothing of the bytes they were
decoded from, and compare by what they say. Only an explicit raw relay
(`respan_gateway_http::relay::Relay::RAW`) sends a request's own bytes, and a
response is always written again from its events. The `relayed` module, which
other kinds (MCP, WebSocket) still use, keeps a value with the bytes it came
from.

The SDK depends on core, `bytes` and `serde_json`, not HTTP.

Protocol authors use `respan_gateway_http::http` for codecs and transports
and `respan_gateway_http::formats` for JSON/SSE framing, error encoding and
opaque passthrough. Routing mechanisms live in `respan_gateway_plugins::routing`;
MCP and WebSockets are separate optional integrations. No old SDK facade
packages are retained.
