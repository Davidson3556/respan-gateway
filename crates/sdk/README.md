# Plugin SDK

`respan-gateway-sdk` is the single plugin-author entry point. It reexports
the core contracts (`Plugin`, `ClientContext`, `ProviderContext`, `Filter`,
`Fault`, `EventStream`, `EventSink`, and the gateway/call/response APIs) as the
original types, not wrappers, and provides:

- the conversation kind: a `Conversation` in, `Reply` events out (one event of
  a reply each), `Assembled` folding them, and `Extra` for what a format has
  that the kind does not;
- `Usage`: integer amounts of named units, kind-neutral (the conversation
  kind's units are in `conversation::units`);
- fault facts: `BadRequest`, `Upstream`, `Truncated`, `RetryAfter`,
  `ErrorCode` and `Details`.

The SDK defines no `Format` implementations. `Conversation` / `Reply` is its
only request/event kind; `Usage` and fault facts are kind-neutral. There are no
shared image-generation, video-generation or live-session event types here.
The core accepts any distinct input/output types through `Format<T, R, W>`.
OpenAI Chat and Anthropic Messages formats live in `respan_gateway_providers`;
HTTP bindings live in `respan_gateway_http`. WebSocket JSON events
(`ClientEvent` / `ProviderEvent`) live in `respan_gateway_ws`, and MCP messages
(`ToServer` / `ToClient`) live in `respan_gateway_mcp`.

A `Fault` carries typed facts that each edge renders as it knows: `BadRequest`,
`Upstream` and `Truncated` (each with a `fault` constructor), `RetryAfter`,
`ErrorCode` and `Details`. Core adds only its own markers (`Abandoned`, `Internal`, `Config`,
`NotFound`).

A reply is a sequence of events, the same for a stream and a whole body, and
a `Reply` is one such event, not a whole reply: `Reply::Start`, then for each content block a `BlockStart`, its `Delta`s and a
`BlockEnd`, with `Usage` increments, a `Finish` and `Other` (an event the kind
has no form for, in its format's own JSON) among them. A `Delta` is a
`Delta::Text`, `Delta::Arguments` or `Delta::Reasoning`, of its block's own
kind, so a plugin need not track what a block holds. An encoder collects the
events into one body or writes them as a stream, so a plugin handles one shape;
`Assembled` folds events for both plugins and whole-body encoders. Its `items()`
keeps text, reasoning, tool blocks and opaque events together; `blocks()` reads
just the blocks, and `text()` joins only text blocks. Blocks occupy their opening
position, with deltas concatenated; opaque events retain their arrival order.
This is not the original event stream or an automatic conversion to conversation
history. Assembly checks block consistency; the reader checks stream completion.
`Reply::is_content` selects starts, block events and finishes; usage and opaque
events are excluded. Policies may instead select specific variants.

Tools are typed too: a request's `Tool`s and `ToolChoice`, an assistant's
`Content::ToolCall` and its `Content::ToolResult`, and a response's
`Block::ToolCall`, whose arguments stream as `Delta::Arguments` of JSON text.
`Message::text` exposes typed text, explicitly skipping tool calls and other
content; `Message::is_plain` checks for a known role, text parts and no message
extras. Text-part extras are allowed. Holdback, JSON-text rewriting and placeholders remain
private example support, not SDK promises.

What the kind has no form for stays as the format that decoded it has it, in an
`Extra` (a format name and its fields) on a `Conversation`, `Message`, `Tool`
or `Start`, or in a `Content::Other` or `Reply::Other`; only that format writes
it, and translating it is refused if it could change the answer.

Conversation values are plain data: they keep nothing of the bytes they were
decoded from, and compare by what they say. Only an explicit raw relay
(`respan_gateway_http::relay::Relay::RAW`) sends a request's own bytes, and a
response is always written again from its events.

The SDK depends only on core and `serde_json`.

The simplification changes the pre-alpha API: `Assembled.message`, `.reasoning`
and `.other` are replaced by `items()`, `blocks()` and `text()`. `Details` holds
a JSON object map, so every accepted value is a set of error fields.

Protocol authors use `respan_gateway_http::http` for codecs and transports
and `respan_gateway_http::formats` for JSON/SSE framing, error encoding and
opaque passthrough. Routing mechanisms live in `respan_gateway_plugins::routing`;
MCP and WebSockets are separate optional integrations. No old SDK facade
packages are retained.
