# Conversation kind

`respan-gateway-conversation` defines the shared LLM request/event pair:
`Input` in, `Output` events out. It depends on `respan-gateway-sdk`
for general faults and usage, and on `serde_json` for JSON values.

```rust
use respan_gateway_conversation::{Input, Message, Output, Role};
use respan_gateway_sdk::{ClientContext, Fault, Plugin, Response, Usage};
```

The crate exports conversation types and helpers directly from its root;
usage unit names live in `respan_gateway_conversation::units`.
OpenAI Chat and Anthropic Messages formats live in `respan-gateway-providers`.
This crate defines no protocol or transport implementations.

A reply is a sequence of events, the same for a stream and a whole body, and
an `Output` is one such event, not a whole reply: `Output::Start`, then for each content block a `BlockStart`, its `Delta`s and a
`BlockEnd`, with `Usage` increments, a `Finish` and `Native` (an event the kind
has no form for, in its format's own JSON) among them. A `Delta` is a
`Delta::Text`, `Delta::Arguments` or `Delta::Reasoning`, of its block's own
kind, so a plugin need not track what a block holds. An encoder collects the
events into one body or writes them as a stream, so a plugin handles one shape;
`Assembled` folds events for both plugins and whole-body encoders. Its `items()`
keeps text, reasoning, tool blocks and native events together; `blocks()` reads
just the blocks, and `text()` joins only text blocks. Blocks occupy their opening
position, with deltas concatenated; native events retain their arrival order.
This is not the original event stream or an automatic conversion to conversation
history. Assembly checks block consistency; the reader checks stream completion.
`Output::is_content` selects starts, block events and finishes; usage and opaque
events are excluded. Policies may instead select specific variants.

Tools are typed too: a request's `Tool`s and `ToolChoice`, an assistant's
`Content::ToolCall` and its `Content::ToolResult`, and a response's
`Block::ToolCall`, whose arguments stream as `Delta::Arguments` of JSON text.
`Message::text` exposes typed text, explicitly skipping tool calls and other
content; `Message::is_plain` checks for a known role, text parts and no message
native fields. Text-part native fields are allowed. Holdback, JSON-text rewriting and placeholders remain
private example support, not SDK promises.

What the kind has no form for stays as the format that decoded it has it, in optional
`NativeFields` (a mandatory format ID and its fields) on an `Input`, `Message`, `Tool`
or `Start`, or in a `Content::Native(NativeValue)` or `Output::Native(NativeValue)`; only that format writes
it, and translating it is refused if it could change the answer.

Input values are plain data: they keep nothing of the bytes they were
decoded from, and compare by what they say. Only an explicit raw relay
(`respan_gateway_http::relay::Relay::RAW`) sends a request's own bytes, and a
response is always written again from its events.



`Input.options: GenerationOptions` groups the output token limit, sampling,
stop sequences, tool selection, parallel calls and structured-output requirements.
`Content` includes text, reasoning, tool calls/results, images, audio, documents,
and native values. A `Media` has a MIME type, URL or bytes, optional name, and
optional native fields. Codecs do not fetch URLs.

Transport preferences are separate: set
`respan_gateway_providers::StreamResponse(bool)` in call extensions to choose
provider HTTP streaming. Client response framing follows the client's request.
The HTTP binding retains original token-limit spelling on same-format OpenAI
calls; standalone semantic encoding uses `max_completion_tokens`.
Neither spelling nor streaming preference lives in `Input`.

`Finish` records why the model stopped. Delivery completes when the event stream
ends; usage and native events may arrive after `Finish`.
