# Conversation kind

`respan-gateway-conversation` is the conversation kind: the shared LLM
request/event pair (`Input` in, `Output` events out), and its formats over
HTTP, one per vendor ([below](#formats-over-http)). Plugins read any
conversation through the pair and never see which vendor it came from.

```rust
use respan_gateway_conversation::{Input, Message, Output, Role};
use respan_gateway_conversation::Usage;
use respan_gateway_core::{ClientContext, Fault, Plugin, Response};
```

The crate exports conversation types and helpers directly from its root.
The formats are behind features (`openai`, `anthropic`, both on by default,
each turning on `http`). With `default-features = false` the crate is the
kind's types alone, depending only on `respan-gateway-core` and
`serde_json`, for plugins that need no I/O.

`Usage` is the token counts a provider reports: input, output, and the
cached, cache-write and reasoning counts that are parts of them. A count
of 0 is one the provider did not report. A format refuses usage it cannot
write, such as cache writes in OpenAI Chat.

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
private example support, not public promises.

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
optional native fields. Formats do not fetch URLs.

Transport preferences are separate: set
`StreamResponse(bool)` in call extensions to choose
provider HTTP streaming. Client response framing follows the client's request.
The HTTP binding retains original token-limit spelling on same-format OpenAI
calls; standalone semantic encoding uses `max_completion_tokens`.
Neither spelling nor streaming preference lives in `Input`.

`Finish` records why the model stopped. Delivery completes when the event stream
ends; usage and native events may arrive after `Finish`.

## Formats over HTTP

The kind's wire is HTTP: JSON requests, JSON or SSE responses. Each vendor's
format is usable on either edge, as a client format over `ClientIo` and a
provider format over `ProviderIo`, and they translate through the kind's
events.

- `Schema`: what a vendor supplies, its names and its JSON both ways
  (`decode_json`/`encode_json`, `decode_part`/`encode_part`, `encode_whole`,
  its stream encoder and its error body). Its provided methods are the HTTP
  binding on both edges, which a format's `Format` implementations call
  (`bind_client`, `bind_provider`). `decode_*`/`encode_*` convert finite
  values; `read_*`/`write_*` bind lazy event streams to a body.
- `Decoder`: a format's decoder of one response into events, driven over
  SSE, holding a response's start until its first content.
- `StreamResponse(bool)`: a call extension choosing whether the provider is
  asked to stream; without it the call follows the client's request.

The helpers both vendors share (messages and parts, the `native` fields a
value keeps for its own format, usage totals and the faults of what cannot be
written) are private to this crate. How a provider's error body reads,
leniently, as compatible servers send it, is public, `read_provider_error`,
for the HTTP formats of other JSON kinds, such as the media kinds'.

What a format has no form for stays native to it (`NativeFields`,
`Content::Native`, `Output::Native`), is written only by that format, and is
refused in translation where it could change the answer. Only an explicit
raw relay ([`Relay::RAW`](../../wires/http/src/relay.rs)) sends a client's own bytes.

### OpenAI Chat Completions

`openai::OpenAiChat`: a `POST` to a path ending in `/chat/completions`; its
provider base URL includes the version, such as `https://api.openai.com/v1`.

`OpenAiChat::OPENAI`, named `openai-chat`, reads only OpenAI's own fields.
Another vendor's field stays native, so it survives a relay in this format
and is refused in translation where it could change the answer. A vendor
whose fields the kind has a form for gets a dialect of its own name, which
reads and writes them:

| Constant | Name | Its fields |
| --- | --- | --- |
| `GROQ` | `groq-chat` | A stream's usage under `x_groq` |
| `DEEPSEEK` | `deepseek-chat` | Reasoning as `reasoning_content` |
| `VLLM` | `vllm-chat` | Reasoning as `reasoning_content`; a choice's `stop_reason` |
| `OPENROUTER` | `openrouter-chat` | A choice's `native_finish_reason` |
| `AZURE` | `azure-chat` | A choice's `content_filter_results` and `content_filter_offsets` |

A choice's fields in the table describe it and carry no content: the
dialect drops them, as it reads them, rather than keep them native and so
refuse their translation. Every dialect's values are `openai-chat`'s, so
what one keeps native another writes. A dialect writes what it reads: serve
`deepseek-chat` to give clients reasoning, which `openai-chat` has no field
for and refuses.

### Anthropic Messages

`anthropic::AnthropicMessages`, named `anthropic-messages`: a `POST` to a
path ending in `/v1/messages`; its provider base URL leaves the version out,
such as `https://api.anthropic.com`, and `anthropic-version: 2023-06-01` is
sent if the request names none.

Thinking is the kind's reasoning; its signature, and what else the kind has
no form for, stays native to this format.

### Tests

The suites under `tests/` drive the kernel's relay through these formats and
translate between them, so they need both features.
