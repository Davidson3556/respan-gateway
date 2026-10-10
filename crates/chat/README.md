# respan-gateway-chat

Chat formats over HTTP, of the [conversation](../conversation/README.md)
kind: JSON requests, JSON or SSE responses. Each is usable on either edge,
as a client format over `Incoming` and a provider format over `Outgoing`,
and they translate through the kind's events. Features `openai` and
`anthropic`, both on by default, choose the vendors.

## The `Codec` trait

What another such format implements. The HTTP binding around it, at both
edges, is the trait's provided methods.

- `Codec`: what a format supplies (its names, how it reads and writes a
  request, a part, a whole response, its stream encoder and its error body);
  its provided methods are the HTTP codec on both edges, which a format's
  `Format` implementations call (`bind_client`, `bind_provider`).
- `Reader`: a format's decoder of one response into events, driven over SSE,
  holding a response's start until its first content.
- `StreamResponse(bool)`: a call extension choosing whether the provider is
  asked to stream; without it the call follows the client's request.

The helpers both vendors share (messages and parts, the `native` fields a
value keeps for its own format, usage totals, the faults of what cannot be
written, and how a provider's error body reads, leniently, as compatible
servers send it) are private to this crate.

What a format has no form for stays native to it (`NativeFields`,
`Content::Native`, `Output::Native`), is written only by that format, and is
refused in translation where it could change the answer. Only an explicit
raw relay ([`Relay::RAW`](../http/src/relay.rs)) sends a client's own bytes.

## OpenAI Chat Completions

`openai::OpenAiChat`, usable on either edge: as a client format over `Incoming` and as a provider format over
`Outgoing`. A `POST` to a path ending in `/chat/completions`; its provider
base URL includes the version, such as `https://api.openai.com/v1`.

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
refuse their translation. Every dialect's values are
`openai-chat`'s, so what one keeps native another writes. A dialect writes
what it reads: serve `deepseek-chat` to give clients reasoning, which
`openai-chat` has no field for and refuses.

## Anthropic Messages

`anthropic::AnthropicMessages`, named `anthropic-messages`, usable on either edge: as a client format over
`Incoming` and as a provider format over `Outgoing`. A `POST` to a path
ending in `/v1/messages`; its provider base URL leaves the version out, such
as `https://api.anthropic.com`, and `anthropic-version: 2023-06-01` is sent
if the request names none.

Thinking is the kind's reasoning; its signature, and what else the kind has
no form for, stays native to this format.

## Tests

The suites under `tests/` drive the kernel's relay through these formats and
translate between them, so they need both features.
