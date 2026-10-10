# Conversation formats over HTTP

`respan-gateway-conversation-http` is the
[conversation kind](../../kinds/conversation/README.md) over HTTP.

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

The helpers both formats share (messages and parts, the `native` fields a
value keeps for its own format, usage totals and the faults of what cannot be
written) are private to this crate. Each format names its own inert events
(`Schema::is_inert`): those another format drops in translation. How a provider's error body reads,
leniently, as compatible servers send it, is public, `read_provider_error`,
for the HTTP formats of other JSON kinds, such as the media kinds'.

What a format has no form for stays native to it (`NativeFields`,
`Content::Native`, `Output::Native`), is written only by that format, and is
refused in translation where it could change the answer. Only an explicit
raw relay ([`Relay::RAW`](../../wires/http/src/relay.rs)) sends a client's own bytes.

## OpenAI Chat Completions

`openai::OpenAiChat`: a `POST` to a path ending in `/chat/completions`; its
provider base URL includes the version, such as `https://api.openai.com/v1`.

`OpenAiChat::OPENAI`, named `openai-chat`, reads only OpenAI's own fields.
Another vendor's field stays native, so it survives a relay in this format
and is refused in translation where it could change the answer. A vendor
whose fields the kind has a form for gets a dialect of its own name,
`OpenAiChat::new(name, ChatQuirks { .. })`, which reads and writes them.
`ChatQuirks` are the knobs, each off in `ChatQuirks::NONE`:

| Knob | What the dialect reads |
| --- | --- |
| `reasoning_content` | Reasoning as `reasoning_content` |
| `usage_under` | A response's usage under this object, beside it |
| `inert_choice` | These fields of a choice, which describe it and carry no content: dropped as read, rather than kept native and so refusing translation |

The vendors' dialects (`groq-chat`, `deepseek-chat`, `vllm-chat`,
`openrouter-chat`, `azure-chat`) are the
[providers crate](../../providers/README.md)'s. Every dialect's values are
`openai-chat`'s, so what one keeps native another writes. A dialect writes
what it reads: serve `deepseek-chat` to give clients reasoning, which
`openai-chat` has no field for and refuses.

## Anthropic Messages

`anthropic::AnthropicMessages`, named `anthropic-messages`: a `POST` to a
path ending in `/v1/messages`; its provider base URL leaves the version out,
such as `https://api.anthropic.com`, and `anthropic-version: 2023-06-01` is
sent if the request names none.

Thinking is the kind's reasoning; its signature, and what else the kind has
no form for, stays native to this format. A full context window
(`model_context_window_exceeded`) is a native finish whose fallback is the
token limit.

## Tests

The suites under `tests/` drive the kernel's relay through these formats and
translate between them. A vendor's quirks are tested in the providers
crate.
