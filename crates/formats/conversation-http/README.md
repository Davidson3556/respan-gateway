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

`OpenAiChat`, named `openai-chat`, is the spec and nothing else: it reads
OpenAI's fields, and keeps what else a provider sends native, where it
appeared, so a relay in this format sends it back and a plugin can read it
(the module docs give each shape): the response's own fields in the start's
`native`, or, when a later chunk changes them, as a native
`{"response": {..}}`; a choice's and its message's or delta's as a native
`{"choices": [..]}`; usage's own as a native `{"usage": {..}}`. A vendor's
departures from the spec, such as reasoning in `reasoning_content`, are its
plugins' in the [providers crate](../../providers/README.md), which read
these. Reasoning has no field in this format, and is refused.

A block is any number unique in a response, such as a plugin's own: both
formats' encoders write blocks in the order they open.

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
