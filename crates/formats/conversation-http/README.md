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

The helpers the formats share (messages and parts, the `native` fields a
value keeps for its own format, usage totals and the faults of what cannot be
written) are private to this crate. Each format names its own inert events
(`Schema::is_inert`): those another format drops in translation. How a provider's error body reads,
leniently, as compatible servers send it, is public, `read_provider_error`,
for the HTTP formats of other JSON kinds, such as the media kinds'.

What a format has no form for stays native to it (`NativeFields`,
`Content::Native`, `Output::Native`), is written only by that format, and is
refused in translation where it could change the answer. Only an explicit
raw relay ([`Relay::RAW`](../../wires/http/src/raw.rs)) sends a client's own bytes.

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

## OpenAI Responses

`responses::OpenAiResponses`, named `openai-responses`: a `POST` to a path
ending in `/responses`; its provider base URL includes the version, as
`openai-chat`'s does. It is plain HTTP, a `POST /v1/responses` answered whole
or, with `stream: true`, as named SSE events; Responses over WebSocket is
`responses-ws`, a format of its own kind.

`instructions` is a leading system message and `input` the messages after it;
an assistant's consecutive messages, function calls and reasoning items are one
assistant message, and consecutive function call outputs are one user message
of results. A response's output text, function calls and reasoning (its
summary) are the kind's blocks, and its status and `incomplete_details` the
finish. A stream is written as the documented sequence (`response.created`,
`response.in_progress`, each item's `output_item.added`, content part, delta and
`done` events, then `response.completed` or `response.incomplete` with the
response whole), one item at a time, with the ids, `output_index`,
`content_index` and `sequence_number` the kind has no place for made when
another provider answered.

What the kind has no form for stays native and is refused in translation:
`previous_response_id` and `conversation` (another provider cannot resolve
them), `store: false`, `include`, `truncation`, `reasoning` (effort and
summary), `metadata`, built-in tools (web search, file search, computer use,
code interpreter, MCP) and the items they make, refusals, and annotations. A
relay in this format sends them back. The ids and statuses of items, and a
function tool's default `strict`, are inert: any other format drops them, and
a tool another format sent is written with `strict: false`, as it is not the
Responses default. Stop sequences and a failed tool result have no form in a
request, and log probabilities are not carried. The module docs say it all.

## Google Gemini

`gemini::GeminiGenerate`, named `gemini-generate`: a `POST` to a path
`.../models/{model}:generateContent` or `:streamGenerateContent?alt=sse`
(the JSON-array stream, without `alt=sse`, is refused); its provider base URL
includes the version, such as `https://generativelanguage.googleapis.com/v1beta`,
with the key in `x-goog-api-key`.

The model is in the path, not the body: a client's request names it there
(with no `models/`), and a call to a provider writes it from the input's
model. Whether to stream is the method, not a field. `contents` are
messages (`model` is the assistant), `systemInstruction` a leading system
message, `thought` parts reasoning, `inlineData` and `fileData` an image,
audio or document by MIME type, and `functionCall` and `functionResponse` the
tool call and its result. Gemini names a result by its tool, not by an id:
a call without an `id` is given one of the format's own (`gemini_call_..`),
which its result finds by name and which Gemini never sees; writing a
request, a result finds its tool by the call's id. A result's `response`
object is its text as JSON; an object written stays one, other text goes
under `output` (`error` for a failure). The results of one turn are one
content. Tool declarations take `parameters` (OpenAPI types in upper case)
or `parametersJsonSchema`, and are written as the latter; `toolConfig` is the
tool choice (`ANY` with one allowed name is a named tool); `generationConfig`
gives the output limit, `temperature`, `topP`, stop sequences and structured
output (`responseMimeType` with a schema).

What the kind has no form for stays native and is refused in another format:
`safetySettings`, `cachedContent`, `topK`, `thinkingConfig` and other
`generationConfig` fields, tools Google runs (`googleSearch`), a `toolConfig`
beyond the choice, video parts. A part's `thoughtSignature` is the one native
field another format drops, as it only hints at the thinking behind the part
(Gemini 3 needs one on a function call in the history; the
[Gemini quirk](../../providers/README.md) supplies its documented placeholder).
Writing, a request for several candidates, one tool call at a time and an end
user's id are refused.

A response's first candidate is the events: consecutive text parts one text
block, a function call a tool block (its arguments whole), `STOP` a tool use
when calls were made. Reasons that filter (`SAFETY`, `RECITATION`, `BLOCKLIST`,
..) are filtered, a blocked prompt too; others (`MALFORMED_FUNCTION_CALL`,
`OTHER`, ..) stay native, and another format refuses them rather than call
the answer finished. Usage counts the thoughts in the output, and the cached
tokens in the input. A stream with no `finishReason` was cut. Errors are
`{"error": {"code", "message", "status"}}`.

## Tests

The suites under `tests/` drive the kernel's relay through these formats and
translate between them. A vendor's quirks are tested in the providers
crate.
