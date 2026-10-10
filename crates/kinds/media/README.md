# Media kinds

`respan-gateway-media` is the media kinds and their formats over HTTP. Each
operation is a kind of its own, an `Input` in and an `Output` out, in a
module of its name:

| Module | Kind | `Input` | `Output` events |
| --- | --- | --- | --- |
| `image` | Image generation and edits | `prompt`, source `images` and `mask` (an edit has images), `count`, `size`, `quality`, `output_format` | `Start`, `Partial` previews, `Image`, `Usage` |
| `transcription` | Speech to text, or to English (`Task`) | `audio`, `task`, `language`, `prompt`, `temperature` | `Delta`, `Transcript`, `Rendered`, `Usage`, `Seconds` |
| `speech` | Text to speech | `text`, `voice`, `instructions`, `speed`, `audio_format` | `Audio` chunks, `Usage` |

Every `Input` has an optional `model`, which a router reads:
`ModelRouter::new(|i: &image::Input| i.model.as_deref())`. Media are the
conversation kind's `Media` (a MIME type, bytes or a URL, a name) and token
usage its `Usage`; a kind has a type of its own only where neither says it:
a `Transcript` (`text`, `language`, `duration`, `segments`, `words`), and
`Seconds` of audio a provider bills, as Whisper does. Every `Output` also has
`Native`, an event the kind has no form for.

What a kind has no form for stays as the format that decoded it has it, in
optional `NativeFields` on a value or an `Output::Native`; only that format
writes it, and another refuses it. Values are plain data: they keep nothing
of the bytes they were decoded from, and a response is always written again
from its events. A stream and a whole body decode into the same events, so
a client gets the framing it asked for whatever the provider's was.

The formats are behind the default `openai` feature. With
`default-features = false` the crate is the kinds' types alone, depending
only on `respan-gateway-core`, the conversation kind's types, `bytes` and
`serde_json`. Video is to come as a `video` module of this crate.

## Formats over HTTP

`openai::{OpenAiImages, OpenAiTranscriptions, OpenAiSpeech}`, named
`openai-images`, `openai-transcriptions` and `openai-speech`: OpenAI's
endpoints, as OpenAI and compatible vendors serve them. A client's request
is a `POST` to a path ending in the endpoint (`/v1/images/generations` and
`/openai/v1/images/generations` alike); a provider's base URL includes the
version, as OpenAI's SDKs take it: `https://api.openai.com/v1`,
`https://api.groq.com/openai/v1`,
`https://generativelanguage.googleapis.com/v1beta/openai`.

| Format | Endpoints | Request | Response |
| --- | --- | --- | --- |
| `openai-images` | `/images/generations`, `/images/edits` | JSON; an edit a form (`image` or `image[]`, `mask`), or JSON naming its images by `image_url` | JSON; with `stream`, `image_generation.*` or `image_edit.*` events |
| `openai-transcriptions` | `/audio/transcriptions`, `/audio/translations` | A form (`file`) | JSON (`json`, `verbose_json`, `diarized_json`) or text (`text`, `srt`, `vtt`); with `stream`, `transcript.text.*` events |
| `openai-speech` | `/audio/speech` | JSON | The audio's bytes; with `stream_format: "sse"`, `speech.audio.*` events |

- A provider is asked to stream as the call's `StreamResponse` says, else
  as the client's request did, if to the same endpoint, else not. `stream`
  and `stream_format` are framing, not part of an `Input`.
- An edit goes to `/images/edits` as a form of its images' bytes, or in JSON
  if one is a URL. An image a client names by file id cannot be relayed and
  is refused.
- A file's type is its part's, unless that says none or
  `application/octet-stream`, which OpenAI refuses for images; then its
  name's, then its first bytes'.
- Text a provider rendered (`srt`, say) is kept as it came and given back as
  `text/plain`; a client that asked for text a provider gave as JSON gets
  the transcript's text, or SubRip or WebVTT of its segments.
- Audio is relayed chunk by chunk as it comes, the provider read no faster
  than the client takes it, as the content type its `response_format` names.
- Usage `{"type": "tokens"}` is `Usage`, `{"type": "duration"}` `Seconds`; a
  usage object's other fields, such as `input_token_details`, stay native.
- A stream that ends before its end event fails `truncated`.
- A client body without its required field (`prompt`, an edit's `image`,
  `file`, `input`) is refused, 400, before any call. A provider's error is
  read leniently, as compatible servers write it, and a client's is written
  in OpenAI's schema, `{"error": {"message", "type", "param", "code"}}`,
  under its status.

Vendors' fields the kind has no form for (Groq's `x_groq`, Together's
`steps`) stay native, so a relay in the same format keeps them.

### Tests

The suite under `tests/` relays each kind through its format to strict
local fakes of OpenAI's media endpoints (`tests/support/`), which refuse
what the real API refuses, so it needs the `openai` feature.
