# Media kinds

`respan-gateway-media` is the media kinds and their formats over HTTP. Each
operation is a kind of its own, an `Input` in and an `Output` out, in a
module of its name:

| Module | Kind | `Input` | `Output` events |
| --- | --- | --- | --- |
| `image` | Image generation and edits | `prompt`, source `images` and `mask` (an edit has images), `count`, `size`, `quality`, `output_format` | `Start`, `Partial` previews, `Image`, `Usage` |
| `transcription` | Speech to text, or to English (`Task`) | `audio`, `task`, `language`, `prompt`, `temperature` | `Delta`, `Transcript`, `Rendered`, `Usage`, `Seconds` |
| `speech` | Text to speech | `text`, `voice`, `instructions`, `speed`, `audio_format` | `Audio` chunks, `Usage` |
| `embedding` | Embeddings of texts | `items` (`Text` or `Tokens`), `dimensions`, `user` | `Start`, `Embedding` vectors, `Usage` |
| `video` | Video generation, a job | an operation: `Create` (`model`, `prompt`, `seconds`, `size`, a `reference` image), `Remix`, `Retrieve`, `Content` (a `Variant`), `Delete`, `List` | `Job`, `Jobs`, `Content` chunks, `Deleted` |

Every `Input` has an optional `model`, which a router reads:
`ModelRouter::new(|i: &image::Input| i.model.as_deref())`; a video's is
`Input::model()`, as only a `Create` names one. Media are the
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

The formats are behind the default `openai` and `gemini` features. With
`default-features = false` the crate is the kinds' types alone, depending
only on `respan-gateway-core`, the conversation kind's types, `bytes` and
`serde_json`.

## Video jobs

A video is a job: created, polled until done, then downloaded, each a
request of its own, hours apart. A job's `id` is the provider's, opaque to
the gateway: a client gives it back as a path segment, and a follow-up
(`Retrieve`, `Content`, `Remix`, `Delete`, read by `Input::job_id()`) must
reach the provider that made the job. Nothing in the request says which but
the id's spelling, so a gateway routes follow-ups by it: OpenAI's ids start
`video_`, Veo's `veo_`, and one router serves both,
`ModelRouter::new(|i: &video::Input| i.model().or(i.job_id()))` with routes
for `sora`, `video_` and `veo`. Two deployments of one vendor share a
spelling: a gateway that has them keeps which made each job, or wraps the
ids in its own, signed, naming the deployment, within the vendor's length
limits (use case 8), and unwraps them before the call.

Some providers serve a finished job's content only at the URLs the job
names (`Job::videos`), as Veo does: `video::LocateContent`, given those
providers' names and registered after the router, retrieves the job before
a `Content` request goes on, and sets its `url`. Content not ready is
refused, 400 `video_not_ready`.

## Formats over HTTP

`openai::{OpenAiImages, OpenAiTranscriptions, OpenAiSpeech, OpenAiVideos,
OpenAiEmbeddings}`, named `openai-images`, `openai-transcriptions`,
`openai-speech`, `openai-videos` and `openai-embeddings`: OpenAI's endpoints, as OpenAI and compatible vendors serve
them. A client's request is a `POST` to a path ending in the endpoint
(`/v1/images/generations` and `/openai/v1/images/generations` alike), or a
video job's by its method and path; a provider's base URL includes the
version, as OpenAI's SDKs take it: `https://api.openai.com/v1`,
`https://api.groq.com/openai/v1`,
`https://generativelanguage.googleapis.com/v1beta/openai`.

| Format | Endpoints | Request | Response |
| --- | --- | --- | --- |
| `openai-images` | `/images/generations`, `/images/edits` | JSON; an edit a form (`image` or `image[]`, `mask`), or JSON naming its images by `image_url` | JSON; with `stream`, `image_generation.*` or `image_edit.*` events |
| `openai-transcriptions` | `/audio/transcriptions`, `/audio/translations` | A form (`file`) | JSON (`json`, `verbose_json`, `diarized_json`) or text (`text`, `srt`, `vtt`); with `stream`, `transcript.text.*` events |
| `openai-speech` | `/audio/speech` | JSON | The audio's bytes; with `stream_format: "sse"`, `speech.audio.*` events |
| `openai-embeddings` | `/embeddings` | JSON; `input` a text, texts, token ids or lists of them | JSON; vectors as `encoding_format` asks, `float` or `base64`, whatever the provider answers |
| `openai-videos` | `POST /videos`, `GET /videos`, `GET` or `DELETE /videos/{id}`, `GET /videos/{id}/content`, `POST /videos/{id}/remix` | JSON, or a form with an `input_reference` file | A job, a list or a deletion in JSON; content as its bytes |

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
  than the client takes it, as the content type its `response_format` names;
  a video's content likewise, as its variant's: `video/mp4`, `image/webp` for
  a thumbnail, `image/jpeg` for a spritesheet.
- A video's `seconds` is a string in OpenAI's API, as it is written; a
  client may send a number. Its `/videos/{id}` never takes
  `/videos/{id}/content` or `/remix`; an id is escaped in a call's path.
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

`gemini::GeminiVeo`, named `gemini-veo`, behind the default `gemini`
feature: Google's Veo as the Gemini API serves it, a provider format only,
at `https://generativelanguage.googleapis.com/v1beta` with the key in
`x-goog-api-key`.

- A `Create` is `POST /models/{model}:predictLongRunning`, which needs a
  model: its `size` an `aspectRatio` (16:9 or 9:16, the only ones Veo makes,
  else 400) and a `resolution` (`1080p` from 1080 pixels on the short side,
  else `720p`), its `seconds` `durationSeconds` (Veo takes 4, 6 or 8, and
  refuses others itself), its reference image the instance's base64 `image`,
  and its `gemini-veo` native fields (`negativePrompt`, `personGeneration`)
  parameters.
- A job is Veo's long-running operation, its id `veo_` and the operation's
  name, `models/{model}/operations/{op}`, in unpadded base64url
  (`GeminiVeo::job_id`, `GeminiVeo::operation`), as a path segment holds
  it; another id is not found, 404. An operation not done is `in_progress`,
  done with videos `completed`, done with an error, or with none as when
  Veo's filters took them all, `failed`.
- Content is a `GET` of the URL its job names at the provider's base, its
  path after the version, so the key goes to no other host. The Gemini API
  may redirect it to storage: the transport follows only if its client does
  (`HttpTransport::with_client` with a redirect policy), else the answer is
  a 502. Such a client sends its headers on, the key too, to the host it is
  redirected to.
- Veo cannot remix, delete or list videos, nor make a thumbnail or
  spritesheet: those are refused, 400 `unsupported`, before any call.
- No client speaks the format, so what Veo answers beyond the kind is
  dropped rather than kept native.

### Tests

The suite under `tests/` relays each kind through its format to strict
local fakes of OpenAI's media endpoints and of Veo (`tests/support/`), which
refuse what the real APIs refuse, so it needs the `openai` and `gemini`
features.
