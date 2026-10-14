# Vendors

`respan-gateway-vendors` is what the gateway knows of each vendor, one
file each under [`src/`](src): where it is, how it takes a key, the formats
it speaks, and its quirks. `ALL` lists them; the binary's
`vendor = "<id>"` is one.

A vendor is a template (`Vendor`, from `respan-gateway-vendor-kit`). A
`VendorProvider` is one at a base URL with a key, ready to register, so a
provider builder writes none of it again:

```rust,ignore
use respan_gateway_vendors::openai;

// Its base URL, its key from `OPENAI_API_KEY`; `.base(..)` and `.key(..)`
// set others.
let openai = openai::VENDOR.provider("openai");
// A core provider of each format it speaks, each at its base URL, taking
// the key as OpenAI does, carrying its quirks.
let gateway = openai.register(Gateway::new())?;
// Its models, by its model list.
let models = openai.models().await;
```

A provider of no vendor is `VendorProvider::new(name).formats(..).base(..)`,
each format as `speaks(None, name)` gives it.

A format is a spec and names no vendor: it keeps what a vendor sends beyond
the spec native, where it appeared. A quirk is a vendor's deployment
departing from a spec it speaks, and its file handles it:

- as data: the header a key goes in (`Speaks::key_in`), where a format sits
  beside the base URL (`Speaks::at`), how it lists its models if not as
  OpenAI's `GET /models` does (`Vendor::models`, a fn of the provider,
  which calls with `client()`);
- as a plugin, `Provider::plugin`, such as `groq::GroqQuirks`, which turns
  what the vendor sends beyond the spec into the kind's events and back.
  Each core provider of that vendor carries it as its own plugin
  (`respan_gateway_core::Provider::plugin`), so it runs on their calls
  alone, after the gateway's plugins, and each of those sees the kind's
  events. It is part of the provider, not something to turn off.

What a spec itself says, such as OpenAI Realtime's subprotocol, is its
format's (`OpenAiRealtime`), not a quirk. Each quirk is
described in `quirks`, which the table below, the admin UI and
`respan-gateway check` show, and tested in [`tests/<vendor>.rs`](tests) by a
test named for it (a test checks).

A vendor is one file and one line in `ALL`. A provider of no vendor has no
quirks; it takes its key, and sits beside its base URL, as the format's own
vendor, the first in `ALL` that speaks it, does.

<!-- table -->
| Vendor | Formats | Quirks |
|---|---|---|
| `openai` | `openai-chat`, `openai-responses`, `openai-realtime`, `responses-ws`, `openai-images`, `openai-transcriptions`, `openai-speech`, `openai-embeddings` | `openai_transcription_model`: Realtime transcribes the user's audio only with a named model: a session that turns transcription on without one asks for `gpt-4o-mini-transcribe`. |
| `anthropic` | `anthropic-messages` |  |
| `gemini` | `gemini-generate`, `openai-chat`, `gemini-live`, `openai-images`, `gemini-veo`, `openai-embeddings` | `model_id_prefix`: Its model list names each model `models/<id>`.<br>`thought_signature`: A Gemini 3 model refuses a function call in a request's history that has no signature (`thoughtSignature`, or over Chat Completions the call's `extra_content`), as one from another format has none; it takes its documented placeholder in place of one.<br>`extra_content`: Over Chat Completions, a Gemini 3 reply carries its thought signature in the message's `extra_content`, which only Gemini reads back: a client of another format goes without it. |
| `groq` | `openai-chat`, `openai-transcriptions`, `openai-speech` | `x_groq_usage`: A response reports usage under `x_groq`, beside it, rather than in `usage`; the object's other fields stay native. |
| `deepseek` | `openai-chat` | `reasoning_content`: Reasoning is `reasoning_content`, in a request's messages and a response's message or delta. |
| `openrouter` | `openai-chat` | `inert_choice`: A choice's `native_finish_reason`, the upstream provider's own reason, describes its finish. |
| `xai` | `openai-chat`, `openai-images` |  |
| `mistral` | `openai-chat`, `openai-embeddings` |  |
| `together` | `openai-chat`, `openai-images`, `openai-embeddings` |  |
| `fireworks` | `openai-chat`, `openai-embeddings` |  |
| `ollama` | `openai-chat`, `openai-embeddings` |  |
| `azure` | `openai-chat` | `inert_choice`: A choice's `content_filter_results` and `content_filter_offsets` describe the moderation it passed. |
| `vllm` | `openai-chat` | `reasoning_content`: Reasoning is `reasoning_content`, in a request's messages and a response's message or delta.<br>`inert_choice`: A choice's `stop_reason`, the stop token or string that ended it, describes its finish. |
| `typesafe` | `typesafe-systemone` |  |
| `cloudflare` | `openai-chat`, `openai-embeddings` |  |
<!-- /table -->

The table is written from `ALL`: `BLESS=1 cargo test -p respan-gateway-vendors --test registry`.

## Base URLs

A provider's `base_url` is the one its own SDK takes. OpenAI-compatible ones
include the version: `https://api.openai.com/v1`,
`https://api.groq.com/openai/v1`, `https://openrouter.ai/api/v1`, and
Gemini's OpenAI-compatible endpoint
`https://generativelanguage.googleapis.com/v1beta/openai` (all
`openai-chat`). Anthropic's does not: `https://api.anthropic.com`. The media
formats (`openai-images`, `openai-transcriptions`, `openai-speech`,
`openai-videos`, `openai-embeddings`) and `openai-responses` take the same
base URL as `openai-chat`. Gemini's own formats, `gemini-generate`
(`generateContent` and `streamGenerateContent`, the model in the path) and
`gemini-veo`, take `https://generativelanguage.googleapis.com/v1beta` with
the key in `x-goog-api-key`. Session formats take a `wss://` URL as their
SDK does: `wss://api.openai.com/v1` for `openai-realtime` and
`responses-ws`, `wss://generativelanguage.googleapis.com` for `gemini-live`.

Without a config file, `RESPAN_<VENDOR>_BASE_URL` moves a vendor's base URL
(`OLLAMA_HOST` for `ollama`); with one, only the file's `base_url` counts
(`respan-gateway init` writes the moved URL into it). `azure` and `vllm`
need a `base_url`.
