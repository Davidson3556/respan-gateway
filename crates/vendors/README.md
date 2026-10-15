# Vendors

`respan-gateway-vendors` is what the gateway knows of each vendor: where
it is, how it takes a key, and the formats it speaks, as it speaks them.
`ALL` lists them; the binary's `vendor = "<id>"` is one. Each vendor is one
file in its tier's crate:

- [`green/`](green) (`respan-gateway-vendors-green`): maintained, and
  verified end to end against the vendor's live API by the validation
  package's live tests (`validation/tests/live_*.rs`). A test of the crate
  checks each green vendor has one, named `real_<id>_...`.
- [`yellow/`](yellow) (`respan-gateway-vendors-yellow`): maintained, and
  tested against fakes of its API, but not verified end to end. A yellow
  vendor becomes green with a live test of its own.

[`kit/`](kit) (`respan-gateway-vendor-kit`) is what they are built of and
share. This crate puts them together (`ALL`, `GREEN`, `YELLOW`, `find`),
each by its name (`respan_gateway_vendors::openai`).

A vendor is a template (`Vendor`, from the kit). A
`VendorProvider` is one with a key, each format at its base URL, ready to
register, so a provider builder writes none of it again:

```rust,ignore
use respan_gateway_vendors::openai;

// Each format at its base URL, its key from `OPENAI_API_KEY`; `.base(..)`,
// `.base_of(format, ..)` and `.key(..)` set others.
let openai = openai::VENDOR.provider("openai");
// A core provider of each format it speaks, each at its base URL, taking
// the key as the format does.
let gateway = openai.register(Gateway::new())?;
// Its models, by its model list.
let models = openai.models().await;
```

A provider of no vendor is `VendorProvider::new(name).formats(..).base(..)`,
each format as its spec has it (`FORMATS`, `format(None, name)`).

A format is a spec and names no vendor: it keeps what a vendor sends beyond
the spec native, where it appeared. A quirk is a vendor's deployment
departing from a spec it speaks, and its file handles it:

- as data: each format's base URL, as its docs give it, and how it lists
  its models if not as OpenAI's `GET /models` does (`Vendor::models`, a fn
  of the provider, which calls with `client()`);
- as its dialect of the spec's format, such as `groq::Groq`, a
  `Dialect` of Chat Completions
  (`respan_gateway_conversation_http::dialect`): what the vendor takes and
  sends beyond the spec, turned into the kind's events and back, inside the
  format, and the header it takes its key in if not the spec's. The vendor
  lists it as `(&Dialected::new(OpenAiChat, Groq), Some(BASE_URL))`, a
  format of the spec's name, so a client of the spec is still relayed to
  it, and no plugin of the gateway's sees anything but the kind's events;
- as a format of its own, for a vendor that speaks no spec.

What a spec says, and what its own vendor does, such as Anthropic's key in
`x-api-key`, OpenAI Realtime's transcription model or Gemini's thought
signatures in `generateContent`, is the spec's, in its format
(`AnthropicMessages`, `OpenAiRealtime`, `GeminiGenerate`), not a quirk. Each quirk
is described where its file handles it, and tested in
its crate's `tests/<vendor>.rs` by a test named for it.

A vendor is one file in its tier's crate, a line in that crate's `ALL`, and
one in this crate's `ALL` and module list. A provider of no vendor speaks
each spec as it is: it takes its key as the spec's format does, at the
base URL its config gives.

<!-- table -->
| Vendor | Tier | Formats |
|---|---|---|
| `openai` | green | `openai-chat`, `openai-responses`, `openai-realtime`, `responses-ws`, `openai-images`, `openai-transcriptions`, `openai-speech`, `openai-embeddings` |
| `anthropic` | green | `anthropic-messages` |
| `gemini` | green | `gemini-generate`, `openai-chat`, `gemini-live`, `openai-images`, `gemini-veo`, `openai-embeddings` |
| `groq` | yellow | `openai-chat`, `openai-transcriptions`, `openai-speech` |
| `deepseek` | yellow | `openai-chat` |
| `openrouter` | yellow | `openai-chat` |
| `xai` | yellow | `openai-chat`, `openai-images` |
| `mistral` | yellow | `openai-chat`, `openai-embeddings` |
| `together` | yellow | `openai-chat`, `openai-images`, `openai-embeddings` |
| `fireworks` | yellow | `openai-chat`, `openai-embeddings` |
| `ollama` | yellow | `openai-chat`, `openai-embeddings` |
| `azure` | yellow | `openai-chat` |
| `vllm` | yellow | `openai-chat` |
| `typesafe` | green | `typesafe-systemone` |
| `cloudflare` | yellow | `openai-chat`, `openai-embeddings` |
| `respan` | green | `openai-chat`, `openai-responses`, `anthropic-messages`, `gemini-generate` |
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

Respan's formats sit under one API, `https://api.respan.ai/api`:
`openai-chat` and `openai-responses` at it, `anthropic-messages` at
`/anthropic`, `gemini-generate` at `/google/gemini/v1beta` (its key in
`x-goog-api-key`, as Google's). Its models are its public catalog,
`https://www.respan.ai/api/gateway-models/list`, read a page at a time and
sent no key; a provider at another base URL lists at
`{base}/gateway-models/list`.

Without a config file, `RESPAN_<VENDOR>_BASE_URL` moves a vendor's base URL
(`OLLAMA_HOST` for `ollama`); with one, only the file's `base_url` counts
(`respan-gateway init` writes the moved URL into it). `azure` and `vllm`
need a `base_url`.
