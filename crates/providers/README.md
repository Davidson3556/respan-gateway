# Providers

`respan-gateway-providers` is what the gateway knows of each vendor, one
file each under [`src/`](src): where it is, how it takes a key, the models
routed to it, the formats it speaks, and its quirks. `ALL` lists them; the
binary's `{ vendor: <id> }` is one.

A format is a spec and names no vendor: it keeps what a vendor sends beyond
the spec native, where it appeared. A quirk is a vendor's deployment
departing from a spec it speaks, and its file handles it:

- as data: the header a key goes in (`Speaks::key_in`), where a format sits
  beside the base URL (`Speaks::at`), what its model list puts before an id
  (`model_id_prefix`);
- as a plugin, `Provider::plugin`, such as `groq::GroqQuirks`, which turns
  what the vendor sends beyond the spec into the kind's events and back.
  The binary runs it on the calls of providers of that vendor alone, after
  every other plugin's hooks, so each of theirs sees the kind's events.

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
| `openai` | `openai-chat`, `openai-responses`, `openai-realtime`, `responses-ws`, `openai-images`, `openai-transcriptions`, `openai-speech`, `openai-videos`, `openai-embeddings` | `openai_transcription_model`: Realtime transcribes the user's audio only with a named model: a session that turns transcription on without one asks for `gpt-4o-mini-transcribe`. |
| `anthropic` | `anthropic-messages` |  |
| `gemini` | `openai-chat`, `gemini-live`, `openai-images`, `gemini-veo`, `openai-embeddings` | `model_id_prefix`: Its OpenAI-compatible model list names each model `models/<id>`. |
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
<!-- /table -->

The table is written from `ALL`: `BLESS=1 cargo test -p respan-gateway-providers --test registry`.
