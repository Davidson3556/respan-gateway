# Providers

`respan-gateway-providers` is what the gateway knows of each vendor, one
file each under [`src/`](src): where it is, how it takes a key, the models
routed to it, and the formats it speaks, each a value set with the vendor's
quirks. `ALL` lists them; the binary's `{ vendor: <id> }` is one.

Formats name no vendor. A format offers knobs (`ChatQuirks` for OpenAI Chat
Completions, `SessionQuirks` for JSON events over WebSocket), a vendor's file
sets them, and its `quirks` say what each is for: `Provider::knobs()` must
name the same knobs (a test checks), so the table below, the admin UI and
`respan-gateway check` show what the code does. Each quirk has a test of its
own, named for it, in [`tests/<vendor>.rs`](tests).

A vendor is one file and one line in `ALL`. A format any vendor speaks is
reached, for a provider of no vendor too, as the first vendor in `ALL` that
speaks it says (its key header, and where it sits beside the base URL).

<!-- table -->
| Vendor | Formats | Quirks |
|---|---|---|
| `openai` | `openai-chat`, `openai-realtime`, `responses-ws`, `openai-images`, `openai-transcriptions`, `openai-speech`, `openai-videos`, `openai-embeddings` | `protocol`: Realtime speaks the subprotocol `realtime`, which its browser clients offer beside their key; a browser that offered one refuses a `101` that names none. |
| `anthropic` | `anthropic-messages` |  |
| `gemini` | `openai-chat`, `gemini-live`, `openai-images`, `gemini-veo`, `openai-embeddings` | `binary_text`: Live sends each JSON event in a binary frame, as Google's SDK reads frames as bytes.<br>`model_id_prefix`: Its OpenAI-compatible model list names each model `models/<id>`. |
| `groq` | `groq-chat`, `openai-transcriptions`, `openai-speech` | `usage_under`: A stream reports usage under `x_groq`, beside the response; the object's other fields stay native. |
| `deepseek` | `deepseek-chat` | `reasoning_content`: Reasoning is `reasoning_content`, in a request's messages and a response's message or delta. |
| `openrouter` | `openrouter-chat` | `inert_choice`: A choice's `native_finish_reason`, the upstream provider's own reason, describes its finish. |
| `xai` | `openai-chat`, `openai-images` |  |
| `mistral` | `openai-chat`, `openai-embeddings` |  |
| `together` | `openai-chat`, `openai-images`, `openai-embeddings` |  |
| `fireworks` | `openai-chat`, `openai-embeddings` |  |
| `ollama` | `openai-chat`, `openai-embeddings` |  |
| `azure` | `azure-chat` | `inert_choice`: A choice's `content_filter_results` and `content_filter_offsets` describe the moderation it passed. |
| `vllm` | `vllm-chat` | `reasoning_content`: Reasoning is `reasoning_content`, in a request's messages and a response's message or delta.<br>`inert_choice`: A choice's `stop_reason`, the stop token or string that ended it, describes its finish. |
<!-- /table -->

The table is written from `ALL`: `BLESS=1 cargo test -p respan-gateway-providers --test registry`.
