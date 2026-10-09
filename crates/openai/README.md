# respan-gateway-openai

The OpenAI Chat Completions [codec](../codec/README.md), usable on either
edge: as a client format over `Incoming` and as a provider format over
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

The suites of two formats, the kernel's relay driven through this one and
translation to and from Anthropic Messages, live here too, under `tests/`.
