# respan-gateway

An LLM gateway: one endpoint for OpenAI, Anthropic, Gemini and a dozen more
vendors, translating between their APIs. Point any official SDK at it and
name any model; the gateway routes by model, translates chat (text, tools,
images, reasoning), realtime voice, images, speech, transcription,
embeddings and video jobs, and relays everything else untouched. It is a
single binary with a web UI, a Python package, and a Rust library with a
small neutral core: routing, fallback, billing, guardrails and logging are
plugins built on one public API, compiled in (no dynamic loading).

> **Status: pre-alpha, release-candidate core.** The binary, the Python
> package and the four chat formats, the media kinds and both realtime
> formats work and are tested against strict fakes of each vendor; APIs
> around the core may still change. See the [roadmap](#roadmap).

## Install

- **Release binary**: each `v*` tag attaches `respan-gateway` (web UI built
  in) for Linux x86_64 and aarch64, macOS aarch64 and x86_64 (`.tar.gz`)
  and Windows x86_64 (`.zip`, with `respan-gateway.exe`) to its
  [GitHub release](https://github.com/respanai/respan-gateway/releases).
- **Docker**: `docker run -p 8080:8080 -e OPENAI_API_KEY -e RESPAN_CLIENT_KEYS=sk-gw-change-me ghcr.io/respanai/respan-gateway`
  (or `docker build -t respan-gateway .`). The image listens on every
  address, so it refuses to start without a client key.
- **Python**: `pip install respan-gateway` ([`python/`](python/README.md)).
- **Cargo**: `cargo install --git https://github.com/respanai/respan-gateway respan-gateway`,
  or from a checkout `cd apps/ui && pnpm install && pnpm build` then
  `cargo build --release -p respan-gateway`. Without the UI build the
  binary serves a page saying so; everything else works.

Release artifacts exist once a tag has been pushed and the registries are
set up ([CONTRIBUTING](CONTRIBUTING.md#releasing)); until then build from source.

## Quickstart

No config file is needed: every vendor whose key is in the environment
becomes a provider.

```bash
export OPENAI_API_KEY=sk-... ANTHROPIC_API_KEY=sk-ant-... GEMINI_API_KEY=...
respan-gateway        # prints where it listens, the providers, and a curl to try
```

Any client speaks any vendor; the model name picks the provider:

```bash
# OpenAI Chat to Claude (translated)
curl localhost:8080/v1/chat/completions \
  -d '{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hi"}]}'
# Anthropic Messages to GPT
curl localhost:8080/v1/messages \
  -d '{"model":"gpt-4o-mini","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}'
# OpenAI Responses; and `provider/model` picks a provider explicitly
curl localhost:8080/v1/responses -d '{"model":"groq/llama-3.3-70b-versatile","input":"hi"}'
```

SDKs only need a base URL:

```python
from openai import OpenAI
OpenAI(base_url="http://localhost:8080/v1", api_key="unused").chat.completions.create(
    model="gemini-2.5-flash", messages=[{"role": "user", "content": "hi"}])

from anthropic import Anthropic
Anthropic(base_url="http://localhost:8080", api_key="unused").messages.create(
    model="gpt-4o-mini", max_tokens=64, messages=[{"role": "user", "content": "hi"}])

from google import genai   # Gemini's native API, served at /v1beta/models/{model}:generateContent
genai.Client(api_key="unused", http_options={"base_url": "http://localhost:8080"}).models.generate_content(
    model="claude-sonnet-4-5", contents="hi")
```

(`api_key` is a client key when the gateway has `client_keys`.) Media work
the same, at OpenAI's endpoints, to whichever provider serves the model:

```bash
curl localhost:8080/v1/images/generations -d '{"model":"gpt-image-1","prompt":"a lighthouse"}'
curl localhost:8080/v1/audio/transcriptions -F file=@talk.mp3 -F model=whisper-1
curl localhost:8080/v1/embeddings -d '{"model":"text-embedding-3-small","input":"hello"}'
curl localhost:8080/v1/videos -d '{"model":"veo-3.0-generate-001","prompt":"a paper boat"}'
```

Other endpoints: `GET /v1/models` (every provider's models with their
`kind`; `/v1beta/models` in Gemini's shape), `GET /healthz`, and the web UI
at [localhost:8080/ui/](http://localhost:8080/ui/).

## The binary

```
respan-gateway [serve]          serve ./gateway.yaml, or the providers in the environment
respan-gateway init             write a commented gateway.yaml (-p groq enables a vendor;
                                --client-key generates a client key and prints it once)
respan-gateway check            what a config serves; exit 1 on a problem
respan-gateway check --online   ... and call each provider with its key
respan-gateway models           every provider's models, with their kind
respan-gateway proxy --anthropic|--openai
                                sit in front of Claude Code or Codex, relaying a vendor's
                                whole API with each client's own credentials
```

Settings come from flags, the environment, or a `.env` file in the current
directory (the environment wins). `respan-gateway --help` lists them:

| Variable | Meaning |
|---|---|
| `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY` (or `GOOGLE_API_KEY`), `GROQ_API_KEY`, ... | Provider keys; with no config file each is a provider |
| `RESPAN_CONFIG` | The config file, as `--config` |
| `RESPAN_LISTEN` | Where to listen, as `--listen` (`127.0.0.1:8080`, `localhost:8080`, `8080`) |
| `PORT` | The port, on every address (`0.0.0.0`), if neither `--listen`, `RESPAN_LISTEN` nor `listen:` says; for hosts that hand it out |
| `RESPAN_CLIENT_KEYS` | Client keys, comma separated, added to the config's `client_keys:` |
| `RESPAN_LOG` | `text`, `json` or `off` (one stderr line per finished request), as `--log` |
| `RESPAN_<VENDOR>_BASE_URL` | Moves a vendor's base URL when there is no config file |

**Client keys.** With `client_keys: [env:GATEWAY_KEY]` (or
`RESPAN_CLIENT_KEYS`) every request must carry one (`Authorization: Bearer`,
`x-api-key` or `x-goog-api-key`); the gateway takes it off and no provider
sees it. Without keys the gateway answers only to loopback and to
same-origin browsers, and **refuses to start on a non-loopback address**
(`--listen 0.0.0.0:8080`, `PORT`, Docker) so nobody who can reach it spends
your provider keys. `init --client-key` writes a strong key into the file.
A provider with its own key always uses it; one with none receives the
client's own credential headers, only when the client speaks the provider's
format, never across a translation.

**Configuration** is one YAML file, every setting shown in
[`gateway.example.yaml`](gateway.example.yaml): providers (vendor presets or
spelled out), `formats` served, `client_keys`, `default`, `plugins` (model
prefixes), `log`, `ui`, `body_limit`, `ws_message_limit`, `request_log`,
`shutdown_grace`. Routing order: `name/model` goes to
provider `name`; else the `models` plugin's prefixes; else the vendors'
prefixes (longest wins); else the kind's default
([routing](docs/ARCHITECTURE.md#routing)).

**Reloading**: `kill -HUP <pid>`, `POST /ui/api/reload` (with a client key if
keys are set) or `--watch` (on a change of the file) re-read the config and
swap it in for new requests; open requests and sessions finish on the old
one, the request log carries over, and an invalid config is refused while the
old one keeps serving. A changed `listen` needs a restart.

Not there yet: no admin writes (the web UI and admin API are read-only apart
from reload); the **MCP kind** exists as a library but is not wired into the
binary.

## What it speaks

Each kind translates within itself; a client and a provider may use
different formats of one kind.

| Kind | Formats (client and provider side) |
|---|---|
| chat | `openai-chat`, `anthropic-messages`, `gemini-generate`, `openai-responses`; plus `passthrough` for a vendor's other endpoints |
| realtime | `openai-realtime`, `gemini-live`, translated both ways; `responses-ws` relayed to the one provider of its format |
| image | `openai-images` (Imagen through Gemini's OpenAI-compatible endpoint) |
| transcription, speech | `openai-transcriptions`, `openai-speech` |
| embedding | `openai-embeddings` |
| video | `openai-videos` for clients; `gemini-veo` (Veo) as a provider |

Realtime usage after a tool call or an interruption may arrive in the next
`response.done` rather than the one that ended the turn
([details](crates/formats/realtime-ws/README.md)).

Reasoning controls (effort, budget, summaries) and `top_k` map between the
chat formats, or are refused where a format has no form for them
([table](crates/formats/conversation-http/README.md#reasoning-and-sampling)).
Details per format: [chat](crates/formats/conversation-http/README.md),
[media](crates/formats/media-http/README.md),
[realtime](crates/formats/realtime-ws/README.md).

## Vendors

`openai`, `anthropic`, `gemini`, `groq`, `deepseek`, `openrouter`, `xai`,
`mistral`, `together`, `fireworks`, `ollama`, `azure` and `vllm`, a file each
in [`crates/providers`](crates/providers/README.md), which lists the formats
each speaks, the quirks the gateway handles for it (`check` prints them)
and the base URL each format takes. Any other compatible endpoint is a
provider with a `base_url` and a format.

## Web UI

`/ui/` shows providers, routes, each request live (and a playground for
chat, images, transcription, speech and video) through a read-only admin
API under `/ui/api`. `ui: false` turns both off. See
[`apps/ui`](apps/ui/README.md).

## Python

`pip install respan-gateway` gives the same gateway in process: official
OpenAI and Anthropic SDK clients with no socket, a background server, and
the CLI. See [`python/README.md`](python/README.md).

## Library use

The binary is a library too: `respan_gateway::App::new(Config::from_env()?)?`
gives an `axum::Router`. To build your own gateway from the crates, with
your own plugins, read [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and the
runnable, tested [`examples/`](examples/README.md) of the
[use cases](docs/use-cases.md).

## Project layout

| Path | What it is |
|---|---|
| `crates/core/` | The neutral kernel and plugin API: typed streams, plugins, lifecycle, faults |
| `crates/kinds/` | Event types per kind: `conversation`, `media`, `realtime`, `mcp` |
| `crates/wires/` | Bytes at both edges, no kind: `http` (bodies, credentials, passthrough, Reqwest transport), `ws` |
| `crates/formats/` | A kind over a wire, no vendor: `conversation-http`, `media-http`, `realtime-ws`, `mcp-http` |
| `crates/providers/` | Every vendor, a file each: URL, key, models, formats, quirks |
| `crates/plugins/` | Model routing, fallback, race, backoff |
| `crates/serve/` | Hosting: one Axum router for HTTP and WebSocket sessions |
| `apps/gateway/` | The binary and its library: config, composition, client keys, `/v1/models`, request log, admin API |
| `apps/ui/` | The web UI (React, shadcn/ui), embedded in the binary |
| `python/` | The PyPI package: a PyO3 extension over `apps/gateway` |
| `examples/` | Runnable integrations of the use cases |
| `validation/` | Opt-in live-provider and external-tool checks, outside the workspace |

Layers, dependencies and contracts: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
Tests: [`docs/TESTING.md`](docs/TESTING.md). Contributing: [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Roadmap

Goal for 0.1: all 14 [use cases](docs/use-cases.md) pass as integration
tests, and a 10-minute plugin really takes 10 minutes.

| Milestone | Delivers | Status |
|---|---|---|
| M1 Relay kernel | Plugins, byte-for-byte relay, OpenAI and Anthropic, routing, YAML config, `check` | done |
| M2 Plugin API | Calls of their own input, async stream filters, unread bodies | done |
| M3 Formats | Gemini Live and `generateContent`, Responses over HTTP and WebSocket, OpenAI media endpoints, Veo | done |
| M4 Plugins and examples | Fallback, hedge, limits, memo, billing | done, as [examples](examples/README.md) |
| M5 Translation | Tools, images and reasoning between the chat formats | done |
| M6 Hard cases | Realtime translation, video jobs, PII rewriting, admission | done |
| M7 0.1 release | Release binaries, Docker image and PyPI wheels are built by tag workflows; first tag pending | open |
| M8 MCP gateway | The `mcp` kind and formats exist; not wired into the binary | partial |
| M9 Agent loop | Gateway-run MCP tools inside chat requests | open |

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option.
