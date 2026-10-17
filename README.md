# Respan Gateway

One endpoint for every LLM vendor. Point any official SDK at the gateway,
name any model, and it routes the request to the right provider,
translating between OpenAI, Anthropic and Gemini APIs on the way.

- **Any client, any model.** OpenAI Chat Completions, OpenAI Responses,
  Anthropic Messages and Gemini `generateContent` clients can all call
  any model, with tools, images and reasoning translated.
- **More than chat.** Realtime voice (OpenAI Realtime ⇄ Gemini Live),
  images, speech, transcription, embeddings and video jobs. Anything else
  is relayed untouched.
- **Routing and resilience.** Named routes, globs, `provider/model`,
  fallback, retries, hedged races, and picking the cheapest or fastest.
- **MCP.** It fronts MCP servers behind your client keys. It can also run
  their tools inside chat requests for clients that know nothing of MCP.
- **Observability.** A live request log and web UI, per-call pricing, and
  traces sent to [Respan](https://respan.ai).
- **Extensible.** Every feature above is a plugin built on one public Rust
  API. See the [plugin guide](PLUGINS.md).

It ships as a single binary with a web UI built in, as a Python package,
and as a set of Rust crates.

> **Status: pre-alpha.** The core and the formats are tested against
> strict fakes of each vendor. APIs around the core may still change.

## Quickstart

```bash
export OPENAI_API_KEY=sk-... ANTHROPIC_API_KEY=sk-ant-... GEMINI_API_KEY=...
respan-gateway
```

You don't need a config file. Every vendor whose key is in the
environment becomes a provider, and the gateway prints where it listens
and a `curl` command to try.

The model name picks the provider, whatever API the client speaks:

```bash
# OpenAI Chat client → Claude
curl localhost:8080/v1/chat/completions \
  -d '{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hi"}]}'

# Anthropic client → GPT
curl localhost:8080/v1/messages \
  -d '{"model":"gpt-4o-mini","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}'

# `provider/model` names a provider explicitly
curl localhost:8080/v1/responses -d '{"model":"groq/llama-3.3-70b-versatile","input":"hi"}'
```

An SDK only needs a base URL:

```python
from openai import OpenAI
OpenAI(base_url="http://localhost:8080/v1", api_key="unused").chat.completions.create(
    model="gemini-2.5-flash", messages=[{"role": "user", "content": "hi"}])

from anthropic import Anthropic
Anthropic(base_url="http://localhost:8080", api_key="unused").messages.create(
    model="gpt-4o-mini", max_tokens=64, messages=[{"role": "user", "content": "hi"}])

from google import genai
genai.Client(api_key="unused", http_options={"base_url": "http://localhost:8080"}) \
    .models.generate_content(model="claude-sonnet-4-5", contents="hi")
```

Media uses OpenAI's endpoints and goes to whichever provider serves the model:

```bash
curl localhost:8080/v1/images/generations -d '{"model":"gpt-image-1","prompt":"a lighthouse"}'
curl localhost:8080/v1/audio/transcriptions -F file=@talk.mp3 -F model=whisper-1
curl localhost:8080/v1/embeddings -d '{"model":"text-embedding-3-small","input":"hello"}'
curl localhost:8080/v1/videos -d '{"model":"veo-3.0-generate-001","prompt":"a paper boat"}'
```

`GET /v1/models` lists every provider's models and your route names
(`/v1beta/models` gives them in Gemini's shape). `GET /healthz` is a health
check, and the web UI is at [localhost:8080/ui/](http://localhost:8080/ui/).

## Install

| Way | How |
|---|---|
| Release binary | Linux, macOS and Windows builds, with the web UI built in, on each [GitHub release](https://github.com/respanai/respan-gateway/releases) |
| Docker | `docker run -p 8080:8080 -e OPENAI_API_KEY -e RESPAN_CLIENT_KEYS=sk-gw-change-me ghcr.io/respanai/respan-gateway` |
| Python | `pip install respan-gateway`, the same gateway in process (see [`python/`](python/README.md)) |
| Cargo | `cargo install --git https://github.com/respanai/respan-gateway respan-gateway` |

No release is tagged yet, so the binary, Docker image and PyPI package
are not published; until the first tag, build from source.

To build from a checkout, build the UI first so it is embedded:
`cd apps/ui && pnpm install && pnpm build`, then
`cargo build --release -p respan-gateway`. Without the UI build, the binary
serves a placeholder page at `/ui/`; everything else works.

## Command line

```
respan-gateway [serve]           Serve ./gateway.toml, or the providers in the environment
respan-gateway init              Write a commented gateway.toml (-p groq enables a vendor;
                                 --client-key generates a client key and prints it once)
respan-gateway check [--online]  Show what a config serves; --online also calls each provider
respan-gateway models            List every provider's models
respan-gateway mcp login <name>  Log in once to an MCP server that needs a person to approve
respan-gateway proxy --anthropic|--openai
                                 Sit in front of Claude Code or Codex, relaying the vendor's
                                 whole API with each client's own credentials
```

Settings come from flags, the environment, or the nearest `.env` file. The
environment wins over `.env`.

| Variable | Meaning |
|---|---|
| `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY`, `GROQ_API_KEY`, … | Provider keys. With no config file, each key makes its vendor a provider |
| `RESPAN_CONFIG` | The config file (`--config`) |
| `RESPAN_LISTEN` | Where to listen (`--listen`): `127.0.0.1:8080`, `localhost:8080` or `8080` |
| `PORT` | The port to listen on, on every address, for hosts that assign one |
| `RESPAN_CLIENT_KEYS` | Client keys, comma separated |
| `RESPAN_LOG` | `text`, `json` or `off` (`--log`) |
| `RESPAN_<VENDOR>_BASE_URL` | Move a vendor's base URL when there is no config file |

## Configuration

The config is one TOML file, `gateway.toml`, reloaded without a restart on
`SIGHUP`, `POST /ui/api/reload`, or a file change with `--watch`.
[`gateway.example.toml`](gateway.example.toml) documents every setting. A
typical file:

```toml
listen = "127.0.0.1:8080"
client_keys = [
  { name = "ops", key = "env:OPS_KEY", admin = true },
  { name = "app", key = "env:APP_KEY" },
]

[providers.openai]
vendor = "openai"
key = "env:OPENAI_API_KEY"

[providers.anthropic]
vendor = "anthropic"
key = "env:ANTHROPIC_API_KEY"

[providers.local]                       # Any OpenAI-compatible endpoint
formats = ["openai-chat"]
base_url = "http://localhost:8000/v1"

[routes.smart]                          # Clients ask for "model": "smart"
to = ["anthropic/claude-opus-4-1", "openai/gpt-5"]
tries = 2                               # Then fall back to the next candidate

[routes."llama-*"]
to = "local"

[plugins.respan]                        # Each request a trace in Respan
key = "env:RESPAN_API_KEY"
```

Write secrets as `env:NAME` to keep them out of the file.

### Client keys

With `client_keys` set, every request must carry one, as
`Authorization: Bearer`, `x-api-key` or `x-goog-api-key`. The gateway strips
it, so no provider ever sees it. Without keys, the gateway answers only
loopback and same-origin browsers, and **refuses to start on a non-loopback
address**, so nobody who can reach it spends your provider keys.

A key may be named, may be `admin` (needed for the admin API under
`/ui/api/`), and may be limited to certain `tools`. Browser apps on other
origins need a `cors = { origins = [...] }` entry.

### Routing

A request's model goes, in order, to:

1. a route of exactly that name;
2. `provider/model`, to that provider;
3. the first glob route that matches (`"llama-*"`);
4. the provider whose model list has it (the list is fetched at start and
   every 10 minutes);
5. otherwise, the default provider for that kind of request.

A route's `to` is a candidate or a list: `provider`, `provider/model`, or
`provider/{model}`. `pick` orders the candidates: `first`, `cheapest`,
`fastest`, `weighted` or `random`. `needs = ["tools"]` drops models that
lack a capability, and `when` limits a route to certain client keys or
kinds. A failed candidate falls back to the next (`tries`, `backoff`), and
`hedge = "200ms"` races them instead. To see where a model would go and
why, use `GET /ui/api/routes/explain?model=M`.

### Pricing

Each call is priced at its model's price, from the provider's model list or
from a price you declare:

```toml
[models."openai/my-finetune"]
pricing = { input = 1.0, output = 2.0, cached_input = 0.5 }   # $ per million tokens
```

A request's cost is the sum over its calls, retries and tool-loop steps
included. The request log and the UI show it.

### Conversation state

A Responses client's `previous_response_id` works with every provider, not
only OpenAI. When a translated provider keeps no state, the gateway keeps
it in memory for `keep` (default 1 hour). A Chat client's reasoning between
tool calls is kept the same way when OpenAI's Responses API serves it.
`[plugins.responses] enabled = false` turns this off.

## MCP

The gateway fronts [MCP](https://modelcontextprotocol.io) servers behind the
same client keys, serving each at `/mcp/{name}`:

```toml
[mcp.github]                            # HTTP upstream
url = "https://api.githubcopilot.com/mcp/"
auth = { bearer = "env:GITHUB_TOKEN" }

[mcp.files]                             # stdio upstream, a child per client session
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "/data"]

[mcp.linear]                            # A server a person must approve once
url = "https://mcp.linear.app/mcp"
auth = "login"                          # Run: respan-gateway mcp login linear
```

The fields match an MCP client's `mcp.json`, so entries copy across. `auth`
can be:

- `{ bearer = "env:KEY" }`, a static key;
- `{ oauth = { token_url, client_id, client_secret, ... } }`, the gateway's
  own OAuth client;
- `"login"`, a login saved by `respan-gateway mcp login <name>`. That
  command opens your browser on Enter. On a headless machine, it takes the
  callback address pasted back instead;
- `"forward"`, which sends each client's own credentials on.

**One endpoint for all servers.** `[plugins.mcp_aggregate]` serves `/mcp`,
which merges every server into one, with tools named `{server}__{tool}` and
optional `allow`/`deny` globs.

**Tool loop.** With `[plugins.tool_loop]`, the gateway gives the model the
MCP servers' tools and runs the calls itself, for any chat format and any
provider, and in realtime sessions too. The client sees one ordinary reply.

```toml
[plugins.tool_loop]
servers = ["github"]
allow = ["github__search_*"]            # Offer only what you would let any client do
max_steps = 8
```

A tool's description and output are untrusted input to the model. Offer
only tools from servers you trust, and use `deny` to keep destructive ones
out. A client can opt one request out with `x-respan-tool-loop: off`.

## What it speaks

| Kind | Formats, client or provider side |
|---|---|
| chat | `openai-chat`, `openai-responses`, `anthropic-messages`, `gemini-generate`; `passthrough` for a vendor's other endpoints |
| realtime | `openai-realtime` ⇄ `gemini-live`; `responses-ws` relayed |
| image | `openai-images` |
| transcription, speech | `openai-transcriptions`, `openai-speech` |
| embedding | `openai-embeddings` |
| video | `openai-videos`, `gemini-veo` |

A client and a provider may speak different formats of the same kind.
Translation details and edge cases are in each format's README, for
[chat](crates/formats/conversation-http/README.md),
[media](crates/formats/media-http/README.md) and
[realtime](crates/formats/realtime-ws/README.md).

**Vendors:** `openai`, `anthropic`, `gemini`, `respan`, `typesafe`, `groq`,
`deepseek`, `openrouter`, `xai`, `mistral`, `together`, `fireworks`,
`ollama`, `azure`, `vllm` and `cloudflare`. Any other compatible endpoint is
a provider with `formats` and a `base_url`. Each vendor's URLs, formats and
quirks are in [`crates/vendors`](crates/vendors/README.md).

## Use it as a library

The binary is a library, `respan_gateway`: `App::new(config)?.router()`
returns an `axum::Router`. To build your own gateway from the crates, with
your own plugins, start with the [plugin guide](PLUGINS.md) and the runnable
[`examples/`](examples/README.md).

## Repository layout

| Path | What it is |
|---|---|
| `crates/core/` | The kernel and the plugin API: typed event streams, hooks, lifecycles, faults |
| `crates/kinds/` | Event types per kind: `conversation`, `media`, `decision`, `realtime`, `mcp` |
| `crates/wires/` | Bytes at both edges: HTTP, SSE, WebSocket, the Reqwest transport |
| `crates/formats/` | A kind over a wire, one spec each, naming no vendor |
| `crates/vendors/` | Every vendor, a file each; `green/` is verified live, `yellow/` against fakes |
| `crates/plugins/` | First-party plugins: routing, tool loop, pricing, Respan traces, … |
| `crates/serve/` | Hosting: one Axum router for HTTP and WebSocket |
| `apps/gateway/` | The binary: config, composition, client keys, request log, admin API |
| `apps/ui/` | The web UI (React), embedded in the binary |
| `python/` | The PyPI package, a PyO3 extension over `apps/gateway` |
| `examples/` | Runnable, tested policies: billing, guardrails, PII, admission, jobs, … |
| `validation/` | Opt-in tests against live providers and external tools |

[CONTRIBUTING.md](CONTRIBUTING.md) explains how to build, test and release.

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at
your option.
