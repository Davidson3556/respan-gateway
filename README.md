# respan-gateway

An LLM gateway with a neutral core and compiled-in plugins. The goal is to be
for LLM traffic what Nginx is for HTTP: a small, fast, predictable core, with
everything else built as modules on one public API.

> **Status: pre-alpha.** Relays OpenAI and Anthropic chat traffic and
> translates text and tools between them; serves images, transcription,
> speech, embeddings and video jobs through OpenAI's media endpoints, Veo included;
> translates realtime voice sessions between OpenAI Realtime and Gemini Live.
> APIs will change. The core is now treated as half-frozen, like a release candidate:
> its contracts are stable by default while the surrounding APIs continue to evolve.

## Ground rules

1. **The core is neutral.** Routing, billing, guardrails, PII redaction,
   caching and logging are plugins. A feature nobody has thought of yet must be
   writable with the same API as the built-in ones.
2. **Plugins are compiled in.** No dynamic loading of any kind. A custom
   gateway is your own binary that depends on these crates.

Current contracts and package boundaries:
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Install

- **Release binary**: each `v*` tag has `respan-gateway` for Linux (x86_64,
  aarch64) and macOS (aarch64), with the web UI built in, on the
  repository's GitHub releases page.
- **Docker**: `docker build -t respan-gateway .`, then
  `docker run -p 127.0.0.1:8080:8080 -e OPENAI_API_KEY respan-gateway`. The image
  listens on `0.0.0.0:8080`, so set `keys` (below).
- **Python**: `pip install respan-gateway` ([`python/`](python/README.md)).
- **From source**: `cd apps/ui && pnpm install && pnpm build`, then
  `cargo build --release -p respan-gateway`. Without the first step the
  binary builds and serves a page that says the UI is missing; the admin
  API works either way.

## Try it

No config file is needed: every vendor whose key is in the environment
becomes a provider.

```bash
export OPENAI_API_KEY=sk-... ANTHROPIC_API_KEY=sk-ant-...
respan-gateway                  # or, from a checkout: cargo run -p respan-gateway
```

It prints where it listens, the providers it found, what it serves and a
`curl` to try. Models go to their vendor by name, translated when the
client speaks another vendor's API:

```bash
# An OpenAI client asking for a Claude model is translated to Anthropic,
curl localhost:8080/v1/chat/completions \
  -d '{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hi"}]}'
# an Anthropic client asking for a GPT model is translated to OpenAI,
curl localhost:8080/v1/messages \
  -d '{"model":"gpt-4o-mini","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}'
# and `provider/model` picks a provider (here `groq`) and sends `model`.
curl localhost:8080/v1/chat/completions \
  -d '{"model":"groq/llama-3.3-70b-versatile","messages":[{"role":"user","content":"hi"}]}'
```

Media go the same way, each operation a kind of its own (`image`,
`transcription`, `speech`, `embedding`, `video`) served at OpenAI's endpoints, to the
providers serving that kind:

```bash
# gpt-image-1 is OpenAI's; imagen-* would go to Gemini, through its OpenAI-compatible endpoint.
curl localhost:8080/v1/images/generations \
  -d '{"model":"gpt-image-1","prompt":"a lighthouse at dusk"}'
# whisper-1 is OpenAI's; whisper-large-v3 would go to Groq.
curl localhost:8080/v1/audio/transcriptions -F file=@talk.mp3 -F model=whisper-1
# text-embedding-3-small is OpenAI's; gemini-embedding-001 would go to Gemini.
curl localhost:8080/v1/embeddings \
  -d '{"model":"text-embedding-3-small","input":"hello"}'
# A video is a job: veo-* goes to Gemini's Veo, answered in OpenAI's shape;
# polling GET /v1/videos/{id} and GET /v1/videos/{id}/content reach the provider that made it.
curl localhost:8080/v1/videos -d '{"model":"veo-3.0-generate-001","prompt":"a paper boat"}'
```

`GET /v1/models` lists every provider's models as clients name them, each
with its `kind`,
`GET /healthz` answers `ok`, and the web UI at
[localhost:8080/ui/](http://localhost:8080/ui/) shows providers, routes and
each request live; each finished request is also one line on stderr. The UI
reads a read-only admin API, `GET /ui/api/overview`, `providers`, `routes`,
`requests` (and `requests/{id}`) and `events` (a live stream), which
needs a gateway key when `keys` is set. `ui: false` turns off the UI and
the admin API together.

Vendor presets: `openai`, `anthropic`, `gemini` (`GEMINI_API_KEY` or
`GOOGLE_API_KEY`), `groq`, `deepseek`, `openrouter`, `xai`, `mistral`,
`together`, `fireworks`, each reading `<VENDOR>_API_KEY`, `ollama`
(with `OLLAMA_HOST`), and `azure` (`AZURE_OPENAI_API_KEY`) and `vllm`, which
need a `base_url`. Each is a file in
[`crates/providers`](crates/providers/README.md), with the quirks the gateway
handles for it; `check` prints them. Without a config file, `RESPAN_<VENDOR>_BASE_URL`
moves a vendor's base URL; with one, the file's `base_url` alone counts
(`init` writes the moved URL, and `OLLAMA_HOST`'s, into it). A preset speaks every format of its vendor: OpenAI's media endpoints for `openai`, Imagen and Veo for `gemini`,
transcription and speech for `groq`, images for `xai` and `together`,
embeddings for `gemini`, `mistral`, `together`, `fireworks` and `ollama` too. To configure more, write a config and check it:

```bash
cargo run -p respan-gateway -- init    # a commented gateway.yaml from the keys found
cargo run -p respan-gateway -- check   # ./gateway.yaml, or -c FILE
cargo run -p respan-gateway            # reads ./gateway.yaml when there is one
```

`keys: [env:GATEWAY_KEY]` makes every request carry a gateway key
(`Authorization: Bearer`, `x-api-key` or `x-goog-api-key`), which is never
sent on. A provider with its own key (a `key`, an `auth`, or a preset's key
in the environment) always uses that. A provider with none receives the
client's own credential headers, but only when the client speaks that
provider's format (an Anthropic client's `x-api-key` to an Anthropic
provider), never across a translation. Without keys, the gateway refuses (403) a browser's
cross-origin request (an `Origin` other than its own, or
`Sec-Fetch-Site: cross-site`) and, unless it listens on every address, a
`Host` other than `localhost`, `127.0.0.1`, `[::1]`, its listen address or
`respan-gateway`, so no web page can spend your provider keys; curl and the
web UI are served as before. Behind a proxy that rewrites `Host`, set keys.

A model name picks its provider in this order: `name/model` goes to the
provider `name`, which gets `model`, if `name` is a configured provider;
otherwise the whole name is the model, routed by the `models` plugin's
prefixes, then by the presets' (the longest matching prefix wins, then
config order), then to the kind's default. So `meta-llama/Llama-3.3-70B-Instruct-Turbo`
and `deepseek-ai/...` go to `together`, `accounts/fireworks/models/...` to
`fireworks`, and OpenRouter's `anthropic/claude-3.5-sonnet` to `openrouter`
unless a provider is named `anthropic`; `openrouter/anthropic/claude-3.5-sonnet`
always goes to OpenRouter. A provider's name is letters, digits, `_`, `.`
and `-`. [`gateway.example.yaml`](gateway.example.yaml) shows every
setting, presets first. The binary is a library too, `respan_gateway`:
`App::new(Config::from_env()?)?` gives an `axum::Router` to serve or to
hand requests to in process.

Each format served recognises its own requests (OpenAI Chat a `POST` to
`.../chat/completions`, Anthropic Messages to `.../v1/messages`); anything
else is a 404, unless the `passthrough` format is served last, which relays
every other request to a vendor unread (see `gateway.example.yaml`), except
a path readers could read two ways. Passthrough takes a path only if, after
one percent-decode, every byte is an ASCII letter or digit or one of
`/ - _ . ~ : @ + = , !`, and no segment is `.`, `..` or ends in `.`; anything
else (non-ASCII, space, `?`, `#`, `%`, `;`, `\`, control characters) is a 404.
It is an allowlist, so a character some reader treats specially is refused
until someone shows a vendor needs it.

No config file is needed to sit in front of Claude Code or Codex:

```bash
cargo run -p respan-gateway -- proxy --anthropic   # ANTHROPIC_BASE_URL=http://127.0.0.1:8080
cargo run -p respan-gateway -- proxy --openai      # add --listen ADDR (host:port, :port or a port) or --url BASE
```

It relays chat and every other endpoint of that vendor (`count_tokens`,
`/v1/responses`) byte for byte, with each client's own credentials.

A provider's `base_url` is the one its own SDK takes. OpenAI-compatible
ones include the version, such as `https://api.openai.com/v1` or
`https://generativelanguage.googleapis.com/v1beta/openai` (format
`openai-chat`), `https://api.groq.com/openai/v1` or
`https://openrouter.ai/api/v1` (`openai-chat` too); Anthropic's does not:
`https://api.anthropic.com`. A format is its spec: what a vendor sends beyond
it stays native, and a provider with `vendor:` gets that vendor's
[quirks](crates/providers/README.md), such as Groq's usage under `x_groq` or
DeepSeek's `reasoning_content`, read into the gateway's events. The media formats
(`openai-images`, `openai-transcriptions`, `openai-speech`,
`openai-videos`, `openai-embeddings`) take the same base URL; Veo (`gemini-veo`, a provider
format only) takes the Gemini API's,
`https://generativelanguage.googleapis.com/v1beta`, with its key in
`x-goog-api-key` ([media formats](crates/formats/media-http/README.md)).

Sessions go over WebSocket: a provider of format `openai-realtime` (OpenAI
Realtime), `responses-ws` (OpenAI Responses over WebSocket) or
`gemini-live` (Gemini Live) has a `wss://` base URL, as its SDK takes it:
`wss://api.openai.com/v1`, `wss://generativelanguage.googleapis.com` (with
`auth: { type: header, name: x-goog-api-key, key: ... }`). Serving one makes
each WebSocket upgrade a session, relayed event by event. A realtime
session goes to the provider of the model at its URL's `?model=` (a Gemini
Live client's too, by the gateway's convention), translated if that
provider speaks the other API
([realtime formats](crates/formats/realtime-ws/README.md)); a Responses one
to the one provider of its format.

## Glossary

A client sends a request, plugins look at it, the gateway sends it on to a
provider, and the response streams back through the plugins. The kernel
routes events and never sees a byte, a head or a key: the client edge and
the provider edge own them
([architecture](docs/ARCHITECTURE.md#one-shape-at-both-edges)).

- **Gateway**: what you build and hand requests to: providers, client
  formats, plugins and a default target, each registered by value.
- **Plugin**: four hooks, `on_client` and `on_client_end` once per
  request, `on_provider` and `on_provider_end` once per call, in
  registration order. `on_client` passes (`Ok(None)`), replies or refuses.
- **Filter**: edits a request's inputs or a response's outputs as they
  stream; its type says which. It sees every message. For work that waits,
  such as a remote check or a deadline, a plugin wraps the stream in its
  own instead (`cx.wrap`).
- **Format<T, R, W>**: one protocol's binding to owned I/O `T`, receiving
  `R` and sending `W`. Its `bind(io)` returns a `Connection<R, W>` tuple:
  an incoming opening future and an outgoing `EventSink<W>`.
  `EventStream<T>` implements `Stream<Item = Result<T, Fault>>`, and
  `EventSink<T>` implements `Sink<Result<T, Fault>, Error = Fault>`.
  An optional synchronous preparation step configures the sink from stream
  its shape or a finite input before delivery. The kernel pumps events with
  backpressure concurrently with receiving, then closes the sink. The gateway
  chooses the type order for each edge; Format has no client/provider methods.
  Formats of one kind translate through typed events. Calls always encode
  typed input unless an application explicitly supplies raw HTTP data. HTTP formats implement this same contract
  directly: `Format<ClientIo, Input, Output>` accepts a client exchange and
  `Format<ProviderIo, Output, Input>` makes a provider call. Their mirrored
  `bind`s each take a decoder, an encoder and an error writer (client) or
  reader (provider); the HTTP crate's bindings own the rest, not a second
  format trait or an implementation macro.
- **Call**: what the gateway sends a provider on a request's behalf
  (`cx.call`): the request's inputs, as filters and wraps left them, or
  one input of its own (`with_input`). HTTP plugins opt into raw requests
  with `SendRaw(Wire)`, choose heads with `ProviderHead` and `ClientHead`,
  and opt into raw errors with `RawReply`. Credentials remain separate and
  must be copied explicitly. The binary copies a client's credentials to a
  keyless provider of the client's own format, and its `proxy` mode relays
  every request raw with them.
- **Response**: what a call, or `Response::stream`, gives back: read
  (`outputs`), held (`buffer_until`, `until_end`) or sent. Holding borrows the
  response mutably; buffered events, errors and call ownership remain available
  for replay even when inspection fails.
- **Fault**: the one error: a code that says whose fault it is, a message,
  a retry and typed hints such as HTTP's `Status`.

## Roadmap

Goal for 0.1: all 14 [use cases](docs/use-cases.md) pass as integration
tests, and a 10-minute plugin really takes 10 minutes. Each use case becomes a
test when its milestone lands. The kernel is already treated as RC-like; M2
closes remaining contract gaps rather than reopening its design.

| Milestone | Delivers | Status |
|---|---|---|
| M1 Relay kernel | Plugin chain, byte-for-byte relay, response edits, kinds, OpenAI and Anthropic, text translation both ways, model router, HTTP adapter, YAML config, `check` | done |
| M2 Plugin API | Calls of their own input, async stream filters, unread bodies; API fixes before the freeze | done |
| M3 Formats | Gemini Live and Responses over WebSocket, OpenAI's media endpoints, Veo. Not yet Gemini `generateContent` or Responses over HTTP (those relay through `passthrough`) | partial |
| M4 Plugins and examples | Fallback, hedge, limits, memo, application billing | done, as [examples](examples/README.md) |
| M5 Translation | Tools, images and reasoning between OpenAI Chat and Anthropic Messages | done |
| M6 Hard cases | Realtime relay and OpenAI Realtime / Gemini Live translation, video jobs, PII rewriting and admission (examples) | done |
| M7 0.1 release | Docs, examples, benchmarks; release binaries and a Dockerfile exist | open |
| M8 MCP gateway | The `mcp` kind and streamable HTTP / stdio formats exist; no relay, aggregation or per-key access in the binary | partial |
| M9 Agent loop | Gateway-run MCP tools inside chat and Responses requests | open |

## Structure

The kernel defines extension contracts, and plugin authors use it directly;
first-party plugins and providers use those same public APIs. Adapters host
the gateway, and the binary chooses what to compose. See the
[repository layers](docs/ARCHITECTURE.md#repository-layers).

| Path | What it is |
|---|---|
| `crates/core/` | The neutral kernel and plugin-author API: typed streams, plugins, lifecycle, fault facts and native values; no HTTP or message kinds. |
| `crates/kinds/conversation/` | The conversation kind: requests, reply events, typed usage and assembly. |
| `crates/kinds/media/` | The media kinds, a kind per operation: image generation and edits, transcription, speech, embeddings, video jobs. |
| `crates/kinds/realtime/` | The realtime kind: a live session's events, both ways. |
| `crates/kinds/mcp/` | The MCP kind: typed JSON-RPC messages and their envelopes. |
| `crates/wires/http/` | The HTTP wire: owned edges, native bodies, stream/sink helpers, credentials, retry classification, the passthrough format, and and an optional Reqwest transport. |
| `crates/wires/ws/` | The WebSocket wire: sessions at both edges, close codes and limits. |
| `crates/serve/` | Hosting: one Axum router serving a gateway's plain HTTP and WebSocket sessions, with the body limit, session limits and graceful stop. |
| `crates/formats/conversation-http/` | The conversation kind over HTTP: OpenAI Chat and Anthropic Messages, sharing the `Schema` trait. |
| `crates/formats/media-http/` | The media kinds over HTTP: OpenAI Images, Audio Transcriptions, Speech, Embeddings and Videos, and Google's Veo as a provider. |
| `crates/formats/realtime-ws/` | The realtime kind over WebSocket: OpenAI Realtime, Gemini Live, and Responses over WebSocket. |
| `crates/formats/mcp-http/` | MCP over HTTP: streamable HTTP, and a stdio server as a transport. |
| `crates/providers/` | Every vendor, a file each: base URL, key, models, the formats it speaks, its quirks (data and a plugin), and a table of them. |
| `crates/plugins/` | Concrete model routing and public `routing` attempts, fallback, race and backoff. |
| `apps/gateway/` | The binary and its library (`respan_gateway`): configuration (YAML, vendor presets, or none, from the environment), routing (by model within each kind, video jobs by id), client keys, `/v1/models`, the request log, the web UI and its admin API; `serve`, `check`, `init` and `proxy`. |
| `apps/ui/` | The web UI: React and shadcn/ui, built with `pnpm build` to `apps/ui/dist`, which `apps/gateway/build.rs` embeds in the binary. |
| `tests/support/` | Private source-only helpers shared by integration tests; not a public crate. |
| `examples/` | Runnable, tested integrations of the [use cases](examples/README.md). |
| `validation/` | Separate opt-in live-provider and external-tool checks, excluded from workspace tests. |
| `python/` | The Python package (PyPI `respan-gateway`): a PyO3 extension over the `apps/gateway` library, in-process httpx transports, SDK clients, a background server and the CLI. |

Core exposes contracts and shared types, not production `Plugin` implementations.
It does not depend on protocol infrastructure, providers, plugins or adapters.
Specialized provider-aware text reading, holdback and placeholders are private
[example support](examples/support/README.md), not a public API.
Billing policies, priced tallies, reservations and settlement live in private
[example support](examples/README.md#billing-policies), not in a ledger crate or
public accounting plugin. The conversation kind's typed `Usage` is the shared fact; applications own
prices, accounts and durable billing stores. Routing takes an explicit retry classifier, with HTTP classification
available from the HTTP protocol module. The PyO3 bindings and PyPI package live together in
[`python/`](python/README.md).

Crates are in layers: a **kind** (`crates/kinds/`: `conversation`, `media`,
`realtime`, `mcp`) is its events' types alone; a **wire** (`crates/wires/`:
`http`, `ws`) moves bytes at both edges and knows no kind; a **format**
(`crates/formats/`) is a kind over a wire, and names no vendor; and
`crates/providers` is every vendor, a file each, with the quirks it sets on
the formats it speaks. A wire's `reqwest` feature (`HttpTransport`) is opt-in, and hosting is
`crates/serve`'s, so format authors do not need unrelated transport or
server code.

HTTP formats prepare actual requests and event sinks synchronously, then let
core drive delivery. Response readers return streams. Finite inputs are encoded
before sending; an explicit `SendRaw` request can bypass deferred decoding.
`Encoder<T>` only writes values and terminal framing; it does not choose heads,
open responses or act as a transport. Stateful parsers and encoders remain
protocol code, while HTTP helpers own backpressure and response handoff.
Use `http` builders and request/response parts directly, `http-body-util`
for data-stream conversion, and `bytes` for buffer storage. The gateway body
retains inspectable finite bytes and upload ownership; it is not a replacement
for standard HTTP structure or byte buffers.

## Development

See [`CONTRIBUTING.md`](CONTRIBUTING.md).

Checks by area: Rust as in `CONTRIBUTING.md`; the UI with
`cd apps/ui && pnpm install && pnpm lint && pnpm typecheck && pnpm test && pnpm build`;
Python with `cd python && maturin develop && pytest`.

### End-to-end tests

`apps/gateway/tests/e2e.rs` starts the built binary on a free
port with a temp config, and talks real HTTP to it and to fake providers
on localhost from [mockllm](https://github.com/lihe07/mockllm), which
check each call by the real provider's rules and answer in its exact
framing, streams and breaks included. They run with the default test suite. Live provider tests are in a separate
validation package outside the workspace:

```bash
cargo test -p respan-gateway --test e2e
# Explicit live tests; small requests can incur provider charges:
OPENAI_API_KEY=sk-... ANTHROPIC_API_KEY=sk-ant-... \
  cargo test --manifest-path validation/Cargo.toml --test live_providers -- --ignored
```

See [validation coverage and live-provider limitations](docs/TESTING.md).

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option.
