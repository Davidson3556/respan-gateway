# respan-gateway

An LLM gateway with a neutral core and compiled-in plugins. The goal is to be
for LLM traffic what Nginx is for HTTP: a small, fast, predictable core, with
everything else built as modules on one public API.

> **Status: pre-alpha.** Relays OpenAI and Anthropic chat traffic and
> translates text and tools between them; serves images, transcription,
> speech and video jobs through OpenAI's media endpoints, Veo included.
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

## Try it

No config file is needed: every vendor whose key is in the environment
becomes a provider.

```bash
export OPENAI_API_KEY=sk-... ANTHROPIC_API_KEY=sk-ant-...
cargo run -p respan-gateway
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
`transcription`, `speech`, `video`) served at OpenAI's endpoints, to the
providers serving that kind:

```bash
# gpt-image-1 is OpenAI's; imagen-* would go to Gemini, through its OpenAI-compatible endpoint.
curl localhost:8080/v1/images/generations \
  -d '{"model":"gpt-image-1","prompt":"a lighthouse at dusk"}'
# whisper-1 is OpenAI's; whisper-large-v3 would go to Groq.
curl localhost:8080/v1/audio/transcriptions -F file=@talk.mp3 -F model=whisper-1
# A video is a job: veo-* goes to Gemini's Veo, answered in OpenAI's shape;
# polling GET /v1/videos/{id} and GET /v1/videos/{id}/content reach the provider that made it.
curl localhost:8080/v1/videos -d '{"model":"veo-3.0-generate-001","prompt":"a paper boat"}'
```

`GET /v1/models` lists every provider's models as clients name them, each
with its `kind`,
`GET /healthz` answers `ok`, and the web UI at
[localhost:8080/ui/](http://localhost:8080/ui/) shows providers, routes and
each request live; each finished request is also one line on stderr.

Vendor presets: `openai`, `anthropic`, `gemini` (`GEMINI_API_KEY` or
`GOOGLE_API_KEY`), `groq`, `deepseek`, `openrouter`, `xai`, `mistral`,
`together`, `fireworks`, each reading `<VENDOR>_API_KEY`, and `ollama`
(with `OLLAMA_HOST`). A preset speaks every format of its vendor compiled
in: OpenAI's media endpoints for `openai`, Imagen and Veo for `gemini`,
transcription and speech for `groq`, images for `xai` and `together`. To configure more, write a config and check it:

```bash
cargo run -p respan-gateway -- init    # a commented gateway.yaml from the keys found
cargo run -p respan-gateway -- check   # ./gateway.yaml, or -c FILE
cargo run -p respan-gateway            # reads ./gateway.yaml when there is one
```

`keys: [env:GATEWAY_KEY]` makes every request carry a gateway key
(`Authorization: Bearer`, `x-api-key` or `x-goog-api-key`), which is never
sent on. [`gateway.example.yaml`](gateway.example.yaml) shows every
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
cargo run -p respan-gateway -- proxy --openai      # add --listen ADDR or --url BASE
```

It relays chat and every other endpoint of that vendor (`count_tokens`,
`/v1/responses`) byte for byte, with each client's own credentials.

A provider's `base_url` is the one its own SDK takes. OpenAI-compatible
ones include the version, such as `https://api.openai.com/v1` or
`https://generativelanguage.googleapis.com/v1beta/openai` (format
`openai-chat`), `https://api.groq.com/openai/v1` (`groq-chat`) or
`https://openrouter.ai/api/v1` (`openrouter-chat`); Anthropic's does not:
`https://api.anthropic.com`. `openai-chat` reads only OpenAI's own fields; a
vendor whose fields the gateway reads has a format of its own name (the
[dialects](crates/kinds/conversation/README.md#openai-chat-completions)), and one whose additions it does not
read, as Gemini's endpoint, is `openai-chat`. The media formats
(`openai-images`, `openai-transcriptions`, `openai-speech`,
`openai-videos`) take the same base URL; Veo (`gemini-veo`, a provider
format only) takes the Gemini API's,
`https://generativelanguage.googleapis.com/v1beta`, with its key in
`x-goog-api-key` ([media kinds](crates/kinds/media/README.md)).

Sessions go over WebSocket: a provider of format `openai-realtime` (OpenAI
Realtime), `responses-ws` (OpenAI Responses over WebSocket) or
`gemini-live` (Gemini Live) has a `wss://` base URL, as its SDK takes it:
`wss://api.openai.com/v1`, `wss://generativelanguage.googleapis.com` (with
`auth: { type: header, name: x-goog-api-key, key: ... }`). Serving one makes
each WebSocket upgrade a session, relayed event by event to the one
provider of its format ([presets](crates/wires/ws/README.md)).

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
  must be copied explicitly. The binary's `proxy` mode registers this policy.
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

| Milestone | Delivers |
|---|---|
| M1 Relay kernel | Plugin chain, byte-for-byte relay, response edits, kinds, OpenAI and Anthropic, text translation both ways, model router, HTTP adapter, YAML config, `check` (done) |
| M2 Plugin API | Calls of their own input, async stream filters, unread bodies; API fixes before the freeze |
| M3 Formats | Gemini, Responses, embeddings |
| M4 Plugins and examples | Fallback, hedge, limits, memo, application billing |
| M5 Translation | Tool calls, images and reasoning across formats |
| M6 Hard cases | Realtime relay and bridges, jobs, PII rewriting, admission |
| M7 0.1 release | Docs, examples, benchmarks |
| M8 MCP gateway | `mcp` kind, relay, aggregation, per-key access |
| M9 Agent loop | Gateway-run MCP tools inside chat and Responses requests |

## Structure

The kernel defines extension contracts, and plugin authors use it directly;
first-party plugins and providers use those same public APIs. Adapters host
the gateway, and the binary chooses what to compose. See the
[repository layers](docs/ARCHITECTURE.md#repository-layers).

| Path | What it is |
|---|---|
| `crates/core/` | The neutral kernel and plugin-author API: typed streams, plugins, lifecycle, fault facts and native values; no HTTP or message kinds. |
| `crates/kinds/conversation/` | The conversation kind (requests, reply events, typed usage, assembly) and its HTTP formats: OpenAI Chat (with vendor dialects) and Anthropic Messages, sharing the `Schema` trait. |
| `crates/kinds/media/` | The media kinds, a kind per operation (image generation and edits, transcription, speech, video jobs), and their HTTP formats: OpenAI Images, Audio Transcriptions, Speech and Videos, and Google's Veo as a provider. |
| `crates/kinds/realtime/` | The realtime kind (a client/provider JSON event pair) and its WebSocket format, `JsonEvents`, with the vendors' presets. |
| `crates/kinds/mcp/` | The MCP kind (typed JSON-RPC messages and their envelopes) and its formats: streamable HTTP and stdio. |
| `crates/wires/http/` | The HTTP wire: owned edges, native bodies, stream/sink helpers, credentials, retry classification, the opaque format, and optional Reqwest transport and Axum hosting. |
| `crates/wires/ws/` | The WebSocket wire: sessions at both edges, close codes and limits, and optional Axum hosting. |
| `crates/plugins/` | Concrete model routing and public `routing` attempts, fallback, race and backoff. |
| `apps/gateway/` | The binary and its library (`respan_gateway`): configuration (YAML, vendor presets, or none, from the environment), routing (by model within each kind, video jobs by id), client keys, `/v1/models`, the request log, the web UI and its admin API; `serve`, `check`, `init` and `proxy`. |
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

Crates are of two sorts: a **kind** (`crates/kinds/`: `conversation`, `media`, `realtime`, `mcp`) owns
its events, its formats on its wire and its vendors' dialects; a **wire**
(`crates/wires/`: `http`, `ws`) moves bytes at both edges and knows no kind. A kind's formats
are behind default features, so a plugin can depend on its types alone. A
wire's `reqwest` (`HttpTransport`) and `axum` (hosting) features are opt-in,
so format authors and ordinary HTTP hosts do not need unrelated hosting or
transport code.

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
