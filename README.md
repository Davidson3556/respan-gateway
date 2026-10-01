# respan-gateway

An LLM gateway with a neutral core and compiled-in plugins. The goal is to be
for LLM traffic what Nginx is for HTTP: a small, fast, predictable core, with
everything else built as modules on one public API.

> **Status: pre-alpha.** Relays OpenAI and Anthropic chat traffic and
> translates text and tools between them; images come in M5. APIs will
> change. The core is now treated as half-frozen, like a release candidate:
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

```bash
cp gateway.example.yaml gateway.yaml
export OPENAI_API_KEY=sk-... ANTHROPIC_API_KEY=sk-ant-...
cargo run -p respan-gateway -- check
cargo run -p respan-gateway

# An OpenAI client asking for a Claude model is translated to Anthropic,
curl localhost:8080/v1/chat/completions \
  -d '{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hi"}]}'
# and an Anthropic client asking for a GPT model is translated to OpenAI.
curl localhost:8080/v1/messages \
  -d '{"model":"gpt-4o-mini","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}'
```

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
ones include the version, such as `https://api.openai.com/v1`,
`https://api.groq.com/openai/v1` or
`https://generativelanguage.googleapis.com/v1beta/openai`; Anthropic's does
not: `https://api.anthropic.com`.

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
  metadata or a finite input before delivery. The kernel pumps events with
  backpressure concurrently with receiving, then closes the sink. The gateway
  chooses the type order for each edge; Format has no client/provider methods.
  Formats with the same name can relay original bytes and compatible
  metadata even across different concrete transports;
  formats of one kind translate. HTTP formats implement this same contract
  directly: `Format<Incoming, Input, Output>` accepts a client exchange and
  `Format<Outgoing, Output, Input>` makes a provider call. The HTTP crate
  supplies native HTTP bodies and stream/sink construction helpers, not a
  second format trait or an implementation macro.
- **Call**: what the gateway sends a provider on a request's behalf
  (`cx.call`): the request's inputs, as filters and wraps left them, or
  one input of its own (`with_input`). Only a call of the request's inputs
  to a provider of the client's format name is a relay, and carries the
  client's protocol metadata and key.
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

The kernel defines extension contracts; one SDK exposes them to plugin authors;
first-party plugins and providers use those same public APIs. Adapters host
the gateway, and the binary chooses what to compose. See the
[repository layers](docs/ARCHITECTURE.md#repository-layers).

| Path | What it is |
|---|---|
| `core/` | The neutral kernel: typed streams, plugins and lifecycle; no HTTP or message kinds. |
| `sdk/` | One plugin-author facade: core contracts, conversation/usage types and basic typed text accessors; no HTTP dependency. |
| `http/` | Owned HTTP edges, native request/response bodies, stream/sink helpers, credentials, retry classification and optional Reqwest backend; framing/error helpers in `formats`. |
| `plugins/` | Concrete model routing and public `routing` attempts, fallback, race and backoff. |
| `{mcp,ws}/` | Optional MCP and WebSocket protocol/transport integrations. |
| `providers/` | First-party OpenAI Chat and Anthropic Messages codecs, reusable on both edges. |
| `adapters/axum/` | Axum HTTP ingress and optional WebSocket hosting. |
| `gateway/` | The binary: YAML configuration, `check`, `proxy` and composition. |
| `tests/support/` | Private source-only helpers shared by integration tests; not a public crate. |
| `examples/` | Runnable, tested integrations of the [use cases](examples/README.md). |

The SDK exposes contracts and shared types, not production `Plugin` implementations.
It does not depend on protocol infrastructure, providers, plugins or adapters.
Specialized provider-aware text reading, holdback and placeholders are private
[example support](examples/support/README.md), not another SDK.
Billing policies, priced tallies, reservations and settlement live in private
[example support](examples/README.md#billing-policies), not in a ledger SDK or
public accounting plugin. Generic `Usage` remains in the SDK; applications own
prices, accounts and durable billing stores. Routing takes an explicit retry classifier, with HTTP classification
available from the HTTP protocol module. A future first-party PyO3 embedding adapter will
live at `adapters/python/`; it is not implemented yet.

The HTTP protocol module's `reqwest` feature enables `HttpClient`. The Axum adapter's
`ws` feature enables its `ws` hosting module. Both are opt-in, so codec authors
and ordinary HTTP hosts do not need unrelated hosting or transport code.

HTTP formats prepare actual requests and event sinks synchronously, then let
core drive delivery. Response readers return streams. Finite inputs are encoded
before sending; untouched lazy inputs can relay raw bytes without decoding.
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

`gateway/tests/e2e.rs` starts the built binary on a free
port with a temp config, and talks real HTTP to it and to fake providers
on localhost from [mockllm](https://github.com/lihe07/mockllm), which
check each call by the real provider's rules and answer in its exact
framing, streams and breaks included. They run with the rest, in about a
second. The ignored ones are known calls the gateway makes that the
provider refuses:

```bash
cargo test -p respan-gateway --test e2e
# A couple of cheap calls to real providers, skipped for a missing key:
OPENAI_API_KEY=sk-... ANTHROPIC_API_KEY=sk-ant-... \
  cargo test -p respan-gateway --test real_providers -- --ignored
```

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option.
