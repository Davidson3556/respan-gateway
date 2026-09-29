# respan-gateway

An LLM gateway with a neutral core and compiled-in plugins. The goal is to be
for LLM traffic what Nginx is for HTTP: a small, fast, predictable core, with
everything else built as modules on one public API.

> **Status: pre-alpha.** Relays OpenAI and Anthropic chat traffic and
> translates text and tools between them; images come in M5. APIs will
> change.

## Ground rules

1. **The core is neutral.** Routing, billing, guardrails, PII redaction,
   caching and logging are plugins. A feature nobody has thought of yet must be
   writable with the same API as the built-in ones.
2. **Plugins are compiled in.** No dynamic loading of any kind. A custom
   gateway is your own binary that depends on these crates.

Details and reasoning: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and
[`docs/decisions/`](docs/decisions/).

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
every other request to a vendor unread (see `gateway.example.yaml`).

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
the provider edge, each a format, own them
([architecture](docs/ARCHITECTURE.md),
[0020](docs/decisions/0020-symmetric-edges.md)).

- **Gateway**: what you build and hand requests to: providers, formats
  served, plugins and a default target, each registered by value.
- **Plugin**: four hooks, `on_client` and `on_client_end` once per
  request, `on_provider` and `on_provider_end` once per call, in
  registration order. `on_client` passes (`Ok(None)`), replies or refuses.
- **Filter**: edits a request's inputs or a response's outputs as they
  stream; its type says which. It sees every message.
- **Format**: one protocol's view of a kind, such as OpenAI Chat, and the
  two edges that speak it: it accepts a client's connection and dials a
  provider's. Its `Input` and `Output` types are its kind: formats of one
  name relay each message as the bytes it came in, formats of one kind
  translate. An HTTP format is a **Codec** (`crates/http`), one
  protocol's decoders and encoders, made a `Format` by `impl_format!`; it
  reaches a provider through a **Transport** (`http::Transport`), which
  owns the URL, the keys and the connection.
- **Call**: what the gateway sends a provider on a request's behalf
  (`cx.call`): the request's inputs, or its own (`with_input`). Only a call
  of the request's inputs to a provider of the client's format name is a
  relay, and carries the client's origin and key.
- **Response**: what a call, or `Response::stream`, gives back: read
  (`outputs`), held (`until_content`, `until_end`) or sent.
- **Fault**: the one error: a code that says whose fault it is, a message,
  a retry and typed hints such as HTTP's `Status`.

## Roadmap

Goal for 0.1: all 14 [use cases](docs/use-cases.md) pass as integration
tests, and a 10-minute plugin really takes 10 minutes. Each use case becomes a
test when its milestone lands. The kernel API is frozen after M2.

| Milestone | Delivers |
|---|---|
| M1 Relay kernel | Plugin chain, byte-for-byte relay, response edits, kinds, OpenAI and Anthropic, text translation both ways, model router, HTTP adapter, YAML config, `check` (done) |
| M2 Plugin API | Calls of their own input, async stream filters, unread bodies; API fixes before the freeze |
| M3 Formats | Gemini, Responses, embeddings |
| M4 Plugins | Fallback, hedge, limits, memo, ledger |
| M5 Translation | Tool calls, images and reasoning across formats |
| M6 Hard cases | Realtime relay and bridges, jobs, PII rewriting, admission |
| M7 0.1 release | Docs, examples, benchmarks |
| M8 MCP gateway | `mcp` kind, relay, aggregation, per-key access |
| M9 Agent loop | Gateway-run MCP tools inside chat and Responses requests |

## Crates

| Path | What it is |
|---|---|
| `crates/core` | The kernel: ten types, events only; no bytes, heads, HTTP or kinds. |
| `crates/http` | HTTP's wire: `Codec`, `Transport`, the driver, the adapter, credentials. |
| `crates/formats` | The chat kind, OpenAI Chat and Anthropic Messages. |
| `crates/plugins` | First-party plugins, such as the model router. |
| `crates/server` | The axum router and the HTTP transport. |
| `crates/testkit` | A mock transport and tests through the real formats. |
| `crates/respan-gateway` | The binary: YAML config, `check` and `proxy`. |
| `sdks/` | Building blocks for plugins: `routing`, `text`, `ledger`, `mcp`, `ws`. |
| `examples/` | Runnable, tested examples of the [use cases](examples/README.md). |

## Development

See [`CONTRIBUTING.md`](CONTRIBUTING.md).

### End-to-end tests

`crates/respan-gateway/tests/e2e.rs` starts the built binary on a free
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
