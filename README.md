# respan-gateway

An LLM gateway with a neutral core and compiled-in plugins. The goal is to be
for LLM traffic what Nginx is for HTTP: a small, fast, predictable core, with
everything else built as modules on one public API.

> **Status: pre-alpha.** Relays OpenAI and Anthropic chat traffic and
> translates text between them; tools and images come in M5. APIs will
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
else is a 404.

A provider's `base_url` is the one its own SDK takes. OpenAI-compatible
ones include the version, such as `https://api.openai.com/v1`,
`https://api.groq.com/openai/v1` or
`https://generativelanguage.googleapis.com/v1beta/openai`; Anthropic's does
not: `https://api.anthropic.com`.

## Glossary

In plain words: a client sends a request, plugins look at it, the gateway
sends it on to a provider, and the response streams back through the
plugins. A request carries inputs and its response carries outputs. A
request that asks to switch protocols (`Upgrade`), such as a WebSocket,
is upgrading: its inputs stream after its head. Any other request is
whole: its body, even empty, is its one input.

- **Call**: what the gateway sends a provider on a request's behalf
  (`cx.call`): the request's inputs, or its own (`with_input`). The
  target is the provider called when every plugin passes: the one
  `set_target` names, else the gateway's default (`Gateway::target`).
- **Edge**: where hooks run. The client edge runs once per request
  (`on_client`), the provider edge once per call (`on_provider`). Each
  edge's end hook (`on_client_end`, `on_provider_end`) runs once after
  its request or call ends. `on_client` passes (`Ok(None)`), or returns
  a response and skips the plugins after it, or refuses.
- **Filter**: code that edits a request's inputs or a response's
  outputs as they stream; its type says which. Filters see every
  message, keep-alives and usage alone included, and pass on at once what
  they have no reason to hold. `Format::is_content` says only what
  `until_content` commits on.
- **Format**: one protocol's codec, such as OpenAI Chat: it recognises
  its requests, and reads and writes its requests and responses.
- **Kind**: a format's two types, `Input` and `Output`. Formats of one
  kind translate into each other; traffic the gateway does not read is a
  kind of one format (`formats::opaque`).
- **Message**: one message of a body, such as an SSE event, a WebSocket
  frame or a whole body, decoded into one value of the kind: every one,
  even one the kind cannot read, keeps the bytes it came in.
- **Usage**: the kernel counts none, as kinds report it differently or
  not at all ([0010](docs/decisions/0010-usage-out-of-core.md)). In the
  chat kind it is `Reply.usage` (`formats::Usage`); a plugin meters a
  call with a provider-edge filter registered after the filters that may
  fail, and reads its sum in `on_provider_end`. A plugin's own answer
  reaches no provider edge: a cache carries its stored usage in an
  extension. A cost cap reserves before the call (`sdks/ledger`) and
  settles from what that filter priced.
- **Refusal**: a failure before the response head goes out, sent in the
  client's format with the fault's status.
- **Relay**: a format's promise to write each message as the bytes it
  came in while it says the same, an edit written into them: the client's
  inputs to a provider of that format, and the provider's response (or a
  plugin's own, `cx.relay`) back. The kernel sends a whole request no
  plugin changed as the client's bytes, unread.

## Roadmap

Goal for 0.1: all 14 [use cases](docs/use-cases.md) pass as integration
tests, and a 10-minute plugin really takes 10 minutes. Each use case becomes a
test when its milestone lands. The kernel API is frozen after M2.

| Milestone | Delivers |
|---|---|
| M1 Relay kernel | Plugin chain, byte-for-byte relay, response edits, kinds, OpenAI and Anthropic with correct usage, text translation both ways, model router, HTTP adapter, YAML config, `check` (done) |
| M2 Plugin API | Calls of their own `http::Request`, async stream filters, unread bodies; API fixes before the freeze |
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
| `crates/core` | The kernel: plugins, calls, filters; no HTTP, config or kinds. |
| `crates/formats` | The chat kind, OpenAI Chat and Anthropic Messages. |
| `crates/plugins` | First-party plugins, such as the model router. |
| `crates/server` | The HTTP adapter and transport. |
| `crates/testkit` | A mock transport and the kernel's tests. |
| `crates/respan-gateway` | The binary: YAML config and `check`. |
| `sdks/` | Building blocks for plugins: `routing`, `text`, `ledger`, `mcp`. |
| `examples/` | Runnable, tested examples of the [use cases](examples/README.md). |

## Development

See [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option.
