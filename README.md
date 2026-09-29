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

A client sends a request, plugins look at it, the gateway sends it on to a
provider, and the response streams back through the plugins. A request
carries inputs, its response outputs. A streaming request, such as a
WebSocket (the HTTP adapter makes one of a request asking to switch
protocols), has its inputs stream after its head (`Exchange::streams`); any
other is whole: its body, even empty, is its one input. The kernel has no HTTP: the adapter,
transports and formats own its rules and heads
([0013](docs/decisions/0013-no-http-in-core.md)).

- **Call**: what the gateway sends a provider on a request's behalf
  (`cx.call`): the request's inputs, or its own (`with_input`). The
  target is the provider called when every plugin passes: the one
  `set_target` names, else the gateway's (`Gateway::target`).
- **Edge**: where hooks run: the client edge once per request
  (`on_client`), the provider edge once per call (`on_provider`). Each
  edge's end hook (`on_client_end`, `on_provider_end`) runs once after it
  ends, with how it ended (`&Result<(), Fault>`); a fact about a call, such
  as `is_sent`, is a method of its context. `on_client` passes (`Ok(None)`), answers, skipping the plugins
  after it, or refuses.
- **Filter**: edits a request's inputs or a response's outputs as they
  stream; its type says which. Filters see every message, keep-alives and
  usage alone included.
- **Format**: one protocol's codec, such as OpenAI Chat: it recognises
  its requests, and reads and writes its requests and responses.
- **Kind**: a format's two types, `Input` and `Output`. Formats of one
  kind translate into each other; traffic the gateway does not read is a
  kind of one format (`formats::opaque`).
- **Message**: one message of a body, such as an SSE event, a WebSocket
  frame or a whole body, decoded into one value of the kind, which keeps
  the bytes it came in.
- **Refusal**: a failure before the response head goes out, sent in the
  client's format with the fault's status (`Status` hint, else its
  code's default) and retry; a provider's error
  answer (`Fault::answer`) goes as it came to a client of its format.
- **Relay**: a format's promise to write each message as the bytes it
  came in while it says the same, an edit written into them. A whole
  request no plugin changed goes as the client's bytes, unread. A call
  that sends the client's own message to a provider of its format's name
  is `relayed` (`Exchange::relayed`, set by the kernel alone): only a
  relay carries the client's credential.
- **Usage**: not the kernel's
  ([0010](docs/decisions/0010-usage-out-of-core.md)). The chat kind
  reports it in `Reply.usage` (`formats::Usage`); a plugin meters a call
  with a provider-edge filter and reads the sum in `on_provider_end`
  (`examples/billing.rs`, `sdks/ledger`).

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
| `crates/core` | The kernel: plugins, calls, filters; no HTTP, config or kinds. |
| `crates/http` | HTTP at the edges: its head types, the adapter, credentials, what a transport strips. |
| `crates/formats` | The chat kind, OpenAI Chat and Anthropic Messages. |
| `crates/plugins` | First-party plugins, such as the model router. |
| `crates/server` | The axum router and the HTTP transport. |
| `crates/testkit` | A mock transport and the kernel's tests. |
| `crates/respan-gateway` | The binary: YAML config and `check`. |
| `sdks/` | Building blocks for plugins: `routing`, `text`, `ledger`, `mcp`. |
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
