# respan-gateway

An LLM gateway with a neutral core and compiled-in plugins. The goal is to be
for LLM traffic what Nginx is for HTTP: a small, fast, predictable core, with
everything else built as modules on one public API.

> **Status: pre-alpha.** Relays OpenAI and Anthropic chat traffic and
> translates text between them (no tools or images yet). APIs will change.

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
ones hold the version, such as `https://api.openai.com/v1`,
`https://api.groq.com/openai/v1` or
`https://generativelanguage.googleapis.com/v1beta/openai`; Anthropic's does
not: `https://api.anthropic.com`.

## Roadmap

Goal for 0.1: all 14 [use cases](docs/use-cases.md) pass as integration
tests, and a 10-minute plugin really takes 10 minutes. Each use case becomes a
test when its milestone lands. The kernel API is frozen after M2.

| Milestone | Delivers |
|---|---|
| M1 Relay kernel | Plugin chain, byte-for-byte relay, response edits, kinds, OpenAI and Anthropic with correct usage, text translation both ways, model router, HTTP adapter, YAML config, `check` (done) |
| M2 Plugin API | More of the provider edge (waiting, signing, calls from it), async stream filters, reading provider responses, the shared store; API fixes before the freeze |
| M3 Formats | Gemini, Responses, embeddings |
| M4 Plugins | Fallback, hedge, limits, memo, ledger |
| M5 Translation | Tool calls, images and reasoning across formats |
| M6 Hard cases | Sessions and Realtime, jobs, required coverage, PII rewriting, admission |
| M7 0.1 release | Docs, examples, benchmarks, Redis store |
| M8 MCP gateway | `mcp` kind, relay, aggregation, per-key access |
| M9 Agent loop | Gateway-run MCP tools inside chat and Responses requests |

## Development

See [`CONTRIBUTING.md`](CONTRIBUTING.md).

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option.
