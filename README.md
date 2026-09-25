# respan-gateway

An LLM gateway with a neutral core and compiled-in plugins. The goal is to be
for LLM traffic what Nginx is for HTTP: a small, fast, predictable core, with
everything else built as modules on one public API.

> **Status: pre-alpha.** The architecture is still being designed. Nothing
> here is usable yet.

## Ground rules

1. **The core is neutral.** Routing, billing, guardrails, PII redaction,
   caching and logging are plugins. A feature nobody has thought of yet must be
   writable with the same API as the built-in ones.
2. **Plugins are compiled in.** No dynamic loading of any kind. A custom
   gateway is your own binary that depends on these crates.

Details and reasoning: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and
[`docs/decisions/`](docs/decisions/).

## Layout

| Crate | What it is |
|---|---|
| `crates/core` | The kernel: Turn, Part, Plugin, Store, Format, Policy |
| `crates/sdk` | Optional helpers, built only on the public kernel API |
| `crates/formats` | Wire formats: OpenAI, Anthropic, Gemini, YAML generic |
| `crates/store` | Store backends: memory, Redis |
| `crates/server` | HTTP adapter; the kernel itself has no server |
| `crates/testkit` | Mock providers and golden streams for plugin authors |
| `crates/respan-gateway` | The binary: registers compiled-in plugins and formats |

## Development

```bash
cargo fmt --all
cargo clippy --workspace --all-targets
cargo test --workspace
```

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option.
