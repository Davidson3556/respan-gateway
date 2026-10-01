# CLAUDE.md

respan-gateway: an open-source LLM gateway in Rust. Pre-alpha; the architecture
is being designed.

## Hard rules

- Read `docs/ARCHITECTURE.md` before changing the core.
- The core is feature-neutral. Never add a trait or type named after a
  feature to the core crate.
- No dynamic plugin loading of any kind. Do not propose it as an option.
- `docs/ARCHITECTURE.md` is the architectural source of truth. Update it and
  the relevant tests together when changing an architectural contract.

## Layout

- `core/` is the kernel. Nothing feature-specific goes here.
- Every other crate may use only the public API of `core`. If a helper needs
  private access, the kernel API is missing something; fix that instead.
- `sdk/` is the one plugin-author facade: public core contracts and HTTP-free
  shared LLM message types/basic accessors. Its production dependencies must not
  point to HTTP, providers, plugins, MCP, WebSockets, adapters or the binary.
- `http/` owns native HTTP bodies, owned incoming/outgoing bindings and
  stream/sink helpers at its crate root; framing/error helpers live in its public
  `formats` module. HTTP formats implement the core `Format` contract directly.
  `mcp/` and `ws/` own optional integrations.
- Routing attempt orchestration lives in `plugins::routing` with caller-supplied
  retry classification. Specialized text rewriting stays in private example support.
- `plugins/` contains concrete first-party policies such as model routing;
  `providers/` contains concrete provider codecs. Both use public core/SDK APIs,
  just like external extensions. Application-specific plugins belong in
  applications or examples. Billing, pricing, accounts and ledger mechanisms
  stay application-specific; usage types and lifecycle hooks remain reusable.
- `adapters/*` hosts the gateway in an environment: Axum today, a planned PyO3
  adapter in `adapters/python/` later. Hosting must not leak back into the SDK.
- `gateway/` composes providers, plugins and adapters through CLI configuration.
  Integration suites live with their owning crates; `tests/support/` holds
  private source-only helpers. Test-only dependencies may use concrete
  implementations without reversing production dependencies.
- `examples/` demonstrates integrations, including application-specific billing.
  Every example has tests (`examples/tests/<name>.rs`) that `cargo test` runs.

## Commands

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
