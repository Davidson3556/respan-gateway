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
- `sdks/*` contains reusable mechanisms and shared message types, not production
  `Plugin` implementations. SDK production dependencies must not point to
  providers, plugins, adapters or the binary.
- `sdks/messages/` owns HTTP-free shared types; `sdks/formats/` owns HTTP-aware
  codec helpers. Routing orchestration takes caller-supplied retry classification.
- `plugins/` contains concrete first-party policies, including model routing and
  accounting; `providers/` contains concrete provider codecs. Both use public
  core/SDK APIs, just like external extensions. Application-specific plugins
  belong in applications or examples.
- `adapters/*` hosts the gateway in an environment: Axum today, a planned PyO3
  adapter in `adapters/python/` later. Hosting must not leak back into SDKs.
- `gateway/` composes providers, plugins and adapters through CLI configuration.
  `testkit/` supplies public-API testing helpers. Test-only dependencies may
  use concrete implementations without reversing production dependencies.
- `examples/` demonstrates integrations, including application-specific billing.
  Every example has tests (`examples/tests/<name>.rs`) that `cargo test` runs.

## Commands

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
