# CLAUDE.md

respan-gateway: an open-source LLM gateway in Rust. Pre-alpha; the architecture
is being designed.

## Hard rules

- Read `docs/ARCHITECTURE.md` and `docs/decisions/` before changing the core.
- The core is feature-neutral (0001). Never add a trait or type named after a
  feature to the core crate.
- No dynamic plugin loading of any kind (0002). Do not propose it as an option.
- A change that contradicts an accepted decision needs a new decision record.

## Layout

- `crates/core` is the kernel. Nothing feature-specific goes here.
- `crates/sdk` may use only the public API of `core`. If a helper needs
  private access, the kernel API is missing something; fix that instead.
- Formats, stores, the server adapter and the binary are separate crates.

## Commands

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
