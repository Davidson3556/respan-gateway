# Contributing

The project is pre-alpha; the architecture is being designed.

Before proposing a change to the core, read
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), the source of truth for the
current architecture. Architectural changes must explain their rationale and
update that document and the relevant tests together.

Every change must pass:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
