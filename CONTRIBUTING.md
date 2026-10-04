# Contributing

The project is pre-alpha; the architecture is being designed.

Before proposing a change to the core, read
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), the source of truth for the
current architecture. Architectural changes must explain their rationale and
update that document and the relevant tests together.

Every change must pass:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
cargo test --workspace --doc --all-features
```

See [testing](docs/TESTING.md) for suite ownership, focused commands, benchmarks,
and optional tests that require external services or tools.
