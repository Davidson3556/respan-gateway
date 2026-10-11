# Contributing

The project is pre-alpha, but its core is treated as half-frozen, like a
release candidate: changes to its contracts need a reason and tests.

Before proposing a change to the core, read
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), the source of truth for the
current architecture. Architectural changes must explain their rationale and
update that document and the relevant tests together.

Every change must pass the checks for the code it touches. Rust:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
cargo test --workspace --doc --all-features
```

The web UI (`apps/ui`, Node 22 and pnpm):

```bash
cd apps/ui
pnpm install --frozen-lockfile
pnpm lint && pnpm typecheck && pnpm test && pnpm build
```

Build the UI before the Rust binary if you want it embedded; without
`apps/ui/dist` the binary serves a placeholder page.

The Python package (`python/`):

```bash
cd python
maturin develop   # in a virtualenv with pytest, openai and anthropic
pytest
```

`validation/` is outside the workspace and CI only compiles it
(`cargo check --manifest-path validation/Cargo.toml --tests --bins`).

See [testing](docs/TESTING.md) for suite ownership, focused commands, benchmarks,
and optional tests that require external services or tools.
