# Contributing

The project is pre-alpha; the architecture is being designed.

Before proposing a change to the core, read
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and the
[decision records](docs/decisions/). A change that contradicts an
accepted decision needs a new decision record.

Every change must pass:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
