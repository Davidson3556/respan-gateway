# Contributing

The project is pre-alpha, but its core is treated as half-frozen, like a
release candidate (the other crates, the binary and the Python package
may still change): changes to its contracts need a reason and tests.

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
`apps/ui/dist` the binary serves a placeholder page. `pnpm mock` runs the UI
against a fake gateway.

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

## Releasing

Tag `vX.Y.Z` on `main` and push the tag. Three workflows run:

- `release.yml` builds the binary (web UI embedded) for Linux x86_64 and
  aarch64 and macOS aarch64 and attaches them to a GitHub release, and pushes
  the Docker image, `linux/amd64` and `linux/arm64`, to
  `ghcr.io/<owner>/respan-gateway` (tags `X.Y.Z`, `X.Y`, `latest`) with the
  built-in `GITHUB_TOKEN`. After the first push, make the package public in
  the repository's package settings if it should be pullable without login.
- `python.yml` builds abi3 wheels (Linux, macOS, Windows) and the sdist,
  tests the wheels, and publishes them to PyPI.

PyPI uses trusted publishing, so no token is stored. **One-time setup by the
owner**, before the first tag: on pypi.org, add a *pending publisher* for the
project `respan-gateway` (Account settings, Publishing) with the owner and
repository name, workflow `python.yml`, and environment `pypi`; then create
the `pypi` environment in the repository's settings (optionally requiring a
reviewer). Keep the version in `Cargo.toml` and the tag in step.
