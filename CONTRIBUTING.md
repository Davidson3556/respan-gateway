# Contributing

The project is pre-alpha, but its core is treated as half-frozen, like a
release candidate (the other crates, the binary and the Python package
may still change): changes to its contracts need a reason and tests.

Before proposing a change to the core, read
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md), the source of truth for the
current architecture. Architectural changes must explain their rationale and
update that document and the relevant tests together.

With [`just`](https://github.com/casey/just): `just dev` runs the gateway and
the UI with hot reload together (Ctrl-C stops both), `just run` builds the UI
and runs the gateway serving it, `just mock` runs the UI against a fake
gateway, and `just check` runs the checks below but the Python package's,
which need a virtualenv. `just` lists them all.

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

Tag `vX.Y.Z` on `main` and push the tag. Two workflows run (and call `ci.yml`):

- `release.yml` first checks that the tag equals the `Cargo.toml` workspace
  version and runs `ci.yml`; nothing is published if either fails. It builds
  the binary (web UI embedded) for Linux x86_64 and aarch64 (native runners),
  macOS aarch64 and x86_64, and Windows x86_64, and attaches them with a
  `SHA256SUMS` file to a GitHub release. It also pushes the Docker image,
  `linux/amd64` and `linux/arm64`, to `ghcr.io/<owner>/respan-gateway` (tags
  `X.Y.Z`, `X.Y`, `latest`) with the built-in `GITHUB_TOKEN`: each
  architecture's image is assembled from its natively built binary
  (`--target prebuilt` of the `Dockerfile`; no emulation) and the two are
  merged into one manifest. After the first push, make the package public in
  the repository's package settings if it should be pullable without login.
- `python.yml` builds the UI, then abi3 wheels (Linux, macOS, Windows; UI
  embedded) and the sdist (no UI: it builds with the placeholder page), tests
  the wheels, and, once `ci.yml` passes too, publishes them to PyPI.

PyPI uses trusted publishing, so no token is stored. **One-time setup by the
owner**, before the first tag: on pypi.org, add a *pending publisher* for the
project `respan-gateway` (Account settings, Publishing) with the owner and
repository name, workflow `python.yml`, and environment `pypi`; then create
the `pypi` environment in the repository's settings (optionally requiring a
reviewer). Keep the version in `Cargo.toml` and the tag in step.
