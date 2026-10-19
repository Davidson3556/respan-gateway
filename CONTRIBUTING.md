# Contributing

The project is pre-alpha, but its core (`crates/core`) is treated like a
release candidate: a change to its contracts needs a reason, tests, and an
update to the docs that describe it. The other crates, the binary and the
Python package may still change freely.

## Architecture

The crates are in layers. Each depends only on the layers above it:

```
            core                 the kernel and the plugin API; no HTTP, no vendors
           /    \
      kinds      wires           typed events per kind · bytes at the edges, no kind
           \    /
          formats                a kind over a wire: one spec each, naming no vendor
             |
          vendors                each vendor's URLs, formats and quirks
             |
   plugins · serve · apps/gateway · python · examples
```

The rules that keep it that way:

- **Core knows no protocol.** No HTTP, provider, credential or message type.
  It runs plugins and moves typed events with backpressure.
- **A format is a spec, not a vendor.** What a vendor does beyond the spec
  is its *dialect*, in `crates/vendors`, or data such as a key header. It is
  never a plugin, and never a branch in a format.
- **Plugins are compiled in** and use only public APIs. The binary composes
  its plugins exactly as an outside author would. See [PLUGINS.md](PLUGINS.md).
- **Policy belongs to applications.** Prices, billing stores, durable jobs
  and tenant rules are the host's. `examples/` shows them without making them
  supported APIs.

Each crate's `README.md` documents its public contract.

| Path | What it is |
|---|---|
| `crates/core/` | The kernel and the plugin API: typed event streams, hooks, lifecycles, faults |
| `crates/kinds/` | Event types per kind: `conversation`, `media`, `decision`, `realtime`, `mcp` |
| `crates/wires/` | Bytes at both edges: HTTP, SSE, WebSocket, the Reqwest transport |
| `crates/formats/` | A kind over a wire, one spec each, naming no vendor |
| `crates/vendors/` | Every vendor, a file each, in `green/` or `yellow/` |
| `crates/plugins/` | First-party plugins: routing, tool loop, pricing, Respan traces, … |
| `crates/sdk/` | What plugins, vendors and hosts share: model cards and prices |
| `crates/serve/` | Hosting: one Axum router for HTTP and WebSocket |
| `apps/gateway/` | The binary: config, composition, client keys, request log, admin API |
| `apps/ui/` | The web UI (React), embedded in the binary |
| `python/` | The PyPI package, a PyO3 extension over `apps/gateway` |
| `examples/` | Runnable policies: billing, guardrails, PII, admission, jobs, … |
| `docs/` | User docs: configuration and MCP |

## Development

With [`just`](https://github.com/casey/just): `just dev` runs the gateway and
the UI with hot reload together (Ctrl-C stops both), `just run` builds the UI
and runs the gateway serving it, `just mock` runs the UI against a fake
gateway, and `just check` runs every check below except the Python
package's, which need a virtualenv. Run `just` with no arguments to list
them all.

## Checks

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

## Tests

The tests are unit tests, in `#[cfg(test)]` modules beside the code they
test, and the Python package's `pytest` suite. `just check` runs the Rust
and UI ones. They need no keys or network: where a test needs a provider,
it is [`mockllm`](https://github.com/lihe07/mockllm), a scripted fake of
the OpenAI, Anthropic and Gemini APIs, over a real socket.

Focused runs:

```bash
cargo test -p respan-gateway --lib compose::
cargo test -p respan-gateway-conversation-http --lib anthropic::
```

Put a new test beside the code that owns the behavior, and name it for the
behavior. A test must not need keys or the network.

## Releasing

Tag `vX.Y.Z` on `main` and push the tag. Two workflows run (and call `ci.yml`):

- `release.yml` first checks that the tag equals the `Cargo.toml` workspace
  version and runs `ci.yml`; nothing is published if either fails. It builds
  the binary (web UI embedded) for Linux x86_64 and aarch64 (native runners),
  macOS aarch64 and x86_64, and Windows x86_64, and attaches them with a
  `SHA256SUMS` file to a GitHub release.
- `python.yml` builds the UI, then abi3 wheels (Linux, macOS, Windows; UI
  embedded) and the sdist (no UI: it builds with the placeholder page), tests
  the wheels, and, once `ci.yml` passes too, publishes them to PyPI.

PyPI uses trusted publishing, so no token is stored. **One-time setup by the
owner**, before the first tag: on pypi.org, add a *pending publisher* for the
project `respan-gateway` (Account settings, Publishing) with the owner and
repository name, workflow `python.yml`, and environment `pypi`; then create
the `pypi` environment in the repository's settings (optionally requiring a
reviewer). Keep the version in `Cargo.toml` and the tag in step.
