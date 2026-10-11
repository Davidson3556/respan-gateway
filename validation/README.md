# External validation

This independent Cargo package is excluded from the main workspace. Tests that
need real provider APIs, downloaded reference servers or optional system tools
belong here. Normal workspace tests, including `--all-targets --all-features`,
cannot select them.

| Target | Tests | Requirements |
| --- | ---: | --- |
| `live_providers` | 7 | Provider keys and outbound network access |
| `live_ws` | 4 | OpenAI/Gemini keys, model access and outbound TLS connections |
| `reference_server` | 2 | Node.js, npx and network access to download the pinned MCP reference server |
| `pdf` | 1 | Poppler's `pdftotext` on PATH |

All cases remain ignored to require an explicit selection. From the repository
root, choose a target and optionally an individual test name:

```bash
cargo test --manifest-path validation/Cargo.toml --test live_providers -- --ignored
cargo test --manifest-path validation/Cargo.toml --test live_ws -- --ignored
cargo test --manifest-path validation/Cargo.toml --test reference_server -- --ignored
cargo test --manifest-path validation/Cargo.toml --test pdf -- --ignored
```

Live requests may incur provider charges. Required keys and optional endpoint
or model overrides are described in each test source. A test that skips for a
missing key does not establish provider interoperability.

The gateway binary target builds the same source as `apps/gateway/`. Shared
gateway and WebSocket harnesses are included from their owning packages. The PDF
test uses the same converter implementation as the attachments example.

To check compilation without executing validation:

```bash
cargo test --manifest-path validation/Cargo.toml --tests --no-run
```

This package has its own lockfile. Keep shared dependency versions aligned with
the root workspace when updating them.
