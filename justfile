# Run `just` to list the recipes.

ui := "apps/ui"

# List the recipes.
default:
    @just --list

# The gateway on :8080 and the UI with hot reload on http://localhost:5173/ui/. Ctrl-C stops both.
dev *args: ui-deps
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'kill 0' EXIT
    cargo run -p respan-gateway -- {{args}} &
    (cd {{ui}} && pnpm dev) &
    wait -n

# The UI with hot reload against a fake gateway; no Rust needed.
mock: ui-deps
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'kill 0' EXIT
    (cd {{ui}} && pnpm mock) &
    (cd {{ui}} && pnpm dev) &
    wait -n

# Build the UI, then run the gateway serving it at http://localhost:8080/ui/.
run *args: ui-build
    cargo run -p respan-gateway -- {{args}}

# Build the UI into apps/ui/dist, which the gateway embeds.
ui-build: ui-deps
    cd {{ui}} && pnpm build

# Install the UI's packages, if they are not yet.
ui-deps:
    @test -d {{ui}}/node_modules || (cd {{ui}} && pnpm install --frozen-lockfile)

# The checks CONTRIBUTING.md asks for: Rust formatting, lints and tests, `validation/` compiled, and the UI's.
check: ui-deps
    cargo fmt --all --check
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    cargo test --workspace --all-targets --all-features
    cargo test --workspace --doc --all-features
    cargo check --manifest-path validation/Cargo.toml --tests --bins
    cd {{ui}} && pnpm lint && pnpm typecheck && pnpm test && pnpm build
