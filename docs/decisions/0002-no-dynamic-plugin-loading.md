# 0002. No dynamic plugin loading

- Status: accepted
- Date: 2026-09-24

## Context

Plugin systems often load code at runtime: shared libraries, WASM modules or
embedded scripts. Each brings an ABI or sandbox to maintain, a new attack
surface, and behaviour that can only be checked once the process is running.

## Decision

Every plugin is compiled into the binary. The following are out of scope and
will not be added later:

- shared-library plugins (`dlopen`, `libloading`, `.so` / `.dylib`)
- WASM plugin runtimes
- embedded scripting languages used as a plugin mechanism
- any stable-ABI plugin interface

Choosing and ordering compiled-in plugins from config at startup is allowed.
Dynamic dispatch over a fixed registry is not dynamic loading.

## Consequences

- A custom gateway is a small `main.rs` that depends on these crates and
  registers its plugins explicitly.
- A config that names a plugin the binary does not contain fails at startup.
- A config can be validated offline against the exact binary that will run it.
- The plugin API is a Rust API with semver, not an ABI.
