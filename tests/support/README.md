# Shared test support

These private source modules are included with `#[path]` by tests in multiple
workspace packages. This directory is not a Cargo package or a test target.

- `http.rs` constructs requests, drives HTTP gateway exchanges and collects
  bodies. Runnable examples also use these helpers with local providers.
- `hooks.rs` records plugin calls and lifecycle outcomes.
- `realtime/` holds strict fakes of OpenAI Realtime and Gemini Live, each a
  WebSocket server a test scripts (`Fake::answer`, `user_speaks`,
  `user_stops`, `go_away`) that rejects what its API rejects and records
  every frame and handshake. Their own tests are `realtime-ws`'s
  `tests/fakes.rs`.

Keep helpers here only when multiple packages need them. Helpers specific to
one crate belong in that crate's `tests/support/`. Gateway path wire fixtures
live in `apps/gateway/tests/paths/support.rs`; private example helpers live in
`examples/support/`.

Changes to a shared module are checked through all consumers by the workspace
commands in [CONTRIBUTING](../../CONTRIBUTING.md#checks).
