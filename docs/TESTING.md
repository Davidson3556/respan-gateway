# Testing

Run these checks from the repository root. They match CI and exercise all
Rust targets and optional features. The default suite uses local fixtures and
requires no provider credentials; fetching build dependencies may need network
access:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
cargo test --workspace --doc --all-features
```

## Test categories

Classify tests by purpose and execution requirements independently. A test's
Cargo target describes how it is compiled, not whether it uses sockets,
processes or several components.

### Purpose

| Category | What it establishes | Main suites |
| --- | --- | --- |
| Kernel contracts | Hook ordering, ownership, cancellation, buffering, panic handling and progress | `crates/core/tests/`, core unit tests |
| Shared data contracts | Input assembly, text access and message representation | Input unit tests |
| HTTP behavior | Encoding, framing, credentials, headers, retry hints, body handling and stream completion | `crates/wires/http/tests/`, HTTP unit tests |
| Provider compatibility | Native relay, OpenAI/Anthropic translation, tools, errors and usage | `crates/formats/conversation-http/tests/{relay,translate,round_trips}`, format unit tests; each vendor's quirks in `crates/providers/tests/<vendor>.rs` |
| Hosting and WebSocket sessions | HTTP ingress, upgrade, subprotocols, session events, backpressure, concurrency and close/recovery behavior | `crates/wires/ws/tests/`, `crates/wires/http/tests/axum.rs`, WS unit tests |
| MCP behavior | JSON-RPC messages, session state, streamable HTTP, stdio and reference-server interoperability | `crates/formats/mcp-http/tests/`, MCP unit tests |
| Routing policy | Attempts, retries, fallback, races and explicit retry classification | `crates/plugins/tests/routing.rs`, routing unit tests |
| Application behavior | Configuration and presets, zero-config start, CLI/proxy modes, routing (media kinds and video job ids included), client keys, `/v1/models`, the admin API and UI hosting, in-process library use, path matching, redirects, error limits and graceful shutdown of the built executable | `apps/gateway/tests/`, gateway unit tests |
| Use cases | Billing, guardrails, PII, admission, durable jobs, attachments, routing and realtime policies composed through public APIs | `examples/tests/cases/`, `examples/tests/text_*.rs`, example library tests |
| Performance | Timing of whole and streamed in-process relay | `crates/formats/conversation-http/benches/relay.rs` |
| Documentation examples | Compilation or execution of Rust examples in API documentation | Rust doctests |

Security and resilience are cross-cutting concerns. Credential isolation and
path bypass tests belong with their owning HTTP, adapter or application suite;
cancellation, backpressure, resource cleanup and panic tests likewise span
several layers. Local mock-provider tests establish behavior against fixtures;
live tests establish interoperability with the selected remote service.

### Execution requirements

The main workspace contains **1,357 local test instances** and one opt-in local
benchmark, excluding doctests. Even `--workspace --all-targets --all-features`
does not include the separate [`validation/`](../validation/README.md) package.

| Run category | Count | Location and requirements |
| --- | ---: | --- |
| Default local tests | 1,357 | Main workspace: in-process fixtures, localhost HTTP/WebSocket servers, local child processes and temporary storage; no provider credentials or external services |
| Timing benchmark | 1 | Main workspace: explicitly run optimized in-process relay benchmark |
| Live provider tests | 9 | Separate validation package: provider credentials and network access |
| External-tool tests | 3 | Separate validation package: 2 MCP tests using Node/npx and network access; 1 PDF extraction test using Poppler |

The local suite uses `mockllm` for scripted OpenAI, Anthropic, Realtime and
Gemini Live behavior. These mocks run locally; they do not call real providers.
Other suites use smaller in-process transports, handcrafted socket fixtures and
scripted stdio children. Standard local utilities such as `sh`, `sleep` and
`kill` are needed by process tests. Building from a cold cache may fetch Cargo
dependencies, including the pinned `mockllm` Git dependency.

Doctests are a separate local run: 9 runnable doctests and 5 ignored examples.
By Cargo harness, the workspace contains 241 tests in library or binary source
targets, 898 integration-target tests, 218 tests in runnable example targets,
and 1 benchmark-harness test. These are test instances rather than unique source
declarations: included modules can run in multiple targets. Source-target and
example-target tests can contain integration scenarios.

To refresh the inventory without executing test bodies:

```bash
cargo test --workspace --all-targets --all-features -- --list
```

## Suite ownership

| Location | Coverage |
| --- | --- |
| `crates/*/src/` | Unit tests for private implementation details |
| `crates/*/tests/` | Public contracts and protocol integration tests |
| `crates/formats/conversation-http/tests/{relay,translate}/` | Provider suites grouped by requests, responses, filters, errors and related behavior; shared fixtures remain in each parent test target |
| `apps/gateway/tests/` | Executable integration tests (`e2e`, `app`), the library in process (`library`), the media kinds in process against a strict fake of OpenAI's media endpoints and Veo (`media`), redirects and bounded error bodies |
| `apps/gateway/tests/paths/` | Path matching, normalization, allowlists and real client path shapes, using one local wire fixture |
| `examples/tests/cases/<topic>/` | Test modules included by the corresponding example or private support module |
| `examples/tests/text_*.rs` | Automatically discovered text and tool-argument integration suites |
| `tests/support/` | Source-only HTTP and lifecycle helpers shared across packages |
| `crates/formats/conversation-http/benches/` | Opt-in relay timing benchmark |

`examples/Cargo.toml` registers runnable examples with `test = true`. Their
included case modules run under those example targets. Keeping these files in
subdirectories prevents Cargo from compiling them as standalone integration
crates. The four top-level `text_*` files use normal Cargo test discovery.

Focused commands:

```bash
cargo test -p respan-gateway --test paths
cargo test -p respan-gateway-conversation --test relay usage::
cargo test -p respan-gateway-conversation --test translate tools::
cargo test -p respan-gateway-examples --example wallet
cargo test -p respan-gateway-examples --test text_tool_json
```

## Protocol coverage

The shipped binary is tested against mockllm over real HTTP. WebSocket
tests also use mockllm's OpenAI Realtime and Gemini Live servers over real
sockets. MCP uses scripted HTTP responses, real stdio children, and an
LLM tool cycle through the MCP example.

Coverage includes:

- WebSocket optional subprotocols, repeated protocol headers, duplicate
  accept headers, invalid subprotocol tokens, fragmented
  Unicode with an interleaved ping, aggregate message limits, opt-in binary
  UTF-8 normalization, invalid UTF-8, tool-call recovery and cut turns
  without replay.
- MCP malformed JSON-RPC envelopes, invalid replies that must not settle
  requests, bodiless replies to pending requests, every byte split of
  Unicode/multiline SSE, distinct numeric/string IDs, out-of-order replies,
  null/fractional request IDs, ID-less HTTP error refusals, stray JSON on
  stdio, and notification/server-request behavior for JSON versus SSE clients.
- Duplicate IDs within a batch or concurrent stdio requests, preserving
  the original pending request; invalid zero-capacity stdio configuration.
- Unicode HTTP resume cursors, bare SSE ID resets, ignored NUL-containing
  IDs, valid NUL event names, invalid provider session IDs and repeated
  scalar metadata headers.
- Foreign or duplicate provider replies, answered POSTs whose SSE socket
  remains open, and GET resumption of replies from earlier POSTs.
- An OpenAI-format mock client receiving an Anthropic tool call, invoking
  the allowed MCP tool, and returning its Unicode result with the original
  provider call ID.
- The MCP example's complete initialization handshake, protocol-version
  headers, notification handling and schema-correct echo arguments, checked
  against the official stdio server as well as the mock.

The 300-request HTTP bodyless sweeps cap concurrent requests at 64 and
bound their waits, so a typical 1024-file-descriptor limit does not hang
the suite.

## Live provider tests

These targets live in the separate validation package and are also ignored
until explicitly selected. Explicit runs make small requests that
can incur provider charges. Tests skip when their required key is absent, so
an explicit run without credentials does not prove live coverage.

```bash
cargo test --manifest-path validation/Cargo.toml --test live_providers -- --ignored
cargo test --manifest-path validation/Cargo.toml --test live_ws -- --ignored
```

The gateway suite uses `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY`
and `TYPESAFE_API_KEY` for its corresponding providers. The WebSocket suite
uses `OPENAI_API_KEY` and `GEMINI_API_KEY`. Set only the credentials needed for
the tests you select; individual test names can be used as Cargo filters.
Provider endpoint/model overrides are documented in the test sources.

## External tools

The MCP reference-server tests download and run Node-based reference servers;
they require `node`, `npx` and network access:

```bash
cargo test --manifest-path validation/Cargo.toml --test reference_server -- --ignored
```

The separate PDF extraction validation test requires Poppler's
`pdftotext` executable:

```bash
cargo test --manifest-path validation/Cargo.toml --test pdf -- --ignored
```

## Benchmarks

The relay timing benchmark is an ignored test harness in `benches/`, using
local in-process responses. Run it explicitly with optimized code:

```bash
cargo bench -p respan-gateway-conversation --bench relay -- --ignored --nocapture
```

## Adding tests

Use behavior names for suites and modules. Keep tests with their owning crate,
share local fixtures through its support modules, and split large suites into
topic modules under one integration target. Only local tests belong in the main
workspace. Put tests requiring live services or optional system tools in the
separate validation package, document their requirements, and keep them ignored
until explicitly selected.
