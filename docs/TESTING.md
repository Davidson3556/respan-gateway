# Testing

Run these checks from the repository root. They match CI and exercise all
Rust targets and optional features. The default suite uses fakes written in code and
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
| Provider compatibility | Native relay and translation among OpenAI Chat, Anthropic Messages, Gemini `generateContent` and Responses, tools, reasoning controls, errors and usage; realtime relay and OpenAI Realtime/Gemini Live translation | `crates/formats/conversation-http/tests/{relay,translate,round_trips,gemini,responses,reasoning}`, `crates/formats/realtime-ws/tests/{relay,translate,fakes}`, format unit tests; each vendor's quirks in `crates/vendors/tests/<vendor>.rs` |
| Hosting and WebSocket sessions | HTTP ingress, upgrade, subprotocols, session events, backpressure, concurrency and close/recovery behavior | `crates/wires/ws/tests/`, `crates/serve/tests/`, WS unit tests |
| MCP behavior | JSON-RPC messages, session state, streamable HTTP, stdio and reference-server interoperability | `crates/formats/mcp-http/tests/`, MCP unit tests |
| Routing policy | Attempts, retries, fallback, races and explicit retry classification | `crates/plugins/fallback/tests/fallback.rs`, `crates/plugins/routing/tests/routing.rs`, unit tests |
| Application behavior | Configuration and vendors, zero-config start, CLI/proxy modes, routing (media kinds and video job ids included), client keys, `/v1/models`, the admin API and UI hosting, in-process library use, path matching, redirects, error limits and graceful shutdown of the built executable | `apps/gateway/tests/`, gateway unit tests |
| Use cases | Billing, guardrails, PII, admission, durable jobs, attachments, routing and realtime policies composed through public APIs | `examples/tests/cases/`, `examples/tests/text_*.rs`, example library tests |
| Performance | Timing of whole and streamed in-process relay | `crates/formats/conversation-http/benches/relay.rs` |
| Documentation examples | Compilation or execution of Rust examples in API documentation | Rust doctests |

Security and resilience are cross-cutting concerns. Credential isolation and
path bypass tests belong with their owning HTTP, adapter or application suite;
cancellation, backpressure, resource cleanup and panic tests likewise span
several layers. Local fake-provider tests establish behavior against fakes;
live tests establish interoperability with the selected remote service.

### Execution requirements

The main workspace has one opt-in local benchmark besides its tests, and
doctests are a separate run. Even `--workspace --all-targets --all-features`
does not include the separate [`validation/`](../validation/README.md)
package.

| Run category | Location and requirements |
| --- | --- |
| Default local tests | Main workspace: in-process fakes, localhost HTTP/WebSocket servers, local child processes and temporary storage; no provider credentials or external services |
| Web UI tests | `apps/ui`, vitest under jsdom: `pnpm test`; needs Node and `pnpm install` |
| Python tests | `python/tests`, pytest against the built extension: `maturin develop` then `pytest`, from `python/` |
| Timing benchmark | Main workspace: explicitly run optimized in-process relay benchmark |
| Live provider tests | Separate validation package: provider credentials and network access |
| External-tool tests | Separate validation package: MCP tests using Node/npx and network access; a PDF extraction test using Poppler |

CI runs the Rust checks, the UI checks (`pnpm lint`, `typecheck`, `test`,
`build`), the Python tests, and compiles `validation/` (`cargo check --tests
--bins`) without running its tests.

The local suite uses `mockllm` for scripted OpenAI, Anthropic, Realtime and
Gemini Live behavior, and strict in-code fakes of OpenAI Realtime and Gemini
Live (`tests/support/realtime/`: voice detection, barge-in, G.711,
transcripts, and what each API rejects) for translated sessions. These fakes
run locally; they do not call real providers.
Other suites use smaller in-process transports, handcrafted socket fakes and
scripted stdio children. Standard local utilities such as `sh`, `sleep` and
`kill` are needed by process tests. Building from a cold cache may fetch Cargo
dependencies, including the pinned `mockllm` dependency.

Test counts are not recorded here; they change with every commit. Included
modules can run in several targets, so a count of test instances is not a
count of unique tests.

To refresh the inventory without executing test bodies:

```bash
cargo test --workspace --all-targets --all-features -- --list
```

## Suite ownership

| Location | Coverage |
| --- | --- |
| `crates/*/src/` | Unit tests for private implementation details |
| `crates/*/tests/` | Public contracts and protocol integration tests |
| `crates/formats/conversation-http/tests/{relay,translate}/` | Provider suites grouped by requests, responses, filters, errors and related behavior; shared fakes remain in each parent test target |
| `apps/gateway/tests/` | The built executable against mockllm: `e2e` (chat), `e2e_native` (Gemini `generateContent` and Responses), `e2e_media` (media kinds), `e2e_mcp` (MCP servers), `e2e_mcp_aggregate` (the aggregate MCP endpoint), `e2e_tool_loop` (the tool loop), `reload` (config reload); the command line (`cli`: init, check, models, proxy, `PORT`, the non-loopback guard); the admin API's config view and edits (`admin_config`), the request history file (`history`); browser WebSocket keys (`ws_auth`) and browser access (`cors`); the app and library in process (`app`, `library`); the media kinds against a strict fake of OpenAI's media endpoints and Veo (`media`); vendor quirks (`quirks`); `redirects`; bounded error bodies (`error_bodies`) |
| `crates/core/tests/` | The kernel: `buffering`, `cancellation`, `contract`, `edge` and `edge_runtime`, `faults`, `lifecycle`, `output_wrap` and `wrap`, `ownership`, `panic_matrix`, `progress`, `stress`, `symmetric`, `transports`, `until`, `upload` |
| `crates/plugins/*/tests/` | `tool_loop` (the tool loop), `fallback` (attempts, retries, fallback, races) and `routing` (model routing) |
| `crates/vendors/tests/` | `registry`, and one file per vendor's quirks: `azure`, `deepseek`, `gemini`, `groq`, `openai`, `openrouter`, `vllm` |
| `crates/serve/tests/` | `http`: HTTP ingress |
| `python/tests/` | `test_gateway`, `test_kinds`, `test_lifecycle`, `test_config_types`, `test_examples` (pytest against the built extension) |
| `apps/ui/src/**/*.test.ts(x)` | The web UI: components, hooks, pages and `lib/` (API client, export, filters, chat and realtime helpers), vitest under jsdom |
| `apps/gateway/tests/paths/` | Path matching, normalization, allowlists and real client path shapes, using one local wire fake |
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
cargo test -p respan-gateway --test cli
cargo test -p respan-gateway --test e2e_native
cargo test -p respan-gateway-conversation-http --test relay usage::
cargo test -p respan-gateway-conversation-http --test translate tools::
cargo test -p respan-gateway-conversation-http --test gemini
cargo test -p respan-gateway-conversation-http --test responses
cargo test -p respan-gateway-conversation-http --test reasoning
cargo test -p respan-gateway-examples --example wallet
cargo test -p respan-gateway-examples --test text_tool_json
```

## Protocol coverage

The shipped binary is tested against mockllm over real HTTP. WebSocket
tests also use mockllm's OpenAI Realtime and Gemini Live servers, and the
strict realtime fakes, over real sockets. MCP uses scripted HTTP responses, real stdio children, and an
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
OPENAI_API_KEY=sk-... ANTHROPIC_API_KEY=sk-ant-... GEMINI_API_KEY=... \
  cargo test --manifest-path validation/Cargo.toml --test live_providers -- --ignored
cargo test --manifest-path validation/Cargo.toml --test live_ws -- --ignored
# one test, one key:
OPENAI_API_KEY=sk-... cargo test --manifest-path validation/Cargo.toml \
  --test live_providers real_openai_embeddings -- --ignored
```

`live_providers` covers chat relay and translation (OpenAI, Anthropic,
Gemini `generateContent`, Responses, passthrough) and the media kinds
(`real_openai_embeddings`, `real_openai_image_generation`,
`real_openai_speech_then_transcription`, `real_gemini_embeddings`,
`real_gemini_image_generation`). Video has no live test; it is covered
against fakes. The gateway suite uses `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`,
`GEMINI_API_KEY` and `TYPESAFE_API_KEY` for its corresponding providers. The
WebSocket suite uses `OPENAI_API_KEY` and `GEMINI_API_KEY`. Set only the
credentials needed for the tests you select; individual test names can be
used as Cargo filters. Provider endpoint/model overrides are documented in
the test sources.

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
cargo bench -p respan-gateway-conversation-http --bench relay -- --ignored --nocapture
```

## Adding tests

Use behavior names for suites and modules. Keep tests with their owning crate,
share local fakes through its support modules, and split large suites into
topic modules under one integration target. Only local tests belong in the main
workspace. Put tests requiring live services or optional system tools in the
separate validation package, document their requirements, and keep them ignored
until explicitly selected.
