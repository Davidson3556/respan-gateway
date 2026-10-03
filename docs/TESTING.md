# Validation

The core and its public interfaces are unchanged by this protocol audit.
Run the offline checks with:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
cargo test --workspace --doc
python3 -m unittest discover -s scripts/tests
```

The shipped binary is tested against mockllm over real HTTP. WebSocket
tests also use mockllm's OpenAI Realtime and Gemini Live servers over real
sockets. MCP uses scripted HTTP responses, real stdio children, and an
LLM tool cycle through the MCP example.

Added scenarios include:

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

## Live tests

Live tests are ignored by Cargo by default. Explicit runs make small,
charge-incurring requests:

```bash
python3 scripts/test_live.py anthropic gemini typesafe respan mcp
python3 scripts/test_live.py openai
```

The runner reads environment variables first, then `ant_key`, `gemini_key`,
`typesafe_key`, `openai_key`, or `respan_key` in `--keys-dir` (default: the
home directory). It passes keys through child environments, redacts keys
from output, and fails for missing keys rather than silently skipping.
Each invocation selects one exact test and fails if Cargo selects none.
`respan_key` can contain `OPENAI API KEY = ...` and `OPENAI BASE = ...`.
Keys and generated configs containing keys are not saved in the repository.

Model overrides are `ANTHROPIC_MODEL`, `GEMINI_MODEL`, `GEMINI_LIVE_MODEL`,
`OPENAI_MODEL`, `OPENAI_REALTIME_MODEL`, and `RESPAN_MODEL`. The HTTP
OpenAI-compatible tests accept `OPENAI_BASE_URL`; Respan uses
`RESPAN_BASE_URL`. Anthropic's tool-stream test accepts `ANTHROPIC_BASE_URL`.
The MCP check requires Node.js/npm and downloads the pinned official
`@modelcontextprotocol/server-everything@2026.8.31` reference server.

Observed on 2026-10-03 with the supplied credentials:

| Provider | Verified behavior |
| --- | --- |
| Anthropic | Text streaming and forced Unicode tool arguments translated into OpenAI chunks |
| Gemini | Native HTTP SSE, plus Live audio and transcription through the WebSocket gateway over TLS |
| TypeSafe | Native System One decision response through passthrough, including answer and usage fields |
| Respan | Whole chat and native streamed Unicode tool calls through passthrough |
| Official MCP reference server | Initialize, initialized notification, list tools, Unicode echo, delete session over stdio and real HTTP |
| Direct OpenAI | Supplied key rejected with HTTP 401; direct Chat/Realtime interoperability remains unverified |

Gemini Live's real server sends binary JSON, so its transport must enable
`WsTransport::with_binary_text(true)`. Outgoing and client-facing messages
remain text; the existing text-only default remains available.

Gemini HTTP intermittently returned provider HTTP 503 across repeat runs;
both HTTP and Live passed on a separate rerun. The latest combined run
passed every other selected case, including Gemini Live, and reported
the HTTP failure. The live checks do not silently retry provider failures.
Separate direct HTTP/1.1 and HTTP/2 requests both succeeded; a persistent
HTTP/2 failure was not reproduced.

The Respan OpenRouter stream checked here ended with a finish reason and
usage but no `[DONE]`, confirmed by a direct call. The OpenAI chat codec
therefore reports `truncated`, as its protocol requires. Passthrough
preserves that vendor's native stream and was verified successfully; do
not treat the native pass as verification of OpenAI framing. No completion
sentinel is synthesized to conceal an incomplete provider stream.

Protocol sources: [WebSocket handshake negotiation](https://www.rfc-editor.org/rfc/rfc6455#section-4.2.2),
[JSON-RPC envelopes](https://www.jsonrpc.org/specification),
[MCP transports](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports),
[OpenAI Realtime conversations](https://developers.openai.com/api/docs/guides/realtime-conversations#text-inputs-and-outputs),
[Gemini Live messages](https://ai.google.dev/gemini-api/docs/live-api/get-started-websocket),
and [TypeSafe's API](https://docs.typesafe.ai/api).
