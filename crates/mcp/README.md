# respan-gateway-mcp

The MCP HTTP adapter carries typed JSON-RPC messages from
[`respan-gateway-mcp-messages`](../mcp-messages/README.md).

`HttpInput` wraps input messages with HTTP method, batch flag, session,
protocol version, resume cursor and query. `HttpOutput` wraps output messages
with session, SSE event ID and event name. Keeping those fields on the same
envelope preserves their association when a plugin buffers, edits or drops an
event. SSE comments are dropped; ID-only events retain their cursor.

Import `HttpInput` / `HttpOutput` from this crate for HTTP gateway registration
and filtering. Inspect each envelope's `messages` as typed JSON-RPC messages.

- `StreamableHttp::new(path)`: MCP over streamable HTTP as direct
  `Format<Incoming, HttpInput, HttpOutput>` and reversed `Format<Outgoing, HttpOutput, HttpInput>` bindings;
  `SESSION` is its session header. The request shapes the answer: a `POST`
  with no request gets 202 and no body, one with requests an event stream
  (one JSON body if its `Accept` takes no event stream), a `GET` the
  server's own event stream, a `DELETE` 204.
  JSON-only answers contain responses; notifications are omitted, and
  server requests requiring bidirectional interaction are refused. Accept
  `text/event-stream` to receive those independent server messages.
- `StdioTransport::new(program, args)`: an MCP server run as a child
  process per session. `send(Request<Body>)` returns a native response and
  optional write completion; `.connect()` supplies the provider factory.
  Its limits, each with a documented default and reason: `.max_sessions(n)`
  per owner (8), `.max_total(n)` in all (64), `.idle(d)` (10 minutes; an
  open `GET` stream counts as activity), `.timeout(d)` for a `POST`'s
  answers (5 minutes; then the child is sent `notifications/cancelled`),
  `.backlog(n)` messages a stream may fall behind (64), `.max_line(bytes)`
  of one message from the child (32 MiB). The owner is the call's `Owner`
  extension, else the client's `Authorization`, as the HTTP adapter lifted
  it (`respan_gateway_http::Credential`): authenticate first. Its failures
  are faults the client's format writes, with their status, `error_code`
  and JSON-RPC code: no session named (400) or none there (404),
  `no_session`, -32003; a second `GET` (409), `busy`, -32004; past a limit
  (503), `too_many_sessions`, -32005; not answered in time (504),
  `timed_out`, -32001.
- `RpcCode`: the JSON-RPC error code a fault is written with, as a fact.
  A server's non-success answer whose body is a JSON-RPC error is read back
  into one: its code, message and `data` (as `Details`) reach the client's
  error, with the HTTP status kept; any other body gives -32603.
- `error(id, code, message)`: a JSON-RPC error.

Used by: [mcp_proxy](../../examples/integrations/mcp_proxy.rs).

Messages are validated as JSON-RPC envelopes before they can settle a
request. Invalid HTTP provider messages fail `upstream`; stray or malformed
stdio messages are ignored. A 202/204/205 cannot settle a posted request
that still needs a response. Request IDs must be strings or integers;
null IDs are reserved for error responses. An HTTP refusal may omit its
ID and still preserve its JSON-RPC error code and message.
Duplicate request IDs in a batch, and overlapping IDs in a stdio session,
are rejected before forwarding. A stdio backlog of zero is a configuration
fault rather than a channel panic.
Clients must still use unique request IDs across the entire session,
including completed requests; the gateway does not retain an unbounded
history of completed IDs.
Session IDs must contain visible ASCII, and scalar metadata headers must
appear once. Unicode resume cursors are preserved as UTF-8 in
`Last-Event-ID`; SSE ID resets and ignored NUL-containing ID fields follow
the SSE rules.
POST replies are matched to the IDs actually sent, including plugin
rewrites. A POST completes after its last reply without waiting for the
server to close SSE. Unexpected or duplicate replies before completion
fail upstream. GET resumption can receive replies from earlier POSTs.

JSON-only clients and JSON-RPC batches remain supported for compatibility.
MCP 2025-06-18 clients send one message per POST and advertise both JSON
and SSE; batch support is an extension for earlier clients.

The opt-in tests use the pinned official MCP server over stdio and HTTP:

```bash
cargo test --manifest-path validation/Cargo.toml --test reference_server -- --ignored
```

They require Node.js/npm and download `@modelcontextprotocol/server-everything`.
The HTTP check launches the resolved Node entry point directly, then kills
and reaps it after the test.
