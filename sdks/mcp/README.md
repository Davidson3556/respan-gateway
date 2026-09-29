# respan-gateway-mcp

The Model Context Protocol on the gateway. Messages stay JSON-RPC values
that keep their bytes, so an unedited message relays byte for byte.

- `ToServer` / `ToClient`: the kind, a client's request and a server's
  message.
- `StreamableHttp::new(path)`: MCP over streamable HTTP as a `Codec` made a `Format` by `dialect!`;
  `SESSION` is its session header.
- `StdioTransport::new(program, args)`: an MCP server run as a child
  process per session, an `http::Transport`. `.max_sessions(n)` per owner (default 8),
  `.max_total(n)` in all (default 64), `.idle(d)`, `.timeout(d)` for a
  `POST`'s answers (default 5 minutes; then a `TIMED_OUT` error). The owner is the call's
  `Owner` extension, else the client's `Authorization`, as the HTTP
  adapter lifted it (`respan_gateway_http::Credential`): authenticate
  first. Its failures are faults the client's format writes:
  `NO_SESSION` (404), `BUSY` (409), `TOO_MANY_SESSIONS` (503).
- `error(id, code, message)`: a JSON-RPC error.

Used by: [mcp_proxy](../../examples/mcp_proxy.rs).
