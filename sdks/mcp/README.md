# respan-gateway-mcp

The Model Context Protocol on the gateway. Messages stay JSON-RPC values
that keep their bytes, so an unedited message relays byte for byte.

- `ToServer` / `ToClient`: the kind, a client's request and a server's
  message.
- `StreamableHttp::new(path)`: MCP over streamable HTTP as a `Format`;
  `SESSION` is its session header.
- `StdioTransport::new(program, args)`: an MCP server run as a child
  process per session. `.max_sessions(n)` per owner (default 8),
  `.max_total(n)` in all (default 64), `.idle(d)`. The owner is the call's
  `Owner` extension, else its `Authorization` header: authenticate first.
- `error(id, code, message)`: a JSON-RPC error.

Used by: [mcp_proxy](../../examples/mcp_proxy.rs).
