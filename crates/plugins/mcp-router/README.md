# respan-gateway-plugin-mcp-router

`McpRouter` targets an MCP client's request to `/mcp/{name}` at the MCP
server `name` (a core provider of the `mcp` format): the last segment of the
request's cleaned path. It routes MCP servers, not models; the `routing`
plugin routes models.

A key limited to some tools (a request carrying a `ToolScope`) is refused
there with `403 tools_limited`: a server's own endpoint offers all its tools,
so such a key reaches them through `/mcp`, which holds it to them. Made with
`McpRouter::with_switches`, it refuses a server turned off in the shared
`ToolSwitches` with `403 tool_server_off` (a tool turned off is not: the
endpoint relays the server as it is).

```rust,ignore
use respan_gateway_plugin_mcp_aggregate::Aggregate;
use respan_gateway_plugin_mcp_router::McpRouter;

let gateway = gateway.plugin(aggregate).plugin(McpRouter::default());
```

Register it after the aggregate, which answers `/mcp` itself; before it, this
plugin would take `/mcp` for a server named `mcp`. Used by the binary for
every configured MCP server.
