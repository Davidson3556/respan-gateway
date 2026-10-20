# respan-gateway-plugin-mcp-aggregate

`Aggregate` answers the MCP format's requests to `/mcp` itself, fronting
several MCP servers (core providers of the `mcp` format) as one. A client
sees one server and one session; the gateway holds a session with each
server behind it, opened with the client's and ended with it.

- Tools and prompts are named `{server}__{name}`; a `ToolFilter` says which
  tools are offered, and a request's `ToolScope` limits it further.
  `Aggregate::with_switches` takes a shared `ToolSwitches` too: a server or
  tool turned off there is neither listed nor callable, at once.
- Resources keep their URIs under `mcp://{server}/{uri}`.
- Lists are merged in the order the servers are given, with a cursor that
  pages the servers that have more.
- The servers' notification streams are merged into the client's.

```rust,ignore
use respan_gateway_plugin_mcp_aggregate::Aggregate;
use respan_gateway_plugin_tool_loop::ToolFilter;

let filter = ToolFilter::new(vec![], vec!["*__delete_*".into()]);
let aggregate = Aggregate::new(vec!["github".into(), "files".into()], filter);
let closer = aggregate.closer();
let gateway = gateway.plugin(closer.plugin()).plugin(aggregate);
// On shutdown or reload, once the gateway's requests are done:
closer.close(&gateway).await;
```

Register it before any plugin that routes MCP requests by path. A session is
reached by its unguessable id alone. Give none of its servers a client's credentials:
the sessions are the gateway's. Used by the binary's `[plugins.mcp_aggregate]`.
