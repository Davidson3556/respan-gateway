# Tool vendor kit

`respan-gateway-tool-vendor-kit` is a tool vendor as a template, and an MCP
server of it in process. A tool vendor is an HTTP API that is no MCP server,
offered as MCP tools; the tool vendors themselves are
[`respan-gateway-tool-vendors`](../README.md).

- `ToolVendor`: its name in configuration (`id`), where its key comes from
  (`key_env`, `key_from_env`), how it takes the key (`KeyIn`: a bearer token
  or a header of its own), its base URL, and its tools. `api(key, base_url)`
  is its API with that key, at its own base URL or the one given.
- `Tools`: its tools as MCP's `tools/list` describes them (`ToolSpec`, the
  schema as JSON text), and calling one. A tool's result is text; a
  `ToolError` is a result too, one the model is told failed.
- `Api`: an HTTP client of the vendor, with its key. `url(path)` is `path`
  under the base URL, or `path` itself when it is an absolute URL.
  `post_json` and `get_json` send JSON and read JSON; a failed request, an
  answer other than 2xx (its body quoted up to 2000 bytes) or one that is
  no JSON is a `ToolError`. No error names the key. Requests time out after
  2 minutes.
- `ToolServer::new(vendor, api)`: the vendor as a stateless MCP server.
  `send(Request<Body>)` answers what `StreamableHttp` sends:
  `initialize` (the client's protocol version, or `PROTOCOL`), `ping`,
  `tools/list` (one page) and `tools/call`, as JSON, one response or a
  batch's; a `POST` with no request gets 202, a `GET` 405, a `DELETE` 204.
  An unknown tool is a JSON-RPC -32602, an unknown method -32601.
  `.connect(limit)` supplies the provider factory, so a gateway registers
  it as `Provider::new(name, StreamableHttp::new("/"), server.connect(limit))`
  like any MCP server, and `/mcp/{name}`, the aggregate `/mcp` and the tool
  loop reach its tools unchanged.
