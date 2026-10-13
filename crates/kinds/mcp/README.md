# respan-gateway-mcp

`respan-gateway-mcp` is the MCP kind: typed JSON-RPC messages and the
`Input` / `Output` envelopes around them. MCP over streamable HTTP, and an
MCP server run as a child process, are
[`respan-gateway-mcp-http`](../../formats/mcp-http/README.md)'s. MCP is one
standard; there are no vendors.

`Message` (with `Id`, `Outcome` and `RpcError`) is a request, notification, or response. Either direction may
carry any of these shapes, so it is one type, not a directional pair.
Responses have exactly one `Outcome::Success` or `Outcome::Error`; string and
numeric IDs stay distinct. Unknown protocol fields survive in explicit
extension maps.

The kind is `Input` in, `Output` out, as for every kind. `Input` wraps
input messages with HTTP method, batch flag, session,
protocol version, resume cursor and query. `Output` wraps output messages
with session, SSE event ID and event name. Keeping those fields on the same
envelope preserves their association when a plugin buffers, edits or drops an
event. SSE comments are dropped; ID-only events retain their cursor.

Import `Input` / `Output` from this crate for HTTP gateway registration
and filtering. Inspect each envelope's `messages` as typed JSON-RPC messages.
The crate is the kind's types alone, with no I/O: `http` for a request's
method, `serde` and `serde_json`.

- `RpcCode`: the JSON-RPC error code a fault is written with, as an extension.
  A server's non-success answer whose body is a JSON-RPC error is read back
  into one: its code, message and `data` (as `Details`) reach the client's
  error, with the HTTP status kept; any other body gives -32603.
- `error(id, code, message)`: a JSON-RPC error.
