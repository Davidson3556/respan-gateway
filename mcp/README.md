# respan-gateway-mcp

The Model Context Protocol on the gateway. Messages stay JSON-RPC values
(every field survives), and output is always written from them, in the
framing the client's request asks for. What MCP carries in HTTP headers is
part of the kind, so a call carries it with no relay and plugins' filters
see and edit it: a `ToServer` holds the client's `session`,
`protocol_version` and `last_event_id`, and a `ToClient` the `session` the
server named (as on its answer to `initialize`) and its SSE `event_id` and
`event` name (for `Last-Event-ID` resumption), written only in an event
stream; SSE comments are dropped.

- `ToServer` / `ToClient`: the kind, a client's request and a server's
  message.
- `StreamableHttp::new(path)`: MCP over streamable HTTP as direct
  `Format<Incoming, ToServer, ToClient>` and reversed `Format<Outgoing, ToClient, ToServer>` bindings;
  `SESSION` is its session header. The request shapes the answer: a `POST`
  with no request gets 202 and no body, one with requests an event stream
  (one JSON body if its `Accept` takes no event stream), a `GET` the
  server's own event stream, a `DELETE` 204.
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
- `error(id, code, message)`: a JSON-RPC error.

Used by: [mcp_proxy](../examples/integrations/mcp_proxy.rs).
