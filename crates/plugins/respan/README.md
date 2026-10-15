# respan-gateway-plugin-respan

`Respan` sends each request through the gateway to
[Respan](https://www.respan.ai) as a trace, over OTLP/HTTP JSON
(`POST https://api.respan.ai/api/v2/traces`, `Authorization: Bearer <key>`):

- a root span for the request (`traceloop.span.kind = workflow`), its status
  and error, and the customer, the request's `Caller` name
  (`respan.customer_params.customer_identifier`; the gateway's is the client
  key's name);
- a span for each call it made: to a model, a `chat` span
  (`llm.request.type = chat`) with `gen_ai.request.model`,
  `gen_ai.response.model`, `gen_ai.usage.*` and, unless `content` is off,
  the messages sent (`gen_ai.prompt.{N}.*`) and the reply
  (`gen_ai.completion.0.*`), tool calls and results as OpenLLMetry has them
  (`….tool_calls.{M}.id`, `.name`, `.arguments`; a result its own `tool`
  message with `tool_call_id`) and the tools offered
  (`llm.request.functions.{M}.*`); to an MCP server, as the tool loop's are, a
  `tool` span for a `tools/call` (named `<server>.<tool>`) and a `task` span
  for the rest (`initialize`, `tools/list`), with `mcp.method`,
  `traceloop.entity.name` (the tool or method) and, unless `content` is off,
  `traceloop.entity.input` (the tool's arguments, or the params) and
  `traceloop.entity.output` (its result, or error). A tool's `isError`
  result or a JSON-RPC error marks the span failed. Retries, fallbacks and
  a tool loop's steps are each a call.

Spans are sent off the request path, posted in batches (100 spans, or every
2 s) by a task of its own; while Respan is slow or down, at most 4096 traces
wait and the rest are dropped rather than slow a request. What waits is sent
when the plugin is dropped.

`Respan::health` says how sending goes: spans sent, rejected (OTLP's
`partialSuccess`) and failed, traces dropped, the last success and error;
`Health::on_change` is called when sending starts failing, and when it works
again. The gateway logs those and shows them on its Overview page
(`respan` in `GET /ui/api/overview`).

Register `Respan` before the host's other plugins, so it opens a request's
trace before any plugin answers it (as a tool loop or a route's dispatch
does), and `Respan::calls` after them, so it records each call as sent.
`Respan::only` limits it to some requests: the gateway traces its clients'
alone, not those it hands itself (the tool loop's sessions, the
dashboard's look at a tool server).

```rust
let respan = Respan::new(&key, respan_gateway_plugin_respan::URL, "my-gateway", true)?;
let calls = respan.calls();
let gateway = gateway.plugin(respan) /* .plugin(the others) */ .plugin(calls);
```

In the gateway's config:

```toml
[plugins.respan]
key = "env:RESPAN_API_KEY"
# content = false            # metadata only: no messages or replies
# service = "respan-gateway"
# url = "https://api.respan.ai/api/v2/traces"
```
