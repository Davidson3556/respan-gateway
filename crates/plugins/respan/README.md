# respan-gateway-plugin-respan

`Respan` sends each request through the gateway to
[Respan](https://www.respan.ai) as a trace, over OTLP/HTTP JSON
(`POST https://api.respan.ai/api/v2/traces`, `Authorization: Bearer <key>`):

- a root span for the request (`traceloop.span.kind = workflow`), its status
  and error, and the customer a host names with a `Customer` extension
  (`respan.customer_params.customer_identifier`; the gateway's is the client
  key's name);
- a span for each call it made: to a model, a `chat` span
  (`llm.request.type = chat`) with `gen_ai.request.model`,
  `gen_ai.response.model`, `gen_ai.usage.*` and, unless `content` is off,
  the messages sent (`gen_ai.prompt.{N}.*`) and the reply
  (`gen_ai.completion.0.*`); to an MCP server, as the tool loop's are, a
  `tool` span. Retries, fallbacks and a tool loop's steps are each a call.

Spans are sent off the request path, posted in batches (100 spans, or every
2 s) by a task of its own; while Respan is slow or down, at most 4096 traces
wait and the rest are dropped, counted (`Respan::dropped`), rather than slow
a request. What waits is sent when the plugin is dropped.

```rust
let gateway = gateway.plugin(Respan::new(&key, respan_gateway_plugin_respan::URL, "my-gateway", true)?);
```

In the gateway's config:

```toml
[plugins.respan]
key = "env:RESPAN_API_KEY"
# content = false            # metadata only: no messages or replies
# service = "respan-gateway"
# url = "https://api.respan.ai/api/v2/traces"
```
