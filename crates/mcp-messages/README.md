# MCP messages

`respan-gateway-mcp-messages` owns the complete `Input` / `Output` pair.
Both are type aliases of the same JSON-RPC `Message` enum: a request,
notification, or response. There is no additional wrapper.
Either direction may carry any of these shapes. Responses have exactly one
`Outcome::Success` or `Outcome::Error`; string and numeric IDs stay distinct.
Unknown protocol fields survive in explicit extension maps.

This data-model crate depends only on serde and serde_json. HTTP methods,
batching, sessions, protocol headers and SSE cursors belong to
[`respan-gateway-mcp`](../mcp/README.md), whose `HttpInput` and `HttpOutput`
envelopes carry vectors of these typed messages.
