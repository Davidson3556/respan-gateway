# MCP messages

`respan-gateway-mcp-messages` defines the typed JSON-RPC `Message` MCP
carries: a request, notification, or response. Either direction may carry
any of these shapes, so it is one type, not a directional pair; the MCP
kind's `Input` / `Output` pair is the transport's envelope around it. Responses have exactly one
`Outcome::Success` or `Outcome::Error`; string and numeric IDs stay distinct.
Unknown protocol fields survive in explicit extension maps.

This data-model crate depends only on serde and serde_json. HTTP methods,
batching, sessions, protocol headers and SSE cursors belong to
[`respan-gateway-mcp`](../mcp/README.md), whose `Input` and `Output`
envelopes carry vectors of these typed messages.
