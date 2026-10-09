# Plugin SDK

`respan-gateway-sdk` is the general plugin-author entry point. It reexports
core contracts (`Plugin`, `ClientContext`, `ProviderContext`, `Filter`,
`Fault`, `EventStream`, `EventSink`, and gateway/call/response APIs) as their
original types and adds kind-neutral usage, fault facts and tagged native values.

`Usage` records integer amounts of named units. A `Fault` carries typed facts
that each edge renders as it knows: `BadRequest`, `Upstream`, `Truncated`,
`RetryAfter`, `ErrorCode` and `Details`. Core adds its own markers:
`Abandoned`, `Internal`, `Config` and `NotFound`.

The SDK defines no request/event kinds or `Format` implementations. Import
kind-specific types from the crate that owns them:

| Crate | Input / output pair |
| --- | --- |
| [`respan-gateway-conversation`](../conversation/README.md) | `Input` / `Output` |
| [`respan-gateway-realtime`](../realtime/README.md) | `Input` / `Output` |
| [`respan-gateway-mcp-messages`](../mcp-messages/README.md) | `Input` / `Output` |

Every model crate names its pair `Input` / `Output`. Direction is gateway-wide:
`Input` travels from client to provider; `Output` travels from provider to client.
Use a crate alias when working with multiple kinds, such as
`conversation::Input` and `realtime::Input`.

The core accepts distinct input/output types through `Format<T, R, W>`.
Conversation plugins import `respan_gateway_conversation` alongside the SDK;
conversation types are not reexported here. Usage unit names for conversations
live in `respan_gateway_conversation::units`.

The SDK depends only on core and `serde_json`. Protocol authors use
`respan_gateway_http` for HTTP bindings and reusable wire helpers. Routing
mechanisms live in `respan_gateway_plugins::routing`.


`NativeFields { format, fields }` preserves unsupported fields on a typed
value. `NativeValue { format, value }` preserves an entire unsupported value.
The format ID identifies the wire schema, not a provider deployment; it is
required so codecs can reject unsafe cross-format forwarding. Neither carries
transport state.
