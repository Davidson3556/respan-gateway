# 0004. Two edges, typed formats

- Status: accepted, amended by [0008](0008-events-carry-their-bytes.md), [0009](0009-core-cleanups.md), [0010](0010-usage-out-of-core.md), [0012](0012-http-out-of-the-kernel.md), [0013](0013-no-http-in-core.md), [0015](0015-core-trims.md), [0020](0020-symmetric-edges.md) (no head hooks)
- Date: 2026-09-26

## Context

A request can make many calls: a router asks a judge, a fallback tries
twice, a hedge races two. Some plugins belong with the client, once per
request (guardrails, PII, routing, caching); others with the providers,
once per call (billing, limits, signing). Plugins need a typed view of
what the client sends and gets, whatever the format.

## Decision

### Input and output

- A format's `Input` and `Output` are any
  `Clone + PartialEq + Send + Sync + 'static` types. They differ, so the
  type names the direction; the gateway rejects a format whose two types
  are one.
- A request carries one input or a stream of them, a response one output
  or a stream ([0006](0006-streams-both-ways.md)).
- `Gateway::provider` and `Gateway::serve` take any `Format` by value and
  erase its types; a binary picks formats by name.

### Two edges

- The client edge: `on_client(&mut ClientContext)` runs once per request,
  for each plugin in order, and may pass, answer or refuse.
  `on_client_end(&ClientContext, &Result<(), Fault>)` runs once, at the end, for
  each plugin whose `on_client` started.
- The provider edge: `on_provider(&mut ProviderContext)` runs before
  every call, for each plugin in order until one fails the call, without
  waiting for earlier calls' end hooks. It may be cancelled at any await;
  its `on_provider_end` still runs, so a plugin records what it reserves
  before awaiting.
- `px.client()` is the request as it was when the call was made, by `&`,
  so `call` and `take_inputs`, which take `&mut`, do not compile there.
  End hooks get `&` too.
- The provider hook sees the call's provider, format, kind, first input
  and extensions (the request's plus its own); it may fail the call,
  change its extensions, filter its inputs after the client edge's input
  filters, and filter its outputs before the client edge's. The call's
  head is its format's to write (0020).
- The provider edge sees its response as decoded, whoever reads it.
- `on_provider_end(&ProviderContext, &Result<(), Fault>)` runs once per call whose
  `on_provider` started: when the call fails before a response, its body
  ends, or its reader stops at a failure. Dropped, cut off, or open when
  its request ends, it ends `abandoned`, before the request's
  `on_client_end`, which waits for it.
- The end is how the edge ended, `Ok` or a `Fault`. A provider's error
  answer is a fault.
- Client-edge filters end with the request, even when the client leaves.

### Providers

- A provider is a name, a format and a transport sharing its wire,
  `Gateway::provider(name, format, transport)`, with no type of its own.
- The transport owns where and how: a URL and credentials, a process, a
  socket, or in-process code. It may keep state its calls share.
- `Transport::send` gets the call as its format wrote it.
  A relay's call starts from the client's head, so the transport strips
  what its protocol forbids a proxy to pass on (`http::proxy_call`). Its
  body carries the inputs, one per data frame; its extensions are the
  call's, with `Exchange::relayed` set on a relay alone.
- The transport decides whose keys reach the provider. `HttpTransport`
  sends `CallHeaders`, else its own headers, else the client's
  `Credential`, the last only on a relay. It strips the client origin's
  headers from a call and the provider origin's (`set-cookie`) from a
  response.
- A per-call key is a value in the call's extensions for the transport.

## Consequences

- A provider without HTTP, such as an MCP server over stdio, needs only a
  transport of its own.
- The core has no notion of credentials or origins.

## History

- 0008: usage filters went; `()` no longer marks a format of no kind.
- 0009: `Call::with_filter`, client-edge usage and `on_provider` waiting
  for earlier calls' end hooks went; cookies and forwarding headers
  became the transport's.
- 0010: `Outcome.usage` went.
- 0012: `Outcome.head` went; the kernel stopped filling calls;
  credentials became an extension.
- 0013: `Transport::send` takes an `Exchange` of its wire's heads.
- 0015: the end is a plain `Result<(), Fault>`; `relayed` is a field.
