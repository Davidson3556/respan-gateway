# 0004. Two edges, neutral events

- Status: accepted
- Date: 2026-09-26

## Context

A request can make many calls: a smart router asks a judge model which
model to use, then calls it; a fallback tries twice; a hedge races two.
Some plugins belong with the client, once per request: guardrails, PII,
routing, caching. Others belong with the providers, once per call:
billing, rate limits, signing, per-target conversion. Billing on the
client edge alone would miss the judge's call. Sessions
([0003](0003-kernel-contracts.md)) need a type for what the client
sends.

## Decision

### Input and output

- `Event` is the bound on a kind's input and output: any
  `Any + Send + Sync + Clone + PartialEq` type.
- The two types differ, so the type names the direction. The gateway
  refuses a format whose input and output are one type.
- A plain request carries one input, decoded from its whole body.

### A typed `Format`

- `Provider::new` and `Gateway::serve` take any `Format`. The kernel
  compares its types and erases them.
- `Format` is not used as `dyn`, so a binary picks formats by name from a
  table of constructors.

### Two edges

[ARCHITECTURE](../ARCHITECTURE.md#two-edges) draws the two edges.

- The client edge: `on_client(&mut Context)` runs once per request, for
  each plugin in the gateway's order, and may pass, answer or refuse.
  `cx.filter` and `cx.map_head` edit what the client gets.
- `on_client_end(&Context, &Outcome)` runs once, at the end, for each
  plugin whose `on_client` started.
- The provider edge: `on_provider(&mut ProviderContext)` runs before
  every call the request makes, for each plugin in the gateway's order
  until one refuses.
- A call lives inside its request. `px.client()` is the request's
  `Context` as it was when the call was made; its `call` is an `Err`.
- The hook sees the call's target, format and model, and its own copies
  of the input and the extensions. It may refuse the call before
  anything is sent, and add filters to its response, which run before
  the client edge's.
- `on_provider_end(&ProviderContext, &Outcome)` runs once per call, for
  each plugin whose `on_provider` started, with that call's status and
  usage. It runs when the call fails before a response, or when its
  response ends, is collected or is dropped.
- A call's response still open when its request closes ends abandoned,
  its filters getting their `end`. A request's calls therefore finish
  their `on_provider_end` before its `on_client_end` starts.
- Billing counts every call on the provider edge. The client edge's
  `Outcome` has the committed call's usage plus any `with_usage`, so the
  two edges' usage is never added.

### Providers

- A provider is a name, a format and a transport:
  `Provider::new(name, format, transport)`.
- The transport owns where and how: a URL and credentials, a process, a
  socket, or code in the same process. The kernel knows none of them.
- Each provider has its own transport, which may hold state its calls
  share, such as a connection pool.
- `Transport::send` gets the call's request as the format prepared it.
  Its URI is the path and query relative to the transport. It has no
  hop-by-hop headers, none the client's `Connection` names, no `Host` and
  no `Expect`. Its extensions are the call's.
- A format passes the client's headers and query on within its own
  protocol, credentials included: it knows the protocol, not the keys.
- The transport decides whose keys reach the provider. `HttpTransport`
  with headers of its own removes the client's credentials first; with
  none, it passes them through, as a local proxy needs.
- A per-call key, such as a tenant's, is a value a hook puts in the
  call's extensions (`px.extensions_mut()`) for the transport to read,
  such as the server crate's `CallHeaders`.
- A transport checks its own settings when built, such as a URL or a
  header.

## Consequences

- A provider without HTTP, such as an MCP server over stdio, needs only a
  transport of its own.
- The core has no notion of credentials.
