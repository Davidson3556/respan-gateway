# 0004. Two edges, neutral events

- Status: accepted
- Date: 2026-09-26
- Amends: [0003](0003-kernel-contracts.md)
- Amended by: [0005](0005-no-routes-global-plugins.md)

## Context

A request can make many calls: a smart router asks a judge model which model
to use, then calls it; a fallback tries twice; a hedge races two. Some
plugins belong with the client, once per request (guardrails on what the
client sends and gets, PII, routing, caching); others belong with the
providers, once per call (billing, rate limits, signing, per-target
conversion). Billing at the client edge sees only the committed answer, so
the judge's call goes unbilled. Sessions (0003) need a type for what the
client sends.

## Decision

### Input and output

- `Event` is the bound on a kind's input and output: any `'static + Clone
  + PartialEq + Send + Sync` type. The two differ, so the type names the
  direction.
- A plain request carries one input, decoded from its whole body.

### A typed `Format`

- `Provider::new` and `Gateway::serve` take any `Format`, whose types the
  kernel compares and erases.
- `Format` is not used as `dyn`, so a binary picks formats by name from a
  table of constructors.

### Two edges

The two edges are drawn in [ARCHITECTURE](../ARCHITECTURE.md#the-idea).

- The client edge: `on_client(&mut Context)` runs once, for each plugin in
  the gateway's order, and may pass, answer or refuse; `cx.filter` and
  `cx.map_head` edit what the client gets; `on_client_end(&Context,
  &Outcome)` runs once at the end for each plugin whose `on_client`
  started.
- The provider edge: `on_provider(&mut ProviderContext)` runs before every
  call the request makes, the target's, a fallback's or a plugin's own,
  for each plugin in the gateway's order until one refuses.
- A call lives inside its request: `px.client()` is the request's
  `Context` as it was when the call was made, whose `call` is an `Err`.
  The hook sees the call's target, format and model and its own copies of
  the input (`px.input_mut`) and the extensions; it may refuse the call
  before anything is sent and add filters to its response, which run
  before the client edge's.
- `on_provider_end(&ProviderContext, &Outcome)` runs once per call for
  each plugin whose `on_provider` started: when the call fails before a
  response, or when its response ends, is collected or is dropped, with
  that call's status and usage. A call whose response is still open when
  its request closes, such as one a plugin kept, ends abandoned then, its
  filters getting their `end`, so a request's calls finish their
  `on_provider_end` before its `on_client_end` starts.
- Billing counts every call on the provider edge; the client edge's
  `Outcome` keeps the committed answer's usage.

### Providers

- A provider is a name, a format and a transport:
  `Provider::new(name, format, transport)`. The transport owns where and
  how, such as a URL and credentials, a process, a socket or code in the
  same process; the kernel knows none of them. Each provider has its own
  transport, which may hold state its calls share, such as a connection
  pool or a process.
- `Transport::send` gets the call's request as the format prepared it:
  the path and query relative to the transport, without the client's
  hop-by-hop headers, `Host` or `Expect`, with the call's extensions.
- The transport owns credentials; the core knows none. A format passes
  the client's headers and query on within its own protocol, credentials
  included: it knows the protocol, not the keys. The transport decides
  whose keys reach the provider: `HttpTransport` with headers of its own
  removes the client's credentials before adding them; without any it
  passes the client's through, as a local proxy needs. A per-call key,
  such as a tenant's, is a value a hook puts in the call's extensions
  (`px.extensions_mut()`), which the transport reads, such as the server
  crate's `CallHeaders`.
- A transport checks its own settings when built, such as a URL or a
  header; `Gateway::new(providers)` checks names and kinds.

## Consequences

- A provider without HTTP, such as an MCP server over stdio, needs only a
  transport of its own.
