# 0004. Two edges, typed formats

- Status: accepted, amended by [0008](0008-events-carry-their-bytes.md), [0009](0009-core-cleanups.md)
- Date: 2026-09-26

## Context

A request can make many calls: a smart router asks a judge model, then
calls the model it picks; a fallback tries twice; a hedge races two. Some
plugins belong with the client, once per request: guardrails, PII,
routing, caching. Others belong with the providers, once per call:
billing, rate limits, signing. Billing on the client edge alone would
miss the judge's call. Plugins need a typed view of what the client sends
and gets, whatever the format.

## Decision

### Input and output

- A format's `Input` and `Output` are any
  `Clone + PartialEq + Send + Sync + 'static` types. They differ, so the
  type names the direction; the gateway rejects a format whose two types
  are one.
- A request carries one input or a stream of them, a response one output
  or a stream ([0006](0006-streams-both-ways.md)).
- `Gateway::provider` and `Gateway::serve` take any `Format` by value and
  erase its types; a binary picks formats by name from a table of
  constructors.

### Two edges

[ARCHITECTURE](../ARCHITECTURE.md#two-edges) draws the two edges.

- The client edge: `on_client(&mut ClientContext)` runs once per request,
  for each plugin in order, and may pass, answer or refuse.
  `on_client_end(&ClientContext, &Outcome)` runs once, at the end, for
  each plugin whose `on_client` started.
- The provider edge: `on_provider(&mut ProviderContext)` runs before
  every call, for each plugin in order until one fails the call, without
  waiting for the end hooks of the request's earlier calls. It may be
  cancelled at any await; its `on_provider_end` still runs, so a plugin
  records what it reserves before awaiting.
- `px.client()` is the request as it was when the call was made, by `&`,
  so `call` and `take_inputs`, which take `&mut`, do not compile there:
  calling from a call's hooks would recurse. End hooks get `&` too.
- The provider hook sees the call's provider, format, kind, first input
  and extensions (the request's plus its own), may fail the call, edit
  its HTTP request after its format writes it (`px.edit_call`), filter
  its inputs after the client edge's input filters, and filter its
  outputs before the client edge, so each edge sees what it is nearer
  to. A filter for one call alone is added here, keyed on the provider
  or a mark in the call's extensions.
- The provider edge sees its response as decoded, whoever reads it, so
  no reader skips its filters or count.
- `on_provider_end(&ProviderContext, &Outcome)` runs once per call whose
  `on_provider` started: when the call fails before a response, its
  body ends, or its reader stops first at a failure. Dropped or cut off
  before its end, or pending or open when its request ends, it ends
  `abandoned`, before the request's `on_client_end`, which waits for
  it, so it spawns slow work.
- An `Outcome` has the response's `head` (the provider's on the provider
  edge, the one sent on the client edge), its `end` (how it was read: a
  500 dropped unread ends `abandoned`; a response refused before its
  first byte ends its call with that failure), and its `usage`.
- Only a provider's response counts: the provider edge has its call's
  usage, the client edge the sum of its calls'. What a plugin makes or
  relays counts nothing; a cached answer's usage travels in an
  extension. Billing counts on one edge.
- Client-edge filters end with the request, even when the client leaves,
  so what they kept back is dropped.

### Providers

- A provider is a name, a format and a transport,
  `Gateway::provider(name, format, transport)`, with no type of its own.
- The transport owns where and how: a URL and credentials, a process, a
  socket, or in-process code. It may keep state its calls share, and
  checks its own settings when built.
- `Transport::send` gets `http::Request<Body>` as the kernel filled it and
  its format and `edit_call` edited it: its URI relative to the
  transport, none of the client's connection headers, `Host` or `Expect`,
  and `Upgrade` with `Connection: upgrade` only if the call switches
  protocols. Its body carries the call's inputs, one per data frame; its
  extensions are the call's.
- The request encoder reads from the first input what the protocol keeps
  outside the body, such as a model in the path
  ([0007](0007-format-is-a-codec.md)). A model in an upgrading request's
  head is the format's spelling, changed with `edit_call`.
- A call relaying the client's message passes the client's headers and
  query on as a proxy does, credentials included. The transport decides
  whose keys reach the provider: `HttpTransport` with headers of its own
  removes the client's credentials, and with none passes them through;
  it strips headers bound to the gateway's origin from the call
  (`Cookie`, `Forwarded`, `Via`, `X-Forwarded-*`, `X-Real-IP`) and
  those bound to the provider's from the response, such as `set-cookie`.
- A per-call key is a value in the call's extensions for the transport to
  read (the server crate's `CallHeaders`); `edit_call` is not for
  credentials, which such a transport strips.

## Consequences

- A provider without HTTP, such as an MCP server over stdio, needs only a
  transport of its own.
- The core has no notion of credentials or origins.

## History

- 0008: usage filters (`filter::<Usage>`) went, replaced by read-only
  meters, as usage now counts as decoded; `()` no longer marks a format
  of no kind.
- 0009: `Call::with_filter`, `Response::with_usage`, client-edge usage
  and `on_provider` waiting for earlier calls' end hooks went; cookies
  and forwarding headers became the transport's to strip.
