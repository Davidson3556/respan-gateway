# 0003. Kernel contracts: minimal, typed by kind, grown without breaking

- Status: accepted, amended by [0006](0006-streams-both-ways.md), [0007](0007-format-is-a-codec.md), [0008](0008-events-carry-their-bytes.md), [0009](0009-core-cleanups.md), [0011](0011-read-until-content-or-end.md), [0012](0012-http-out-of-the-kernel.md), [0013](0013-no-http-in-core.md)
- Date: 2026-09-25

## Context

The [use cases](../use-cases.md) need far more than a first release:
long-lived streams, budgets, retention, work outliving its request. Most
of it is plugins' own state, and adding the rest must not break plugins
or formats. A chat model in the kernel fits neither MCP, images, video
nor users' own protocols, and translators per pair of formats grow with
the square of the formats and give plugins no typed view.

## Decision

### A minimal kernel

- Four hooks ([0004](0004-two-edges-typed-formats.md)) and one response
  pipeline. A new hook needs a use case and a test: each is kept forever.
- `on_client` changes the head and target, never the body's bytes, so
  every change is written by a format. *Superseded by 0012:* it changes
  the target and extensions; a call's head is edited with `edit_call`. Filters edit inputs and outputs
  as they stream, and are the one way to edit inputs.
- `Gateway::handle`, the only entry point, returns the response and a
  future of its `Outcome`; the tokio runtime runs end hooks detached, so
  none delays a response. `Gateway::idle` waits for every open response
  and end hook.
- Every head goes out with its body's first byte, so a failure before it,
  such as a guard refusing a cached answer, is still a refusal.
  `Encoder::open` writes opening bytes (`formats::sse::comment`) that
  take the head with them, so a quiet stream opens at once.
- The encoder writes for the status `Response::with_status` set; head
  edits come after and return `Result`, like `edit_call`. An edit's `Err`
  or panic refuses with no edits, as a half-run list makes a head no
  plugin meant. *Superseded by 0012:* `with_status` is gone; head edits
  stay as they were.
- A `Call` runs in the task awaiting it; its request ends it, before its
  own end hooks.
- Usage counts once, as a provider's response is decoded, on its call's
  provider edge, before any filter; a failure stops writing, never
  counting, as usage records what was spent
  ([0008](0008-events-carry-their-bytes.md),
  [0009](0009-core-cleanups.md)).
- Every filter gets exactly one end, even after a failure, drop or panic
  elsewhere, so what it keeps back is released. A list remembers its end,
  so a filter added later sees no value, then that end. A failing input
  filter ends them all. One ending with the stream may fail it; one still
  open at close is told the end and cannot change it, nor can a panic, so
  the `Outcome` is true.
- A filter's panic is an internal fault, like its `Err`, written in the
  client's format even after bytes are out, as the framing is sound. A
  decoder's or encoder's panic aborts the body, as the framing is lost.
- The seven `codes` say whose fault a failure is; a plugin may add its own.

### HTTP rules

*Superseded by [0012](0012-http-out-of-the-kernel.md):* every rule in
this section left the kernel, for the formats, the adapter and the
transports (`crates/http`), which keep them; a translated success keeps
no status or `Retry-After`, a fault carries both, and whose content is
whose (the last bullet) stays the kernel's.

- Connection headers (RFC 9110 7.6.1), the proxy's own (RFC 9110 11.7),
  `Content-Length`, which edits and reframing change, and `Trailer` and
  `TE`, as trailers are not delivered, are removed. `Upgrade` stays where
  a switch needs it, with the `Connection: upgrade` RFC 9110 7.8
  requires.
- An upgrading request's head goes at once, as its client waits for it. A
  101 goes only to a request that asked to switch (RFC 9110 15.2.2): an
  unasked one fails a call `UPSTREAM`, and refuses a plugin's response
  `INTERNAL`. A provider's redirect with a `Location` fails a translated
  call `UPSTREAM`, as it points at a resource in another format.
- A call carries no `Host` or `Expect` of the client's; what its format
  sets is its own ([0007](0007-format-is-a-codec.md)). A call that
  streams its inputs, or whose body differs from the client's, drops
  `Content-Encoding`.
- A 204, 205, 304 or response to a `HEAD` has no body (RFC 9110 9.3.2,
  15.3.5, 15.3.6, 15.4.5), so its head goes at once and nothing more is
  read: a call ends as decoded, or `abandoned` if a body was left.
  `ready` and `outputs` go by the call's method; sending, by the
  client's.
- A translated head keeps its status and `Retry-After` (RFC 9110 10.2),
  the one piece of response context true across a translation.
- A response whose client left ends 499 `abandoned`, nginx's status.
- Content that cannot be written is whose it is: `BAD_REQUEST` for the
  client's, even if a router changed the model; `CONFIG` for what a
  plugin changed or brought ([0007](0007-format-is-a-codec.md)).

### Kinds

- A kind is the pair of types its formats share, `Format::Input` and
  `Format::Output`, defined outside the kernel, never shared types like
  `serde_json::Value`, which would let a plugin for one kind read
  another's. Formats of one name are one protocol, so of one kind.
  Traffic the gateway does not read is a kind of one format
  (`formats::opaque`).
- Plugins ask by type (`cx.is_kind::<Conversation>()`,
  `cx.input::<Conversation>()`, `cx.filter(|r: &mut Reply| ..)`); another
  type is an `INTERNAL` `Err`, a bug.
- A whole body's input is decoded only when read; unchanged, it reaches a
  provider of the client's format as the client's bytes; changed, it is
  written into them. Other formats of the kind write it from its value,
  as does any format a call's own input (`with_input`). A call of the
  client's input to a provider sharing neither its format nor its kind
  fails `CONFIG`.
- Decoding refuses only a body that is not the format; what the kind has
  no place for is relayed within one format.
- The gateway's own credentials are removed before `handle` or by a
  plugin (`head_mut`); the transport decides the rest. *Superseded by
  0012:* the adapter lifts them into a `Credential` extension, which a
  plugin removes.
- Values are cloned and compared, so large payloads sit in an `Arc` of an
  `Eq` type.
- The model belongs to the kind (`Conversation.model`), edited by an input
  filter like any field, as no one model fits every protocol. Its place
  on the wire is the format's: the request encoder reads it from the
  first input, to put a model in the path, say.
- Routing by model reads the input, so an upgrading request, which has no
  input at its head, routes by its head, and opaque traffic by its body
  (`cx.body()`). *Amended by 0012:* a streaming request, which the adapter
  names, routes by the adapter's view of its head (`http::Asked`). A body that does not decode is still relayed: a router
  may ignore an `input` `Err`; a plugin that vets input refuses on it.

### Growing without breaking

- Growing data types are `#[non_exhaustive]` or have private fields;
  types plugins build have constructors.
- New behaviour comes as defaulted trait methods or context methods. The
  `Arc` impls of `Plugin` and `Transport` forward every method; `Format`
  has none, as the gateway takes it by value and shares it.
- How a response ends (`Result<(), Fault>`) never grows.
- Data a later API shows beyond the request and response reaches only
  plugins that opt in, and is an `Err` for others, so a guardrail refuses
  what it cannot see.
- Providers are reached only through calls, so a check on the provider
  edge covers every call. Safety depends on the explicit order (`after`);
  a plugin needing two orders registers two halves.

### Promised

Only what needs the kernel is promised; each lands additively. State
belongs to plugins (their own `Arc`, a call's extensions), keys to
transports, deadlines and draining to the embedder (`idle`).

- A call with its own `http::Request`, to any format and provider.
- An async stream filter beside `Filter`, calling models mid-stream.
- Usage told to the client that calls already count.
- A large body passed on unread, the target chosen from the head or a
  prefix, only if every plugin says it does not need the body.
- Trailers, through defaulted codec methods: relayed untouched;
  translated, refused unless one cannot change the reply.
- Scrubbing a provider's error text, through a defaulted `Format` method.
- Translation between kinds, under a decision of its own.

## Consequences

- The kernel has no request model or chat-shaped types; a plugin for a
  kind works on all its formats.
- Code outside the core matches kernel types with a wildcard arm, so each
  promise lands unbroken.

## History

- 0006: long-lived streams became requests whose inputs stream, not a
  second entry point, as a WebSocket is a request with messages after
  its head.
- 0007: formats became codecs that edit heads, as request functions had
  nowhere to keep a stream's state.
- 0008: usage filters, held usage, `()` as no kind and "a later Part
  comes with a refusing method" went, as each message now is one value
  carrying its bytes and usage.
- 0009: client-edge usage, `te: trailers` and reading a bodiless
  response's call to its end went.
- 0012: the HTTP rules, `head_mut` and `with_status` went; the `http`
  types stay as an envelope the kernel never reads.
