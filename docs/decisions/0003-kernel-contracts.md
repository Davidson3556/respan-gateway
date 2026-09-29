# 0003. Kernel contracts: minimal, typed by kind, grown without breaking

- Status: accepted, amended by [0006](0006-streams-both-ways.md), [0007](0007-format-is-a-codec.md), [0008](0008-events-carry-their-bytes.md), [0009](0009-core-cleanups.md), [0010](0010-usage-out-of-core.md), [0011](0011-read-until-content-or-end.md), [0012](0012-http-out-of-the-kernel.md), [0013](0013-no-http-in-core.md), [0018](0018-plugin-order-is-registration-order.md), [0019](0019-status-out-of-the-kernel.md) (`Fault` has no status), [0020](0020-symmetric-edges.md) (the kernel is events-only: no head hooks, `Gateway::handle` takes the caller's wire, a format is the edge contract)
- Date: 2026-09-25

## Context

The [use cases](../use-cases.md) need far more than a first release, and
adding it must not break plugins or formats. A chat model in the kernel
fits neither MCP, images nor users' own protocols, and translators per
pair of formats grow with the square of the formats.

## Decision

### A minimal kernel

- Four hooks ([0004](0004-two-edges-typed-formats.md)) and one response
  pipeline. A new hook needs a use case and a test: each is kept forever.
- `on_client` changes the target and extensions. Filters edit inputs and
  outputs as they stream, and are the one way to edit inputs. No plugin
  edits a head: heads are the formats' to write (0020).
- `Gateway::handle(wire, extensions)` is the only entry point. It hands
  the caller's wire to a client edge, which writes the response, and
  returns a future of how the request ended (`Result<(), Fault>`); the
  tokio runtime runs end hooks detached.
  `Gateway::idle` waits for every open response and end hook.
- The client edge owns when a response head goes out. HTTP's goes with the
  body's first byte, so a failure before it is still a refusal; a quiet
  stream's codec opens it at once (`Encoder::open`), and an upgrade's goes
  when the reply resolves (0020, rule 3).
- A `Call` runs in the task awaiting it; its request ends it, before its
  own end hooks.
- Every filter gets exactly one end, even after a failure, drop or panic
  elsewhere. A list remembers its end, so a filter added later sees it at
  once. A failing input filter ends them all. A filter still open at close
  is told the end and cannot change it, nor can a panic, so the end is true.
- A filter's panic is an internal fault, like its `Err`, written in the
  client's format even after bytes are out. A codec's decoder or encoder
  panic aborts the body, as the framing is lost.
- Content that cannot be written is whose it is: `BAD_REQUEST` for the
  client's, even if a router changed the model; `CONFIG` for what a
  plugin changed or brought.
- The `codes` say whose fault a failure is; a plugin may add its own.

### Kinds

- A kind is the pair of types its formats share, `Format::Input` and
  `Format::Output`, defined outside the kernel, never shared types like
  `serde_json::Value`. Formats of one name are one protocol, so of one
  kind. Traffic the gateway does not read is a kind of one format
  (`formats::opaque`).
- Plugins ask by type (`cx.is_kind::<Conversation>()`,
  `cx.input::<Conversation>()`, `cx.filter(|r: &mut Reply| ..)`); another
  type is an `INTERNAL` `Err`, a bug.
- A whole body's input is decoded only when read (`Format::opening`);
  unchanged, it reaches a provider of the client's format name as the
  client's origin, unread, written as the client's bytes; changed, it is
  written into them. Other formats of the kind write it from its value,
  as does any format a call's own input (`with_input`). A call of the
  client's input to a provider sharing neither its format nor its kind
  fails `CONFIG`.
- Decoding refuses only a body that is not the format; what the kind has
  no place for is relayed within one format.
- Values are cloned and compared, so large payloads sit in an `Arc`.
- The model belongs to the kind (`Conversation.model`), edited by an input
  filter. Its place on the wire is the format's: the request encoder
  reads it from the first input.
- Routing by model reads the input, so a streaming request routes by the
  adapter's view of its head (`http::Asked`), and opaque traffic by its
  input's bytes (through its origin). A router may ignore an `input` `Err`; a plugin that vets
  input refuses on it.

### Growing without breaking

- Growing data types are `#[non_exhaustive]` or have private fields;
  types plugins build have constructors.
- New behaviour comes as defaulted trait methods or context methods. The
  `Arc` impls of `Plugin` and `http::Transport` forward every method.
- How a response ends (`Result<(), Fault>`) never grows.
- Data a later API shows reaches only plugins that opt in, and is an
  `Err` for others, so a guardrail refuses what it cannot see.
- Providers are reached only through calls, so a check on the provider
  edge covers every call. Safety depends on registration order;
  a plugin needing two orders registers two halves.

### Promised

Only what needs the kernel; each lands additively. State belongs to
plugins, keys to transports, deadlines and draining to the embedder.

- An async stream filter beside `Filter`, calling models mid-stream.
- A large body passed on unread, the target chosen from the head or a
  prefix, only if every plugin says it does not need the body.
- Trailers, through defaulted `Codec` methods.
- Scrubbing a provider's error text, through a defaulted `Format` method.
- Translation between kinds, under a decision of its own.

## Consequences

- The kernel has no request model or chat-shaped types; a plugin for a
  kind works on all its formats.
- Code outside the core matches kernel types with a wildcard arm.

## History

- 0006: long-lived streams became requests whose inputs stream, not a
  second entry point.
- 0007: formats became codecs.
- 0008: usage filters, `()` as no kind and erased `Part`s went.
- 0009: client-edge usage and `te: trailers` went.
- 0010: usage left the kernel; "usage counts once, before any filter" went.
- 0012: the HTTP rules section left the kernel for `crates/http`, the
  formats and transports; `head_mut` and `with_status` went.
- 0013: the promise of a call with its own `http::Request` went; no
  `http` types remain.
- 0015: end hooks and `handle`'s future take a `Result<(), Fault>`, in place
  of a bundle.
- 0020: no plugin edits a head; `handle` takes a wire and the client edge
  writes the response; `Format` is the edge contract.
