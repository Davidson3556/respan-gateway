# 0006. Streams both ways

- Status: accepted, amended by [0008](0008-events-carry-their-bytes.md), [0011](0011-read-until-content-or-end.md), [0012](0012-http-out-of-the-kernel.md), [0020](0020-symmetric-edges.md) (no `Exchange::streams` in the core: a stream is inputs that are not already ended)
- Date: 2026-09-27

## Context

0003 planned a second entry point for WebSockets and quiet streams. But a
WebSocket is a request whose messages come after its head, and a response
already streams; only a streaming request was missing.

## Decision

- No separate concept for long-lived exchanges: a request carries inputs,
  a response outputs, and either may stream.
- A request streams its inputs if its adapter says so
  (`http::Accepted::new(head, body, streams)`; the HTTP adapter sets it
  for `Upgrade`): its client edge resolves `accept` at once, and the
  inputs come after the head, one per data frame (`accept` returns
  `Some(inputs)`, 0020). Any other request is whole: its body, even empty, is
  one input in its origin, so a quiet stream such as an MCP `GET` is an
  ordinary request.
- `on_client` runs at the head, as waiting for more would deadlock a
  client waiting for its answer. For a streaming request `cx.input` is an
  `INTERNAL` `Err`.
- Filters are the one way to edit inputs, head's and later ones alike,
  the first added first. A whole request's input stays one, as dropping
  it is refusing, `Err`'s job. A filter added after the inputs ended sees
  none, then their end.
- `cx.take_inputs::<T>()` reads the inputs as a stream. Later inputs are
  read once, by it or one call's body; they are taken when first awaited,
  and a reader that read no frame gives them back, so a retry gets them.
- `Call` is `'static`, `Clone` and `Sync`, sent when awaited, so a bridge
  clones one call per message.
- A call carries the request's inputs; its `dial` gets them as `later`
  (for HTTP, the request encoder is told they stream, and one that cannot
  carry them fails `Encoder::open` `CONFIG`).
- A streaming request's head goes when the reply resolves, and
  `until_content` returns at once for a call that streams its inputs.
- In a streaming request each data frame is one message both ways.
  Message types, close codes, keys and pings belong to the adapter or
  transport.
- Conversation structure belongs to plugins: a relayed WebSocket is one
  call; a bridge streams a response and calls per message.
- When the request ends, the inputs left are cut off and their filters
  end `abandoned`.

## Consequences

- No new public types or entry points.
- A relayed stream is one call, so one `on_provider`; a refusal per
  message needs a bridge.
- Routing on a client-first frame is not possible in `on_client`, so a
  streaming request routes by its head.
- Streamed inputs a call has read are gone, so no fallback once they flow.
- A plugin calling `input()?` unchecked refuses streaming requests: it
  fails closed, and the bug shows.

## History

- 0008: a frame's message is one value carrying its bytes.
- 0011: `ready` became `until_content`.
- 0012: the adapter, not `Upgrade`, says whether a request streams; a 101
  is the formats' and the adapter's.
- 0015: it said so with `Exchange::streams`, not an argument of `handle`.
- 0020: the core has no flag: `accept` returns `Some(inputs)` if inputs
  follow the head and `None` if not; `Exchange::streams` is HTTP's.
