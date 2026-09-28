# 0006. Streams both ways

- Status: accepted, amended by [0008](0008-events-carry-their-bytes.md), [0011](0011-read-until-content-or-end.md), [0012](0012-http-out-of-the-kernel.md)
- Date: 2026-09-27

## Context

0003 planned a second entry point for WebSockets and quiet streams, with a
splitter per format and hooks per exchange. But a WebSocket is a request
whose messages come after its head, and a response already streams; only
a streaming request was missing. This amends 0003 and 0004.

## Decision

- No separate concept for long-lived exchanges: a request carries inputs,
  a response outputs, and either may stream.
- A request streams its inputs if and only if it is upgrading (`Upgrade`,
  RFC 9110 7.8): they come after its head, one per data frame, split by
  the adapter, which removes an `Upgrade` it will not perform. Any other
  request is whole: its body, even empty, is one input, so a quiet stream
  such as an MCP `GET` is an ordinary request. *Superseded by 0012:* it
  streams if the adapter says so (`handle(req, streaming)`); the HTTP
  adapter says so for `Upgrade`.
- `on_client` runs at the head and the input that came with it, as
  waiting for more would deadlock a client waiting for the 101. For an
  upgrading request `cx.input` is an `INTERNAL` `Err`.
- Filters are the one way to edit inputs, so the head's input and later
  ones take one path, the first added first. A whole request's input
  stays one, so dropping it is refusing, `Err`'s job; a filter added any
  time is applied and ended at once. One added after an upgrading
  request's inputs ended sees none, then their end.
- `cx.take_inputs::<T>()` reads the inputs as a stream. Later inputs are
  read once, by it or one call's body, so nothing buffers; they are taken
  when first awaited, and a reader that read no frame gives them back, so
  a retry after a failed call gets every input.
- `Call` is `'static`, `Clone` and `Sync`, sent when awaited, so a
  response stream clones one call per message and plugins share one
  without a lock.
- A call carries the request's inputs, later ones only if the head its
  format writes upgrades too, else it fails `CONFIG`, as a whole call is
  sent at once. *Superseded by 0012:* the format is told the call
  streams, and one that cannot carry it fails `Encoder::open` `CONFIG`. `Transport::send` takes `http::Request<Body>`, whole or
  streaming.
- An upgrading request's head goes at once, so the client's encoder makes
  a plugin's success a 101, and `ready` returns on a 101 at once.
  *Amended by 0012:* a streaming request's head goes at once, and
  `until_content` returns at once for a call that streams its inputs; a
  101 is the formats' and the adapter's.
- In an upgrading request each data frame is one message both ways.
  Message types, close codes, keys, compression and pings belong to the
  adapter or transport, which own the framing.
- Conversation structure belongs to plugins: a relayed WebSocket is one
  call; a bridge streams a response and calls per message.
- When the request ends, the inputs left are cut off and their filters
  end `abandoned`, so nothing kept past it holds the client's connection.

## Consequences

- No new public types or entry points.
- A relayed stream is one call, so one `on_provider`, with no refusal per
  message; that needs a bridge.
- Routing on a client-first frame, such as Gemini Live's setup, is not
  possible in `on_client`, so an upgrading request routes by its head
  ([0003](0003-kernel-contracts.md#kinds)).
- Streamed inputs a call has read are gone, so no fallback or hedge once
  they flow.
- A plugin calling `input()?` unchecked refuses upgrading requests with a
  500: it fails closed, and the bug shows.

## History

- 0008: a frame's message is one value carrying its bytes, so "an input a
  filter releases at the end has no bytes" and splitting a frame into
  one item per value went.
- 0012: the adapter, not `Upgrade`, says whether a request streams.
