# 0007. A format is one protocol's codec

- Status: accepted, amended by [0008](0008-events-carry-their-bytes.md), [0009](0009-core-cleanups.md), [0012](0012-http-out-of-the-kernel.md), [0013](0013-no-http-in-core.md), [0020](0020-symmetric-edges.md) (no head hooks)
- Date: 2026-09-27

## Context

A format wrote responses with a codec per stream, but requests with
functions of one message. A protocol whose messages depend on earlier
ones, such as a WebSocket's session ids, had nowhere to keep state, and
two places wrote a call's body.

## Decision

- A format is one protocol's codec: beside `name` and `accepts`, a
  request and a response decoder and encoder. Format and transport are
  stateless and shared; each body, either way, gets its own decoder or
  encoder, which keeps that stream's state.
- Codecs see the heads they need: the request decoder the client's head;
  the request encoder the client's head on a relay, and the call's first
  bytes to edit; the response decoder the answer and the call as sent;
  the response encoder the provider's head on a relay, the client's
  request and any refusal.
- Heads are edited, never built by the kernel: a relay's call starts from
  the client's head, any other anew; the encoder writes the head
  (`Encoder::head`).
- Factories never fail: a failure travels in the stream.
- A whole request's input is decoded only when read; a streaming
  request's frames are decoded as they pass.
- A whole call is one frame of exact size; a streaming call's encoder is
  fed each later input.
- A format is never told whether it relays, only handed the heads that a
  relay passes on.
- Content that cannot be written is whose it is, one rule for every
  input: `BAD_REQUEST` if filters left it equal to its decoding, else
  `CONFIG`, as is a call's own input.
- Bodies are `Bytes`, which only formats read, as the kernel does not
  know JSON.

## Consequences

- A stateful protocol keeps its state in its codecs.
- A plain relay decodes nothing. A 17 KB chat relayed whole takes about
  half the time it did before.
- A relayed WebSocket is decoded as it passes, so a frame its format
  refuses aborts the call.
- A translated call carries none of the client's headers; one the
  provider needs is the format's to set, or the transport's from the
  call's extensions.

## History

- 0008: codecs became typed over one value per message.
- 0009: cookies and the `Forwarded` family moved to the transport.
- 0012: the kernel stopped filling calls and keeping a translated head's
  status; the proxy's header list moved to `http::proxy_call`.
- 0013: codecs take their wire's heads, not `http` types.
