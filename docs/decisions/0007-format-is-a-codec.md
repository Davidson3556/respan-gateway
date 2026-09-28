# 0007. A format is one protocol's codec

- Status: accepted, amended by [0008](0008-events-carry-their-bytes.md)
- Date: 2026-09-27

## Context

A format wrote responses with a codec per stream, but requests with
functions of one message, a `prepare` that built the call, and a JSON view
of a body. A protocol whose messages depend on earlier ones, such as a
WebSocket's session ids, had nowhere to keep state; two places wrote a
call's body, and three rules had no principle of their own. This amends
0003, 0004 and 0006.

## Decision

- A format is one protocol's codec, four factories beside `name` and
  `accepts`: a request and a response decoder and encoder. Format and
  transport are stateless and shared; each body, either way, gets its own
  decoder or encoder, which keeps that stream's state.
- A codec sees the message it reads or writes and the request of the
  exchange, `http::Request<Bytes>`, its body empty for an upgrading
  request. The client's request decoder gets the request as it arrived;
  the response decoder the call as sent; the response encoder the
  client's request, its head as plugins left it. Only `Gateway::handle`
  and `Transport::send` see bodies.
- Heads are edited, never built. The kernel fills a call with the
  client's method and path and the call's extensions; the request encoder
  edits it, as the response encoder edits the response head. What the
  format sets is its own, and it labels a head without a content type
  (RFC 9110 8.3).
- Factories never fail: a failure travels in the stream, and an encoder
  that cannot write its head fails its first encode, which the kernel
  runs before sending.
- A whole request's input is decoded only when read; an upgrading
  request's frames are decoded as they pass, as its decoder keeps state.
- A whole call is one frame of exact size; an upgrading call's encoder is
  its stream, fed each later input.
- A format is never told whether it relays. The kernel knows only who
  relays heads: a translated head keeps its status and `Retry-After`.
- The client's own message in its own format goes as a proxy sends it,
  with the client's query and the end-to-end headers. Not: the
  connection's (RFC 9110 7.6.1), keeping `Upgrade`; `Host`; `Expect`;
  the `Forwarded` family (RFC 7239); conditions and ranges (RFC 9110 13,
  14), which ask of the client's copy; method overrides; cookies; and
  `Accept-Encoding`, as the gateway decodes what it reads.
- Content that cannot be written is whose it is, by one rule for every
  input, first or later: `BAD_REQUEST` if filters left it equal to its
  decoding, else `CONFIG`, as is a call's own input. (The first input
  was once retried as the client's on a fresh encoder; 0008 dropped
  that.)
- Bodies are `Bytes` (`cx.body()`, `edit_call`), as the kernel does not
  know JSON.

## Consequences

- A stateful protocol keeps its state in its codecs.
- A plain relay decodes nothing: OpenAI probes two fields of the body. A
  17 KB chat relayed whole takes about half the time it did before.
- A relayed WebSocket is decoded as it passes, so a frame its format
  refuses aborts the call even if no plugin reads it.
- A translated call carries none of the client's headers; one the
  provider needs is the format's to set, or `edit_call`'s.

## History

- 0008: codecs became typed (`Decoder<T>`, `Encoder<T>`) over one value
  per message instead of erased `Part`s; the request codecs see
  `http::Request<Bytes>`; the kernel no longer strips another format's
  bytes or tells edits, as each value keeps its own.
