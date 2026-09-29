# 0008. Events carry their bytes

- Status: accepted, amended by [0009](0009-core-cleanups.md), [0010](0010-usage-out-of-core.md), [0011](0011-read-until-content-or-end.md), [0012](0012-http-out-of-the-kernel.md), [0014](0014-api-trims.md), [0020](0020-symmetric-edges.md) (no head hooks)
- Date: 2026-09-28

## Context

A body decoded into erased parts: values with the bytes they came from,
bytes no value came from, or the end. The kernel copied each to tell an
edit, stripped another format's bytes on translation and split parts per
frame: about a dozen rules with no principle, and codecs that downcast at
run time.

## Decision

- A body is a stream of messages, each decoded into one value of the
  kind: `Decoder<T>` and `Encoder<T, H>` of the format's `Input` or
  `Output`. A body's end is `decode`'s or `end`'s `Result`, written by
  `Encoder::end`. A decoder ends only at the end of its body and says
  whether it was complete (`Fault::truncated()`).
- Every message decodes to one value, even one the kind cannot read,
  which keeps the bytes it came in (`formats::relayed`). A format writes a
  value as those bytes while it says the same, and writes what changed
  into them. The kernel keeps no value's bytes; the one thing it keeps
  unread is the client's request body, which it hands only to formats; a
  plugin reads the bytes an input came in through its origin (0020).
- A provider's error answer keeps its format's name
  (`Fault::with_answer(format, answer)`, for an error event within a
  response too), as only that format writes it.
- `Format::is_content` decides only what `until_content` commits on.
- An input, first or later, that cannot be written is the client's fault
  (`BAD_REQUEST`) if filters left it equal to its decoding, else `CONFIG`.
- Traffic the gateway does not read is a kind of one format,
  `formats::opaque`, so it relays only to itself, byte for byte.
- The request encoder gets the client's bytes as the body when the call
  relays a whole request no plugin changed, which stays unread.
- In the conversation kind a `Reply` is one message: its start, content,
  finish and usage.

## Consequences

- Codecs are typed, so no kind mismatch reaches them.
- Byte-exactness survives only in kinds that keep their origin; a
  decoder that drops a message it cannot read loses it.
- A filter drops whole messages, so dropping one drops a finish or usage
  it carried.
- A value a filter holds and releases keeps its bytes and can go out of
  place, so a stream format keeps its framing well formed itself.
- A call settles at the end of its body, not its terminator.
- A copy of the client's input sent as a call's own keeps its origin.
- A provider's error event goes as it came to a client of the same
  format; to another, in the client's words.

## History

- 0009: `Response::with_usage` went; `outputs` yields every value.
- 0010: usage left the kernel; `says` became `is_content`, and filters
  see every value.
- 0011: `ready` became `until_content`.
- 0012: encoders are told a call streams.
- 0012-0015: an error answer or event is `with_answer(format, answer)`;
  `with_raw`, `with_words` and the `in_place` flag came and went.
