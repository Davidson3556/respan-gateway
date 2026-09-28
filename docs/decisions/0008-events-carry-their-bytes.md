# 0008. Events carry their bytes

- Status: accepted, amended by [0009](0009-core-cleanups.md), [0010](0010-usage-out-of-core.md)
- Date: 2026-09-28

## Context

A body decoded into `Part`s: an `Item` of erased values with the bytes
they came from, bytes no value came from, usage, or the end. So the kernel
copied each item to tell an edit, stripped another format's bytes on
translation, held usage for the next item, ran usage filters, split an
item per upgrading frame, and ignored Parts after the end: about a dozen
rules with no principle of their own, and codecs that downcast at run
time. This amends 0003, 0004, 0005, 0006 and 0007.

## Decision

- A body is a stream of messages, each decoded into one value of the
  kind: `Decoder<T>` and `Encoder<T>` of the format's `Input` or `Output`.
  A body's end is `decode`'s or `end`'s `Result`, written by
  `Encoder::end(&Result)`. A decoder ends only at the end of its body and
  says whether it was complete (`Fault::truncated()`).
- Every message decodes to one value, even one the kind cannot read,
  which keeps the bytes it came in (`formats::relayed`). A format writes a
  value as those bytes while it says the same, and writes what changed
  into them. The kernel keeps, strips and compares no bytes; keeping them
  is each kind's obligation, stated in `Format`'s documentation. A
  provider's error body keeps its format's name
  (`Fault::with_raw(format, raw)`), as only that format writes it.
- `Format::says`: a value a reader sees nothing in, such as a keep-alive,
  framing or usage alone, passes no filter, is no content for `ready`, and
  is written where it came; `outputs` still yields it
  ([0009](0009-core-cleanups.md)). *Amended by 0010:* renamed
  `Format::is_content`, it decides only what `ready` commits on; filters
  see every value.
- Usage is what a provider's messages report (`Format::usage`), counted as
  decoded, on the call's provider edge, before any filter: a failure stops
  writing, never counting. It is written as the value says after filters,
  and read in `Outcome.usage`. What a plugin makes or relays is shown,
  never counted ([0009](0009-core-cleanups.md)). *Amended by 0010:* the
  kernel counts no usage; `Format::usage`, `Outcome.usage` and core's
  `Usage` are gone.
- An input, first or later, that cannot be written is the client's fault
  (`BAD_REQUEST`) if filters left it equal to its decoding, else `CONFIG`;
  the first is not retried on a fresh encoder.
- Traffic the gateway does not read is a kind of one format,
  `formats::opaque::{Sent, Got}`, generic over the format, so it relays
  only to itself, byte for byte.
- `request_encoder(request, first)` gets the client's bytes as the body
  when the call relays a whole request no plugin changed, which stays
  unread, and may edit head and body; `first` is the input when it is
  written from its value.
- In the conversation kind a `Reply` is one message: its start, content,
  finish and usage.

## Consequences

- 14 public types; `Format` has 10 members. Codecs are typed, so no kind
  mismatch reaches them.
- Usage edits no longer count, so a kind-neutral usage filter, such as a
  markup, is gone. A cost cap reserves before the call (`on_provider`),
  settles from `Outcome.usage`, and cuts a stream with a provider-edge
  filter that estimates from text. A meter once saw each increment, but
  providers report output usage at a stream's end, so it could not cap
  one mid-flight; it is gone. An estimate a plugin makes reaches no
  `Outcome`.
- Byte-exactness survives only in kinds that keep their origin; a decoder
  that drops a message it cannot read loses it silently. A frame may
  decode to several inputs.
- A filter drops whole messages, so dropping one drops a finish or usage
  it carried; a filter that means to keep them edits the message instead.
- A value a filter holds and releases keeps its bytes and can go out of
  place, so a stream format keeps its framing well formed itself
  (Anthropic holds events before its `message_start`, and a block's stop
  until the block starts).
- A call settles at the end of its body, not its terminator: a response
  read with `ready` and dropped before its end ends abandoned, and a
  client-edge failure ends the call however the bytes fell.
- A copy of the client's input sent as a call's own keeps its origin, so
  a format of the client's writes it into the client's bytes.
- A request encoder tells an upgrading call by the `Upgrade` a relay
  passes on; one translating an upgrading request has neither a first
  input nor a body to tell it by.
- A provider's error event in a stream, or an error in a 200, goes as it
  came to a client of the same format; to another, in the client's words,
  as a refusal must be.

## History

- 0009: `Response::with_usage` and counting a plugin's own bytes went, and
  `outputs` stopped hiding values that say nothing.
- 0010: usage left the kernel; `Format::says` became `Format::is_content`,
  and filters see every value.
