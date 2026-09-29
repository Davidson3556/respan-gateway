# 0015. Core trims

- Status: accepted, amended by [0020](0020-symmetric-edges.md) (`Exchange`, `relayed` and `streams` are HTTP's; the core derives both facts)
- Date: 2026-09-28

## Context

The owner allows no new public type in the core. A review found facts
the kernel keeps in the wrong place: a marker type for one fact it
already knows, a flag on `Fault` that one SDK reads, a bundle of one
field, a flag beside the request it describes, and a read accessor
missing beside its writer. Two public types go.

## Decision

1. **`ProviderContext::extensions()`** reads the call's own map, the one
   `extensions_mut()` writes (amends 0009, 0014).
2. **`Exchange::relayed: bool` replaces `Relayed`** (0012, 0013), on
   HTTP's `Exchange` (0020). It is set only when the call is written from
   the client's origin, which the core hands over only when the call sends
   the client's inputs, not its own, to a provider whose format has the
   client format's name; it is false on a request, response and a
   translated or own-input call. No plugin can set it: `Call::with_extension`
   and `on_provider` reach extensions, not the flag, and no plugin
   reaches a head. `http::credentials(&mut Exchange<Ask>, &HeaderMap)`
   reads it, so a plugin removing `Credential` still stops pass-through.
3. **`Exchange::streams: bool` replaces `handle`'s `streaming`**
   (amends 0012), on HTTP's `Exchange`. It means later frames follow the
   head. The adapter says it of a request (`Accepted::new`); the driver
   sets it on a call whose inputs are still flowing. The core has no such
   flag: `accept` says whether inputs follow the head (0020). `HttpTransport` sends a call whole or streamed by it, not by
   its body's size hint.
4. **`Fault` loses `in_place`, `is_answer` and `answered`** (amends 0008,
   0011, 0012, 0014). `with_answer(format, answer)` and `answer(format)`
   stay: a same-format refusal relays the provider's bytes. Whether a
   provider refused before beginning a response is not a fact of a fault:
   a provider's 429 or 500, an error event and a cut stream are each a
   fault with a code and an untouched tally. The kernel says it on the
   context, beside `is_sent`: **`ProviderContext::is_refused()`**, true
   once the provider's head arrived and its body, not broken off by the
   transport, decoded no value.
5. **`Outcome` goes** (amends 0003). `on_client_end(&self, cx, end:
   &Result<(), Fault>)`, `on_provider_end(&self, px, end)`, and
   `handle`'s future yields `Result<(), Fault>`. A per-call fact beyond
   the end goes on the context, as `is_sent` and `is_refused` are, not
   on a bundle.

## Consequences

- `Relayed` and `Outcome` are gone: two public types fewer.
- `is_sent` and `is_refused` are facts of a call's context: the call was
  handed to its transport; the provider's head arrived and its body began no
  value.
- `sdks/ledger` charges by them: not sent, 0; sent with no head, at least
  its reserve; `Err` with `is_refused`, unless `abandoned`, `truncated` or
  `internal`, 0; ended `Ok` and reported, the tally; else at least the
  reserve. A provider's 429 or 500 is never billed. Where a provider says an
  error with 200, or a stream's first event is an error, before any value,
  the call is charged 0, where it was charged the reserve.
- `sdks/routing` retries by whether the call got a response: a failure of
  the response as its status says, unless it is `internal`, `config` or
  `abandoned`; a call not made, only `upstream`.
- Every plugin's end hooks take `end` in place of `outcome`;
  testkit's `Outcomes` and `Calls` record `Result<(), Fault>`.
- A custom HTTP transport calls `credentials(&mut call, &headers)`, and
  sends a call whole unless `call.streams`.
- `Fault::with_answer_of` stays crate-private: a filter that fails a
  response's end keeps the provider's answer and retry.
