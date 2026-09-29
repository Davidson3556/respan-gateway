# 0015. Core trims

- Status: accepted, amended by [0017](0017-untyped-handle.md) (`handle` is untyped)
- Date: 2026-09-28

## Context

The owner allows no new public type in the core. A review found facts
the kernel keeps in the wrong place: a marker type for one fact it
already knows, a flag on `Fault` that one SDK reads, a bundle of one
field, a flag beside the request it describes, and a read accessor
missing beside its writer. Public types go from 16 to 14.

## Decision

1. **`ProviderContext::extensions()`** reads the call's own map, the one
   `extensions_mut()` writes (amends 0009, 0014).
2. **`Exchange::relayed: bool` replaces `Relayed`** (0012, 0013). The
   kernel sets it on a call, and only when the call sends the client's
   inputs, not its own, to a provider whose format has the client
   format's name; it is false on a request, response and a translated or
   own-input call. No plugin can set it: `Call::with_extension`,
   `edit_call` and `on_provider` reach extensions and heads, not the
   flag. `http::credentials(&mut Exchange<Ask>, &HeaderMap)` reads it, so
   a plugin removing `Credential` still stops pass-through, and
   `Extensions` are read by no one in the kernel.
3. **`Exchange::streams: bool` replaces `handle`'s `streaming`**
   (amends 0012). It means later frames follow the head. The adapter sets
   it on a request; the kernel sets it on a call that streams the
   client's inputs. `Gateway::handle(req)` takes it from the request. `HttpTransport` sends a call whole or streamed by it, not
   by its body's size hint. (0017: `handle` takes and gives erased heads.)
4. **`Fault` loses `in_place`, `is_answer` and `answered`** (amends 0008,
   0011, 0012, 0014). `with_answer(format, answer)` and `answer(format)`
   stay: a same-format refusal relays the provider's bytes. Whether a
   provider refused before beginning a response is not a fact of a
   fault, and the ledger cannot read it from what it sees: a provider's
   429 or 500 and an error event, or a stream cut, are each a fault with
   a code and an untouched tally, and a refusal carries the status of
   its head just as a cut carries a 502. So the kernel says it on the
   context, beside `is_sent`: **`ProviderContext::is_head_only()`**,
   true once the provider's head arrived and its body, not broken off by
   the transport, decoded no value. `sdks/ledger` charges as before: not
   sent, 0; sent with no head, at least the reserve; `Err` with
   `is_head_only`, unless it is `abandoned`, `truncated` or `internal`, 0;
   ended `Ok` and reported, the tally; else at least the reserve. A
   provider's 429 or 500 is never billed. Where a provider answers 200
   and says an error, or a stream's first event is an error, before any
   value, the call is now charged 0 where it was charged the reserve.
   `sdks/routing` retries by whether the call got a response: a failure
   of the response is retried as its status says, unless it is
   `internal`, `config` or `abandoned`; a call not made, only `upstream`.
5. **`Outcome` goes** (amends 0003). `on_client_end(&self, cx, end:
   &Result<(), Fault>)`, `on_provider_end(&self, px, end)`, and
   `handle`'s future yields `Result<(), Fault>`. A per-call fact beyond
   the end goes on the context, as `is_sent` and `is_head_only` are, not
   on a bundle.

## Consequences

- 14 public types (0013 said 16): `Relayed` and `Outcome` are gone.
- Every plugin's end hooks take `end` in place of `outcome`;
  testkit's `Outcomes` and `Calls` record `Result<(), Fault>`.
- A custom HTTP transport calls `credentials(&mut call, &headers)`, and
  sends a call whole unless `call.streams`.
- `Fault::with_answer_of` stays crate-private: a filter that fails a
  response's end keeps the provider's answer and retry.
