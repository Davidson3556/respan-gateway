# 0019. Status out of the kernel

- Status: accepted
- Date: 2026-09-29

## Context

After 0013 the kernel read no HTTP head but `Fault` still held a `u16`
status in HTTP's numbering, and the kernel wrote 400, 404, 499, 500 and 502
itself. The kernel named codes and numbers both, and only one of them is
neutral.

## Decision

1. **`Fault` has a code, not a status.** `Fault::new(code, message)`; the
   kernel constructors (`bad_request`, `not_found`, `upstream`, `truncated`,
   `config`, `internal`) are its vocabulary of codes. `Whose::fault` and
   `recoded` set only a code, and `recoded` clears the hints, which were of
   the cause it replaces.
2. **A `Fault` carries typed hints.** `Fault::with(value)` and
   `Fault::get::<T>()`, on the public `Extensions` in its box, so a `Fault`
   is still one pointer wide. The kernel never reads a hint. No new public
   type.
3. **HTTP's status is a hint defined in `crates/http`.** `Status(pub u16)`
   and `status(&Fault) -> StatusCode`: the hint if any, else a default per
   kernel code (`bad_request` 400, `not_found` 404, `upstream` and
   `truncated` 502, `abandoned` 499, `config` and `internal` 500), else 500,
   as for a number that is no status. The adapter, `error_head`, `plain` and
   the formats read it there.
4. **A provider's error answer** carries the provider's status as a `Status`
   hint, set by the format's decoder; `retry_after` stays a kernel
   `Duration`. A plugin writes
   `Fault::new("over_limit", msg).with(Status(429))`.
5. **SDKs** that name or read a status depend on `respan-gateway-http`: the
   ledger (`Status` for its 401 and 402) and routing (`status`, to tell a
   provider's retryable answer from its other 4xx).

## Consequences

- The kernel holds no HTTP number. The wire is unchanged: every status in
  the tests and the end-to-end suite is the same.
- `Fault::status` and `Fault::new`'s status argument are gone (amends 0013,
  0012 and 0003). `Format`, `Decoder`, `Encoder` and `Transport` did not
  change.
- Another wire adds its own hint type the same way.
- Equality ignores hints, as it does source and answer; it compared the
  status before.
