# 0026. Bound ready batches and drive caller readiness during opening

- Status: accepted
- Date: 2026-09-29
- Amends the batch discussion in 0023 and the execution contracts in 0024/0025.

## Context

An audit reproduced two failures. Coalescing every immediately ready
provider event consumed 10,000 events before returning first content;
an endlessly ready stream could keep the task polling forever. The same
reader runs during output disposal, so failed delivery preparation also
consumed all 10,000 events. Separately, a caller binding whose receive
opening needed send-sink readiness stalled because only receiving was polled.

## Decision

Coalesce at most 128 ready provider stream items per batch. Yield when a
batch fills without an end, including when a consumer immediately requests
more batches. A terminal batch transfers directly to its owner so delivery
errors retain their precedence over a buffered provider end.
The limit is internal and bounds event coalescing, not message sizes or work
performed by user filters. Provider filters still process each entire
batch before client filters receive it. Large protocol frames may span
batches; accounting must not assume that unread later batches are metered
after a failure. Explicit `until_end` buffering remains the caller's choice.

Keep the existing completion behavior of output disposal for now. Its read
attempt is subject to the same batch limit; it cannot drain an unbounded
ready stream in search of an end. Removing that read requires a separate
completion redesign.

While caller receiving opens, also poll send-sink readiness. Remember a
successful readiness check until submission instead of polling it again.
Receiving may finish first, allowing hooks and input decoding to unblock
readiness. Readiness failure or panic before opening returns a fault and
drops both halves. An opening failure still goes to the sink as a refusal.
Completed opening futures are released before delivery. Cancellation drops
both halves and releases the gateway's in-flight count.

## Validation

Regression tests cover bounded first-content reads and output disposal,
yielding and cancellation, ordering and filters across batches, final
success/failure and exactly-once provider completion, send-side handshake
progress, readiness failure/panic, cancellation while opening, and receiving
before send readiness. Existing HTTP, WebSocket, metering and ledger tests
remain required.
