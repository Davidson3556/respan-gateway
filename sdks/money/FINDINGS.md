# Findings, round 11 (core after PLAN-r10)

Score: 8/10

**Port**
- No source changes.
- One test changed: `a_client_filter_that_panics_mid_stream_leaves_the_call_estimated` now expects 200. A filter panic is now handled like its `Err` (T10-5). "Hello" passed before the panic in the same batch, so the head goes out with it and the error follows inside the stream. The Estimated charge is unchanged.

**Old findings**
- **M-5, F1, F1b, F3, F6–F9, F11, F7-2, F7-6, X7-1, K9-1, K9-7, the Filter::end doc:** fixed; they still pass.
- **M-4:** open (accepted as V9-4), unchanged.
- **F4:** open (promised). Still fails under `--ignored`.
- **F5:** open (rejected). Still fails under `--ignored`.
- **"Harder, low" (`on_client_end` can't price an upgrading answer):** unchanged. An upgrading request has no first input, so `prepare` still gets `None`.
- **Pinned, low (an output filter's failing `end` makes the call's `Outcome.end` partial):** unchanged; it never undercharges.
- **F2, F10, M-1, M-2, M-3, M7-1:** unchanged.

**New findings** (`tests/r10.rs`)
- **T10-5 works:** a client-edge filter that panics and its `Err` twin give the client identical bytes and the budget an identical charge.
- **T10-5 works for usage:** a usage filter that pushes the increment and then panics counts it, just as with `Err`.
- **T10-3 works:** a plugin relays cached SSE through `cx.relay`, and an output filter fails on " world". The usage that follows in the same frame is still counted on the client edge, so the answer is charged in full (`STREAM_COST`) and the usage is not written.
- **N10-1, medium-low (core-caused):**
  - The one `Filter` impl core provides, for closures, pushes the value only after the closure returns.
  - So a closure usage filter that returns `Err` passes the increment on, and it counts.
  - One that panics passes nothing. The call's `Outcome.usage` is empty, and the budget charges its estimate (50) instead of the reported usage topped up (80).
  - This breaks two rustdoc promises: "a panic … is an internal fault, as its `Err`" and "a failure stops writing, never counting".
  - Fix: in the closure impl, push first and edit in place (`out.push(value); self(out.last_mut())`).
  - Tests: `a_closure_usage_filter_that_panics_counts_as_one_that_fails` (ignored, fails) and the `today_…` pin.
  - Metric: no type or term changes.
