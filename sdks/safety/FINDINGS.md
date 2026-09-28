# Findings, round 11 (core after PLAN-r10)

Score: 9/10

**Port**
- No source changes.

**Old findings**
- **S7-1, X7-1, S-R2, S-N1, S-N3, S-N6, X6-1, K9-5:** fixed; they still pass.
- **F13:** by design; the workaround still works.
- **S-N2:** open (accepted).
- **F2, F7:** open (rejected). Still fail under `--ignored`.
- **F6:** open (rejected).
- **F4, F8, F10:** open (formats crate). Still fail under `--ignored`.
- **F5:** open (formats crate).
- **S-N4, S-N5:** unchanged.
- **S-R1:** open (the price of Q-4).

**New findings** (`tests/r10.rs`, and the round-10 section of `tests/streams.rs`)
- **T10-5 works:** a panicking input filter, output filter on the first item, or head edit gives exactly what its `Err` twin gives: a 500 `internal` refusal before any byte. The input case sends nothing to the provider.
- **T10-5 works:** a holding filter whose `end` panics matches its `Err` twin. What it released is dropped, and the end is `internal`. Both still send a 200 and the relayed role chunk first, but that is F10, not the panic.
- **T10-1 works:**
  - An input filter added after an upgrading request's inputs ended sees no values and gets the stored end (`Ok`).
  - After a guard's failure, the late filter gets the stored `BLOCKED`, adding it returns that error, and the request ends `BLOCKED`.
- **Pinned, by design (F2 family):** a guard ordered after a plugin that drained the inputs becomes a late filter. It checks nothing, adding it succeeds, and the request ends `Ok` with "bomb" passed unchecked. The stored end tells the guard the inputs ended, not that it missed them. Not a new finding.
