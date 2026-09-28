# Findings, round 11 (core after PLAN-r10)

Score: 8/10

**Port**
- No source changes.

**Old findings**
- **F8, Z8-5, MC-1, F1, R1, R3, R4, X6-1, X7-1:** fixed; they still pass.
- **Relayed DELETE:** still holds, and it is now one path.
  - `a_relayed_delete_keeps_its_verb_read_or_unread` now uses a recording wrapper around `StreamableHttp`.
  - With or without a plugin reading the input, `prepare` gets `None` and the call goes out as `DELETE`.
  - The round-9 two-path trap is gone: the "read" branch of `StreamableHttp::prepare` now runs only for changed, translated or a call's own inputs.
- **R5:** fixed by the idiom; the trap is unchanged.
- **F7:** open.
- **F10:** open (rejected). A format of no kind still carries a `prepare` input parameter it never receives.
- **R2:** open (accepted).
- **R8-C:** accepted (V9-3).
- **Opened stream can't be refused:** by design, unchanged.
- **`Response::stream::<ToClient, _>` turbofish:** minor, unchanged.

**New findings** (`tests/r10.rs`, `tests/aggregate.rs`)
- **T10-2 works:** a server's 204 to a `DELETE`, read with `ready()` and then dropped, ends its call `Ok`.
- **Pinned, consistent with the rustdoc:**
  - A 204 dropped *unread* also ends `Ok`. Core decodes a bodiless response's end as it arrives, so there is nothing left to abandon.
  - An empty 200 dropped unread ends `abandoned`, as documented.
- **Noted, trivial:** instrumenting a format means a wrapper with seven forwarding methods, since `impl Format for Arc` and any delegation helper are gone. Not a finding.
