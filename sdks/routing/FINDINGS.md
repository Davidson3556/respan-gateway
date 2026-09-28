# Findings, round 11 (core after PLAN-r10)

Score: 8/10

**Port**
- No source changes.

**Old findings**
- **The two-path finding (`whether_prepare_sees_the_input_depends_on_another_plugins_read`):** fixed by T10-6.
  - The probe now shows `[(None, true), (None, true)]`: `prepare` gets `None` whether or not another plugin read the input.
  - Renamed to `whether_a_plugin_read_the_input_does_not_change_what_prepare_sees`. It now asserts `None` on both paths and that the path is the client's.
- **K9-8, R8-A, Y7-1, R-3, X7-1, F2, R-2, N7, N8, X6-1:** fixed; they still pass.
- **R8-B:** accepted (V9-2), unchanged.
- **R-1:** accepted.
- **F3:** open (rejected). Still fails under `--ignored`.
- **F10:** open (formats crate). Still fails under `--ignored`.
- **F9, F11, F12, F14, N4, N6/F8, and the latent M-4 pattern in `smart.rs` and `attachments.rs`:** unchanged.
- **The V9-1 counterfactual pin:** unchanged.

**New findings** (round-10 section of `tests/r9.rs`)
- **Works:** a filter that leaves the input equal gives `prepare` `None` and the client's path.
- **Works:** a model changed on the provider edge (`px.filter`) or by one call's own `with_filter` reaches `prepare` as `Some` and changes the path.
- **N10-R1, low (docs):**
  - Here a changed input encodes to the client's own bytes, because the model lives only in the path. Yet `prepare` still gets `Some` with `relayed = true`.
  - The real rule is "the input differs from the client's". The rustdoc says "the body is not the client's own bytes", and `relayed` says "relays the client's own body". Taken literally, both are wrong for a changed relay.
  - The behavior is right; only the wording is off. It should state the one rule.
  - Metric: no type or term changes.
