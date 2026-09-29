# 0014. API trims

- Status: accepted, amended by [0020](0020-symmetric-edges.md) (`Encoder` is `http::Encoder`)
- Date: 2026-09-28

## Context

The core had methods that said the same thing twice, or that one call
could carry.

## Decision

1. **`Extensions`** loses `len`, `is_empty` and `clear`, which nothing
   used; `Exchange::with_extensions` goes, as `extensions` is a public
   field.
2. **A call's own extensions: `px.extensions()` reads and
   `px.extensions_mut()` writes** (0015 restored the reader). Its
   transport and end hooks read what `on_provider` puts there (a
   hold, a tenant's `CallHeaders`, a tally), and `client()` is `&` so
   that no provider hook can `call` or `take_inputs`.
3. **`Encoder::open` returns `Ok(true)` for a body done before it
   began**; there is no separate method. `http::Encoder` has four methods.
4. **`Response::until_end` returns the end with the held response**:
   `Ok((response, end))`, `end` `Ok` for a call that streams the
   request's inputs, which returns at once. There is no separate accessor.
5. **`Fault::with_answer(format, answer)`** is the one way to give a
   fault a provider's words, for an error answer and for an error event
   within a response alike (0015 dropped a flag this record had added).

## Consequences

- Fewer public methods; `Encoder` implementations fold the done check
  into `open`, and delegating encoders can no longer forget to forward it.
- A call refused before it began (`is_head_only`, 0015) is charged
  nothing by the ledger; a failure after a value, at least its reserve.
- With `Until::End`, `sdks/routing` judges a success by the end
  `until_end` returned.

## History

- 0015: `px.extensions()` is back; `with_answer` lost its flag.
