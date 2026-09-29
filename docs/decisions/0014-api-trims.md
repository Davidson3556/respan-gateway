# 0014. API trims

- Status: accepted
- Date: 2026-09-28

## Context

The core had methods that said the same thing twice, or that one call
could carry.

## Decision

1. **`Extensions`** loses `len`, `is_empty` and `clear`, which nothing
   used; `Exchange::with_extensions` goes, as `extensions` is a public
   field.
2. **A call's extensions are read as `px.client().extensions()`.**
   `ProviderContext::extensions` goes (0009 kept it). `extensions_mut`
   stays, the one way to write a call's own map: its transport and end
   hooks read what `on_provider` puts there (a ledger's hold, a tenant's
   `CallHeaders`, a tally), and `client()` is `&` so that no provider
   hook can `call` or `take_inputs`.
3. **`Encoder::open` returns `Ok(true)` for a body done before it
   began**, in place of `Encoder::done` (0012). `Encoder` has four
   methods.
4. **`Response::until_end` returns the end with the held response**:
   `Ok((response, end))`, `end` `Ok` for a call that streams the
   request's inputs, which returns at once. `Response::end` (0011) goes.
5. **`Fault::with_answer(format, answer, in_place)`** replaces
   `with_answer` and `with_words` (0012). `in_place` is what
   `is_answer` reports: the provider answered with it in place of a
   response. An error within a response, such as an error event, passes
   `false`.

## Consequences

- Eight fewer public methods; `Encoder` implementations fold `done` into
  `open`, and delegating encoders can no longer forget to forward it.
- A ledger still charges nothing for `is_answer` and its reserve for an
  error event.
- With `Until::End`, `sdks/routing` judges a success by the end
  `until_end` returned.
