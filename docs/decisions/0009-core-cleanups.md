# 0009. Core cleanups

- Status: accepted, amended by [0010](0010-usage-out-of-core.md), [0011](0011-read-until-content-or-end.md), [0012](0012-http-out-of-the-kernel.md), [0014](0014-api-trims.md)
- Date: 2026-09-28

## Context

An audit of `crates/core` found features or policy written as kernel
rules, each a job that plugins, extensions or transports already do.

## Decision

1. **`on_provider` waits for no earlier call's end hooks.** The wait
   delayed a fallback's next attempt and could deadlock. `on_client_end`
   still runs after the calls' `on_provider_end`. *Instead:* a budget
   reserves in `on_provider` before awaiting and settles in its end hook.
2. **`Response::outputs` yields every value**, keep-alives and usage
   alone included. *Instead:* a reader filters with the kind's own check
   (`Reply::is_content`).
3. **`Call::with_filter` is gone**; it duplicated `on_provider`.
   *Instead:* `cx.call(p).with_extension(Mark)`, then in `on_provider`:
   `if px.extensions().get::<Mark>().is_some() { px.filter(f)? }`.
4. **Cookies and forwarding headers are the transport's to strip**
   (`http::strip_client_origin_headers`), as the core has no notion of
   origins.
5. **One default target.** `Gateway::target` sets one provider, replaced
   when given again. *Instead of a default by kind:* a plugin,
   `if cx.format() == "anthropic-messages" { cx.set_target("anthropic") }`.
6. **No hidden client format.** Every output a plugin makes
   (`Response::stream`) is content. *Instead:* filter out what says
   nothing before `Response::stream`.

`px.extensions()` stays beside `px.client().extensions()`.

## Consequences

- A fallback's next attempt may see the last attempt's hold still in a
  ledger.
- A custom transport that forwards everything sends the client's cookies
  and forwarding headers.

## History

- 0010: the usage rules here (only a provider's response counts,
  `Response::with_usage` gone) became "the kernel counts no usage".
- 0012: `TE` stripping and bodiless responses moved to `crates/http` and
  the formats (`Encoder::done`); the header helpers moved from the server
  crate to `crates/http`.
- 0014: `px.extensions()` went; a call's are read as
  `px.client().extensions()` and written with `px.extensions_mut()`.
