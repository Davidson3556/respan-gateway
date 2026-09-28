# 0009. Core cleanups

- Status: accepted, amended by [0010](0010-usage-out-of-core.md), [0011](0011-read-until-content-or-end.md)
- Date: 2026-09-28

## Context

With the meter gone, an audit of `crates/core` found more rules of its
kind: features or policy written as kernel rules, each with a job that
plugins, extensions or transports already do. This amends 0003, 0004,
0005, 0007 and 0008.

## Decision

1. **Only a provider's response counts usage**, on its call's provider
   edge. `Response::with_usage` and client-edge counting (a plugin's own
   bytes, `cx.relay`) are gone: they existed to bill cache hits, a
   billing feature, and extensions already carry per-request facts. The
   client edge's `Outcome.usage` is the sum of the request's calls'. We
   kept the field rather than drop it from the request's `Outcome`:
   `Outcome` stays one type for both edges, and `on_client_end` bills a
   request in one line. The sum is exact, as a request ends its calls
   before its own end. Bill on one edge, not both.
   *Back:* a cache stores each call's `Outcome.usage` from
   `on_provider_end` with its entry. On a hit it sets
   `cx.head_mut().extensions.insert(CachedUsage(u))`, which billing reads
   in `on_client_end`. *Amended by 0010:* the kernel counts no usage at
   all, so `Outcome.usage` is gone on both edges; a cache reads usage
   with a filter of its kind.
2. **`on_provider` waits for no earlier call's end hooks.** The wait held
   only within one request, did nothing for hedges, delayed a fallback's
   next attempt on logging hooks, and made waiting on what `on_client`
   holds a deadlock. `on_client_end` still runs after the calls'
   `on_provider_end`. *Back:* a budget reserves in `on_provider` before
   awaiting and settles in its end hook. It may also await its own
   pending releases for the request.
3. **`TE` is stripped like every connection header**, `te: trailers`
   included, even when `Connection` names it. Trailers are not delivered,
   so the exception could not serve gRPC. *Back:* nothing, until trailers
   are delivered (0003's promise), which brings `TE` back with them.
4. **`Response::outputs` yields every value**, keep-alives and usage
   alone included, so a bridge or merge can show usage. *Back:* a reader
   filters with the kind's own check (`Reply::says`, `Reply::is_content`
   since 0010).
5. **`Call::with_filter` is gone.** It duplicated `on_provider` and
   `px.filter`. *Back:* `cx.call(p).with_extension(Mark)`, then in
   `on_provider`: `if px.extensions().get::<Mark>().is_some() {
   px.filter(f)? }`, or key on `px.provider()`.
6. **Cookies and forwarding headers are the transport's to strip.**
   The kernel no longer drops `Cookie`, `Forwarded`, `Via`,
   `X-Forwarded-*` and `X-Real-IP`. That is origin and privacy policy,
   and the core has no notion of origins (0004). `HttpTransport` strips
   them (`strip_client_origin_headers`, next to its credential and
   `set-cookie` stripping), and plugins still see them in the head. The
   kernel still drops `Host`, `Expect`, `Accept-Encoding`, conditions,
   ranges and method overrides. *Back:* a custom transport calls
   `respan_gateway_server::strip_client_origin_headers`.
7. **One default target.** `Gateway::target` sets one provider, and a
   later call replaces it. A request it cannot take fails `CONFIG`
   through `cx.call`, like any call. Choosing a default by kind was
   routing. *Back:* a plugin:
   `if cx.format() == "anthropic-messages" { cx.set_target("anthropic") }`.
8. **A bodiless client response reads nothing more.** A 204, 205, 304 or
   response to a `HEAD` ends its call as decoded, as a provider's
   bodiless response's end already is, or `abandoned` if a body was left.
   The old reason, counting usage in a body no client gets, applied only
   when a plugin changed the status over a real body. *Back:* such a
   plugin reads the body (`outputs`) before it answers.
9. **No hidden client format.** `Response::stream` no longer captures the
   client's format from a task-local to judge content for `ready`: every
   output a plugin makes is content. A plugin knows what it makes.
   *Back:* filter out what says nothing before `Response::stream`.

`px.extensions()` stays. Merging it into `px.client().head().extensions`
would churn the SDKs and examples for no gain.

## Consequences

- `Response::with_usage` and `Call::with_filter` are gone; `Gateway::target`
  replaces; the server crate adds `strip_client_origin_headers`.
- A plugin that bills on both edges counts twice.
- A fallback's next attempt may see the last attempt's hold still in a
  ledger, as its settlement is no longer awaited.
- A custom transport that forwards everything sends the client's cookies
  and forwarding headers.
