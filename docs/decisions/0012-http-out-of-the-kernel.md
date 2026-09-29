# 0012. HTTP out of the kernel

- Status: accepted, amended by [0013](0013-no-http-in-core.md), [0014](0014-api-trims.md), [0019](0019-status-out-of-the-kernel.md) (the status is a hint)
- Date: 2026-09-28

## Context

The kernel implemented HTTP's proxy rules itself: hop-by-hop headers,
what `Upgrade` means, which responses have no body, when a 101 or a
redirect is allowed, `Retry-After`, 499 for a client that left, and
text/plain for what no format writes. None is a kernel rule (0001).

## Decision

1. **A provider's error answer is a `Fault`.** Its decoder ends `Err` with
   the provider's status (a `Status` hint, 0019), `with_retry_after` and
   `with_answer(format, answer)`; an error within a response, such as an
   error event, is the same. Whether the provider refused before beginning
   a response is `ProviderContext::is_head_only`, not a fact of the fault
   (0015). The client's format writes a refusal: the answer as it came if
   it read it, else the fault in its own words with its status and retry.
2. **The rules live at the boundaries**, in `crates/http`: the adapter
   (`serve`, `handle`) strips the connection's headers, writes what no
   format can as text/plain, and strips what a hop must not carry on the
   way out (`outgoing`); a transport strips what a proxy must not pass on
   (`proxy_call`) and the client origin's headers.
3. **The adapter says whether a request streams**, with
   `Exchange::streams` on the request `Gateway::handle` takes (0015); `Err`
   for what no format can write. `Format::request_encoder` is told
   `streams`; a protocol that cannot carry streamed inputs fails
   `Encoder::open`. A decoder refuses a 101 its call did not ask for; an
   encoder of a body that has none says so (`Encoder::open` returns
   `Ok(true)`, 0014), and its head goes at once.
4. **Credentials are an extension.** The adapter lifts `Authorization`,
   `x-api-key` and key query parameters into `http::Credential`; plugins
   read the rest of the head as `http::Asked`. A transport sends
   `CallHeaders`, else its own headers, else the client's `Credential`,
   the last only on a relay, which the kernel marks with
   `Exchange::relayed` (0015).
5. stdio MCP returns faults for 404, 409 and 503.

## Consequences

- A plugin that reads headers depends on `crates/http`.
- A request's end says no status: a success is `Ok`; a failure carries
  its status.
- A translated success's head is the client format's alone: status and
  `Retry-After` do not cross. A redirect or 304 is a 502 in another
  format, and relays as it came in its own.
- A custom HTTP transport calls `proxy_call`, or forwards the client's
  connection-level and conditional headers.

## History

- 0013: the `http` types left the core too; head hooks became generic.
- 0014, 0015: the flags this record named (an answer's `is_answer`, a
  `Relayed` marker, `handle`'s `streaming`, `Encoder::done`) became
  `ProviderContext::is_head_only`, `Exchange::relayed`, `Exchange::streams`
  and `Encoder::open`'s `Ok(true)`.
