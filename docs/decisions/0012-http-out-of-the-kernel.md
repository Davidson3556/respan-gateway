# 0012. HTTP out of the kernel

- Status: accepted
- Date: 2026-09-28

## Context

The kernel carried heads beside inputs and outputs because it implemented
HTTP's proxy rules itself: which headers are hop-by-hop and which a relay
passes (`fill`, `passes`, `strip_named`, `outgoing`), what `Upgrade`
means, which responses have no body, when a 101 or a redirect is allowed,
what `Retry-After` means, 499 for a client that left, and text/plain for
what no format writes. None of them is a kernel rule; they are HTTP's
(0001). This is the first of two steps (`target/review/DESIGN-heads.md`,
"Stage A"); the second, typed wires with no `http` types in the core,
comes under a decision of its own. This amends 0003 to 0011.

## Decision

The `http` types stay in the core as an envelope it never reads: it hands
a head, like bytes, only to a format of the name it came in, and only for
a relay.

1. **A provider's error answer is a `Fault`.** Its decoder ends `Err` with
   the provider's status, `with_retry_after`, and `with_answer(format,
   answer)` (the HTTP formats' answer is `http::Response<Bytes>`), which
   replaces `with_raw`; `is_answer` says the provider answered with it in
   place of a response. An error within a response it began, such as an
   error event in a stream, keeps its words with `with_words`. The client's
   format writes a refusal, now handed to `Format::response_encoder`: the
   answer as it came if it read it, else the fault in its own words with
   its status and `Retry-After`; a fault whose status is no error's, it
   cannot write. `Response::head`, `Response::with_status` and
   `Outcome.head` are gone; a biller charges nothing for `is_answer`.
2. **The rules live at the boundaries.** `crates/http`
   (`respan-gateway-http`): the adapter (`serve`, `handle`) strips the
   connection's headers, writes what no format can as text/plain and
   strips what a hop must not carry on the way out; a transport strips
   what a proxy must not pass on (`proxy_call`) and the client origin's
   headers. A relay's call starts from the client's head, as its unread
   body does; its format edits it, as `HttpTransport` and `MockAt` clean
   it.
3. **The adapter says whether a request streams**:
   `Gateway::handle(req, streaming)` replaces `Upgrade` detection, and is
   `Err` for what no format can write. `Format::request_encoder` is told
   `streams`; a protocol that cannot carry streamed inputs fails
   `Encoder::open`, now fallible. A 101 and a body HTTP forbids are the
   formats' and the adapter's: a decoder refuses a 101 its call did not
   ask for, an encoder of a body that has none says so (`Encoder::done`),
   and its head goes at once.
4. **Credentials are an extension.** The adapter lifts `Authorization`,
   `x-api-key` and key query parameters into `http::Credential`; plugins
   read the rest of the head as `http::Asked`. `ClientContext::head` and
   `head_mut` are gone for `extensions` and `extensions_mut`. A transport
   sends `CallHeaders`, else its own headers, else the client's
   `Credential`.
   The last only on a relay, which the kernel marks with `Relayed` in the call's extensions (a 14th public core type), so no key crosses a translation.
5. `edit_response_head` and `edit_call` stay, typed as `http`.
6. stdio MCP returns faults for 404, 409 and 503; only the session header
   and its successes are its own.

## Consequences

- 13 public core types; the core has no HTTP rule left, only its types.
  `Encoder` has `done` and a fallible `open`; `Format::request_encoder` and
  `response_encoder` take one more argument each.
- A plugin of any kind that reads headers depends on `crates/http`.
- An outcome no longer says the status sent: a success is `Ok`; a
  failure carries its status.
- A translated success's head is the client format's alone: its status
  and `Retry-After` do not cross. A redirect or 304 is a 502 in another
  format, and relays as it came in its own.
- A custom HTTP transport that forwards everything forwards the client's
  connection-level and conditional headers; it calls `proxy_call`.
- `sdks/routing`'s `Until::Head` can no longer tell an error answer from
  a success before reading it.
