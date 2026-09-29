# 0020. Symmetric edges

- Status: accepted; amended by 0021, 0022 and 0023
- Date: 2026-09-29

## Context

The kernel carried bytes, heads and keys: it decoded and encoded bodies,
timed the response head, and let plugins edit heads. So the kernel knew
HTTP's shape, a streaming request was a flag, and a second wire
(WebSocket, stdio, an embedder's own) had to look like HTTP. The core
should route events and never see a byte, a head or a key, and a plugin
should never hold a head.

## Decision

**The core is events-only.** Every request and every call is a two-way
stream of one kind's events: inputs toward the provider, outputs back. An
edge is a format speaking over a connection: the client edge is accepted,
the provider edge is dialed. The core has ten public types (`Call`,
`ClientContext`, `Extensions`, `Fault`, `Filter`, `Format`, `Gateway`,
`Plugin`, `ProviderContext`, `Response`) and four aliases (`Ended`,
`Events`, `Half`, `Origin`; a fifth, `Dialed`, was inlined in loop 2). A
request is the whole exchange with one client; a session is a request
whose inputs stream after its head. It depends on no `http` crate and its
contracts carry no head or body.

**`Format` is one protocol's view of a kind and the edge contract.** Associated types `Input`
and `Output` (the kind), `Wire` (a client connection as its adapter hands
it over) and `Dialer` (how calls reach one provider); methods `name`,
`accepts(&Wire)`, `is_content`, `accept` (the client edge), `opening` (the
lazy first input) and `dial` (the provider edge). Formats of one name share
a kind, wire and dialer. Its rules, in the rustdoc of `Format`:

1. `accept` resolves when the request is in and never awaits its `reply`;
   the wire carries the adapter's slot, and the adapter drives
   `Gateway::handle` to its end while it awaits that slot. Its inputs are
   `Some(events)` if any follow the head and `None` if none do, as `dial`'s
   `later` says it. `opening` is `Some` only for a request whose inputs are
   `None`.
2. A provider edge classifies its answer at its head: a refusal resolves
   `Ok((origin, Err(fault)))`, which `is_refused` reports.
3. The client edge owns its head timing: a refusal is written as one; an
   open answer's head when the edge decides.
4. The client edge calls `Ended` exactly when its answer's end is out, or
   drops it (the request ends `abandoned`); `Ended(Err(f))` also ends the
   answer's call with `f`; outputs dropped unread end their call as its end,
   if ready, says, else `abandoned`.
5. What a provider edge's outputs have ready at once is one batch: the
   call's filters see it all before the client's see its first.

The client edge has defaults (`accepts` is false, `accept` refuses, `opening`
is `None`), so a format that keeps them is dial-only, such as an in-process
model whose `Dialer` is the model.

`dial` returns `Err` now (not sent), a future's `Err` (sent, not
answered), `Ok((origin, Err(fault)))` (refused) or `Ok((origin, Ok(outputs)))`
(open). Each input carried whether it was the client's as decoded, which
said whose fault it is if the format cannot write it (`Fault::of_input`);
0021 moved that to the kind. `Fault::abandoned` is public for edges outside
the core.

**Heads travel as sealed origins.** An `Origin` is what came with a head
(for a client, the head and a whole body), opened only by a format of the
name it came in. The core hands the client's origin to a provider edge in
one case: the call carries the request's own inputs (it has no
`with_input`) to a format of the same name (`relay_origin` in
`call.rs`). So a byte-exact relay, and the client's key, ride along only
then. A provider's origin returns to the client edge only on a relay.

**No plugin touches a head.** `ProviderContext::edit_call`,
`ClientContext::edit_response_head`, `ClientContext::relay` and
`ClientContext::body()` are gone. A key for one call is a value in its
extensions (`CallHeaders`). A plugin answers with `Response::stream` of
values, which a client of the format they came in gets as their bytes under
a fresh head. A plugin that needs the bytes an input came in reads the
value's origin (`Conversation::origin`).

**`Gateway::handle<W>(wire, extensions)` takes the caller's wire**, one
generic, and returns the future of how the request ended. A format of
another wire type never accepts it.

**HTTP's bytes live in `crates/http`.** An HTTP format implements `Codec`
(`accepts`, `request_decoder`, `request_encoder`, `response_decoder`,
`response_encoder`, `refusal`, `is_content`) and gets `Format` from
`impl_format!(Type)`;
`Wire` is `Accepted` and `Dialer` is `Dialer`, made of any `Transport`.
`Decoder`, `Encoder`, `Transport`, `Exchange`, `Frames` and the adapter
(`serve`, `handle`) are there. One symmetric pair, a reader (frames to
events through a `Decoder`) and a writer (events to frames through an
`Encoder`), runs both edges, and owns head timing, the verdict at the head
(a non-2xx answer, but a 101 to a call that asked to switch, is read whole
before `dial` resolves), framing, batches and the byte guarantees. The
driver sets `Exchange::relayed` if and only if the call carries the
request's own inputs to a format of the same name (it wrote from an
origin), as the plugins' filters left them: a filter-edited input still
relays, so a redactor keeps a subscription passthrough working; a call with
`Call::with_input` does not, so a plugin bills its own prompt to itself.
A format on another wire implements `Format` itself.

## Consequences

- A plugin loses a custom success status, header edits on calls beyond
  `CallHeaders` and extensions, and replaying a cached head. If plugins need
  response headers, an HTTP extension that the client edge reads can carry
  them.
- A 204, 205, 304 or 101 reaches a client only as a relayed provider's head
  or its format's own.
- A streaming client that a provider refuses at its head gets a complete
  refusal, not a head and then a failing body (W1).
- `is_refused` means "the provider refused at its head", a refusal
  classified by the provider edge (rule 2).
- `examples/jobs.rs` keys a request by its conversation's origin bytes.
- Amends 0003, 0004, 0005, 0006, 0007, 0008, 0011, 0012, 0013, 0014, 0015 and
  0016, and supersedes 0017.

## History

Six steps, each green:

1. A throwaway spike: the kernel on the contract with a toy edge.
2. The head hooks and `body()` removed from plugins.
3. The kernel on the edge contract, under the name `Dialect`; the old
   `Format` and `Transport` served by a private legacy driver in the core.
   It adopted seven fixes to the draft: `dial`'s later inputs are an
   `Option`; `Dialed` is an alias; the gateway polls a client's
   inputs once at open; `opening` is `Some` only if the inputs had ended;
   `accept` never awaits its reply; outputs dropped unread end their call as
   their end says; `Ended` dropped first cuts the outputs with
   `Err(abandoned)`.
4. The HTTP driver added to `crates/http`.
5. The flip: the legacy driver and the old `Format`, `Transport`,
   `Exchange`, `Decoder` and `Encoder` left the core; `Dialect` became
   `Format`; every HTTP format became a `Codec` plus `impl_format!`; transports
   import `http::Transport`.
6. Clean-up: the documentation and the decision records.

The design, with the reasoning and the costs, is
[target-architecture.md](../target-architecture.md), now historical.

Loop 1 (a review of the contract's simplicity and naming), one pass:

- `accept` returns `Option<Events<Input>>`: `None` means no inputs follow, as
  `dial`'s `later` does. The rule that an `Events` whose first poll is
  `Ready(None)` is ended, and the gateway's read-ahead that found it, are
  gone.
- The client edge's `reply` is `(Half<Output>, Ended)`: no edge read the
  extensions it carried, and `Reply` left the core (now 5 aliases).
- The client edge has defaults, so a provider-only format needs no stubs.
- Kept, not changed: the per-input bool in `dial`. The core cannot own the
  fault split: an HTTP body's frames are written by the format's encoder
  and read by the transport, so a failure to write a later input never
  passes through the core, and the flag of the last input pulled cannot be
  matched to it there. (Reversed in loop 2 by 0021: the format applies the
  split, but the kind, not the core, knows it.)
- Renames: `is_head_only` is `is_refused`; `dialect!` is `impl_format!`
  and "dialect" is no synonym of format; `relayed::Origin<T>` is
  `Came<T>`; `Ask` and `Answer` are `RequestHead` and `ResponseHead`.
- A `Codec` writes a refusal's words with `refusal(fault, body, head)`;
  the driver writes a provider's answer as it came (`answered`) and the
  status, so `response_encoder` lost its `refusal` parameter.
- `Fault::with_details` and `details` are a hint, `formats::Details`, as
  `Status` is (0019): the kernel never read them, and the core no longer
  depends on `serde_json`.

Loop 2 (simplicity and use cases), one pass:

- `Format::Output` and both `filter<T>` lost an unused `PartialEq` bound.
- `accept` resolves `Result<(Option<Origin>, Option<Events<Input>>), Fault>`:
  nothing read the origin of a request that broke.
- The per-input bool left `dial` (0021).
- `Dialed` is inlined, as it differed from `Dialer` by one letter: `dial`
  returns `Result<BoxFuture<Result<Half<Output>, Fault>>, Fault>`, which
  spells out "not sent" and "sent, not answered" (now 4 aliases).
- `ClientContext::gateway()` hands a plugin its gateway, for a job that
  outlives the request (use case 8), in place of a slot set after the
  gateway is built. The job keeps it, not the plugin: the gateway holds its
  plugins.

Loop 3 (simplicity, use cases and correctness), one pass:

- `Gateway::handle` is a plain `fn` that returns a `'static` future: a
  request counts for `idle` from the call, so a job handed from an end hook
  is never missed.
- A call may carry a stream of its own inputs (`Call::with_inputs`, 0022),
  so async work on each input of a session is a plugin, not a wrapped
  `Format`. (Loop 4: replaced by `ClientContext::wrap`, 0023, which keeps
  the relay.)
- Words: "request" is the whole exchange everywhere, "session" only a
  request whose inputs stream. The internal `session.rs` is `reply.rs`, as
  it turns a response into the client edge's reply.
- The adapter's part of rule 1 is a compiled example in `Format`'s rustdoc.
- Kept, not changed: the name `Half`. `Headed<T>` reads better, but the
  rename touches every hand-written `Format` in five crates, being changed
  in parallel at the time, for no change in meaning. `Answer` would collide
  with `Fault::answer`. Revisit when those branches have merged.
