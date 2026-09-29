# 0020. Symmetric edges

- Status: in progress (step 3 of the migration has landed)
- Date: 2026-09-29

## Context

The target is a core that routes events and never sees a byte, a head or a
key ([target architecture, §1 Summary](../target-architecture.md#1-summary),
a working draft kept outside the repository). Every session and every call
is a two-way stream of the kind's events. An edge is a dialect (`Format`)
speaking over a connection: the client edge is accepted, the provider edge
is dialed. Heads travel only as sealed origins that only their dialect
opens. The core hands the client's origin to a provider edge in one case:
the call carries the session's own inputs to a format of the same name. So
a byte-exact relay, and the client's key, ride along only then. The byte
machinery (codecs, framing, head timing) moves into one symmetric driver in
`crates/http`.

The migration runs in six steps, each green: a throwaway spike; removing
the head hooks (this step); the kernel on the new contract with the old
byte code as a private legacy driver; the HTTP driver; the flip; clean-up.
The owner approved the draft's answers to its open questions: ten public
types; the edge contract on `Format` plus a `dialect!` macro; removal of
the head hooks; `Credential` still lifted into extensions; `is_head_only`'s
two shifts accepted; `Fault::abandoned` and `Fault::of_input` public; an
`Events` that nothing follows is already ended; no `bytes` in the core;
codecs in `crates/http`; the driver and the flip as separate steps.

Sealing forbids a plugin to write a head, read the client's raw bytes, or
make a head of its own: once heads are sealed, no plugin holds one.

## Decision

Step 2 removes those powers:

1. **`ProviderContext::edit_call` goes.** A call's head and bytes are its
   format's to write. A key for one call is a value in its extensions
   (`CallHeaders`, or one its transport reads).
2. **`ClientContext::edit_response_head` goes.** A response's head is the
   client's format's to write: relayed, from the provider's; a refusal's,
   from its fault (`Status` is a hint, 0019).
3. **`ClientContext::relay` goes.** A plugin answers with
   `Response::stream` of values. A value that keeps its origin is written
   as its bytes to a client of that format, under a head written anew.
4. **`ClientContext::body()` goes.** A plugin that needs the bytes an
   input came in reads the value's origin (`Conversation::origin`).

The kernel plumbing that served only these goes too: a call's list of
edits and its run before the transport, and the client edge's list of head
edits and its run before the head goes out. The client edge keeps its
output filters on its context alone. Nothing else changes.

## Consequences

- Plugins lose a custom success status, header edits on calls beyond
  `CallHeaders` and extensions, and replaying a cached head. If plugins
  need response headers later, an HTTP extension that the client edge
  reads can carry them: the target contract hands that edge the session's
  extensions.
- A 204, 205, 304 or 101 reaches a client only as a relayed provider's
  head or its format's own. The adapter's rules are unchanged.
- `examples/jobs.rs` keys a request by its conversation's origin bytes.
- The core's toy panic points 30 and 31 moved from the head edits to a
  client output filter's end and the request encoder's `open`, which runs
  where a call's head is written.
- No new public type. `Format`, `Decoder`, `Encoder` and `Transport` did not
  change.
- Amends 0003, 0004, 0007, 0008, 0013 and 0015, which named the removed
  methods.

## Step 3: the kernel on the contract

The kernel routes events. The edge contract is in the core under a
temporary name, `Dialect` (with `Events`, `Origin`, `Half`, `Ended`,
`Reply`, `Dialed`), served with `Gateway::serve_dialect`, called with
`Gateway::provider_dialect` and opened with `Gateway::open`; the flip
renames them. It adopts the seven fixes to the draft: `dial`'s later
inputs are an `Option` (`None`: none follow), and "ended at once" holds
for `accept`'s inputs alone; `Reply` and `Dialed` are aliases; the gateway
polls a client's inputs once as the session opens and keeps what that
poll yields; `opening` is `Some` only if the inputs had ended then;
`accept` never awaits its reply, the wire carries the adapter's slot, and
the adapter drives `open` while it awaits the slot; outputs dropped unread
end their call as their end, if ready, says, else abandoned; and `Ended`
dropped first cuts the outputs with `Err(abandoned)`.

`Format` and `Transport` did not change. A private driver
(`core/src/legacy`) serves every old format and transport as a dialect and
its dialer: the byte code the kernel had, moved, with one new part, the
answer classified at its head (rule 3). It reads what of the answer is
ready when the transport returns, waiting for nothing; a decoder's failure
before any value, the body unbroken, is a refusal. So a streaming client
its provider refuses gets a complete refusal (W1 is fixed), and
`is_head_only` says "refused at its head", which is the tested meaning.

One rule the draft did not state: what a provider edge's outputs have
ready at once is one batch, whose values the call's filters see before
the client's see the first. It keeps usage metered when a client filter
fails on a value that shares its frame (`line.rs::f2_...`). The driver
says `Pending` between frames, so its batch is a frame, as before.

`Fault::abandoned` and `Fault::of_input` are public, for edges outside the
core.
