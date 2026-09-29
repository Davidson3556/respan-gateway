# 0023. A plugin wraps a stream

- Status: accepted
- Date: 2026-09-29

## Context

0022 let a plugin do async work on each input of a session by giving a call
a stream of its own: `cx.call(p).with_inputs(cx.take_inputs().and_then(check))`.
Two reviews (loop 4) found it flawed:

1. **It loses the relay.** A call with its own inputs never relays, so a
   guarded same-format realtime session lost the client's path and query,
   its key and the provider's 101 head, and a client that asked for a
   subprotocol failed its handshake. The reason given, that no plugin can
   forge the client's inputs, does not hold: a filter on a relay edits,
   drops and inserts the same inputs, and the relay stays.
2. **The guard must make the call.** So it had to be registered last, could
   not be combined with another plugin that makes the call, and a fallback
   to another provider lost the inputs, which were bound to one `Call`.

The output side had the matching gap. No plugin could wrap a response's
outputs and keep the provider's head: `examples/realtime.rs`'s `MaxLength`
cuts a session at a deadline by making the call and replying with
`Response::stream`, which drops the head (subprotocol clients fail) and
has the same composition problem.

A filter cannot do either job: it is synchronous, so nothing wakes it for a
remote check's answer or a timer.

## Decision

**`ClientContext::wrap(f)` replaces the request's inputs or the outputs
sent to its client with the stream `f` makes of them.** `f` takes
`Events<T>` and returns any stream of `Result<T, Fault>`; `T` says which
stream, as it does for `filter`:

```rust
cx.wrap(|turns: Events<Turn>| turns.and_then(check))?;
cx.wrap(|events: Events<Event>| events.take_until(sleep(limit)))?;
```

- **The request stays what it was.** A call of its inputs relays exactly as
  before (the client's origin and key go, the provider's head comes back);
  a wrap only changes what flows, as a filter does. Relay therefore has one
  rule again: a call relays unless it carries an input of its own
  (`with_input`), to a format of the client's name.
- **No plugin makes the call.** A wrap belongs to the request, so any
  number compose, with each other and with filters, and every call of the
  request gets the wrapped inputs, a fallback's too.
- **Plugin order.** Wraps and filters apply in registration order: a wrap
  sees inputs after the filters and wraps added before it, and outputs
  before them, as a filter does (0018).
- **The stream rules of a session's inputs.** A wrap is pulled as its
  reader reads (backpressure); read once, given back to a retry or a
  fallback if the call read none. The request's end cuts what it wraps
  and what it gives with `Err(abandoned)`, never a clean end (B1): its
  inputs are cut before anything else, and a stream whose own end comes as
  the cut lands, as a wrap's that ends when what it wraps is cut does,
  ends with the cut.
- **A failure ends the call with it.** Inputs that fail, not cut (a wrap's
  or a filter's refusal, a client edge's error), end the call's outputs
  with that fault, so the call, its hooks and the reply end with it and the
  client edge can say it (a WebSocket's close code), not `abandoned`.
- **A whole request's input is refused** (`internal`): it came with its
  head and has no stream to wrap. A plugin awaits its check in `on_client`
  and edits it with a filter, and later plugins read the result with
  `input()`.

`Call::with_inputs` and its machinery (the `Own` enum, its type check, the
two cases in `send`) are removed. `take_inputs` stays, for a plugin that
reads the inputs itself, such as a bridge from WebSocket to SSE.

Also, found by the same review: a reader that finds a request's inputs
already read gets `Err`, not a clean empty upload, unless none were ever
read (then an empty upload is what the client sent).

### Alternatives

1. **`ClientContext::set_inputs(stream)`** (the simplicity review's
   prototype: 11 fewer core lines than 0022). With `take_inputs` it covers
   the input side with the relay kept, and composes. It leaves the output
   side unsolved: outputs do not exist in `on_client`, so there is nothing
   to set, and `MaxLength` would still need a second method. `wrap` is the
   same idea as one method for both directions.
2. **A pair, `map_inputs` and `map_outputs`.** The same semantics as
   `wrap`, but two methods where the type already says the direction,
   unlike `filter`, which is one method for both.
3. **Keep `with_inputs`, let it relay.** A one-line fix for the relay, but
   the guard still has to make the call: no composition, no fallback.
4. **`Response::map` or a `Response` of a call's outputs with the head
   kept.** Solves `MaxLength` only for the plugin that makes the call.
5. **An async `Filter`.** 0022's reasons stand: filters run synchronously
   under the layer engine's locks, which is what keeps a provider's batch
   whole to the call's filters before the client's see its first (rule 5)
   and ends each filter exactly once on any path.
6. **Wraps on `ProviderContext` too, per call.** No use case needs a
   per-call async stage; the request's wrap reaches every call.
7. **Wraps only, no filters.** A wrap cannot replace a filter:
   - it pulls one value at a time, so rule 5's batch guarantee (a meter
     sees usage that shares a frame with an output a client filter fails
     on) would be lost;
   - a filter edits a whole request's one input in place, so later plugins
     read it with `input()` and an unchanged relay stays byte-exact;
   - filters run per call (`ProviderContext::filter`), a wrap per request;
   - a filter's `end` runs once on every path, with what it kept back.

   So a filter is for what can be decided on each value as it passes, a
   wrap for what must wait.

## Consequences

- The core still has 10 public types and 4 aliases. `ClientContext` gains
  `wrap`; `Call` loses `with_inputs`. The public method count is unchanged.
- Core code: 2152 lines at 0022, 2216 now. `with_inputs` cost 29; the new
  lines are the output side (a wrap stage per plugin in the reply), the
  inputs' chain (a wrap puts new inputs in front of the ones it wraps,
  which end with the request), the failure signal from a call's inputs to
  its outputs, and the retry fix.
- A call made before a wrap keeps the inputs as they were, as a call is a
  copy of the request as it is when made.
- A wrap of outputs that ends them cleanly ends the reply cleanly; the
  call, its outputs dropped unread, ends `abandoned`. A plugin that wants
  the cut told, as a deadline does, ends its stream with a fault of its
  own, which ends the reply and its call (loop 5, below).
- An edge's classification now reaches the end: a client edge that reports
  a client leaving as an input failure says `abandoned` (`sdks/ws` does,
  for a drop and a binary frame).
- `ClientContext::gateway()` now says that a request handed back inherits
  no extensions and nests with no depth limit, so a plugin marks it.
- Pinned by `crates/core/tests/wrap.rs`: a gate refusing an input after an
  async check, and the call and reply ending with that refusal; a filter's
  refusal ending the call with it; order; backpressure; cuts ending
  `abandoned`, a wrap that swallows the cut included; a failing and a
  panicking stream; the relay and the provider's head kept; a gate and a
  deadline composed; plugin order of wraps and filters; a fallback getting
  the wrapped inputs; a retry after every input was read; a drop at every
  await. `upload.rs` pins the retry on plain session inputs.
- Supersedes 0022. Amends 0020 (a call carries the request's inputs, as
  plugins left them, or one input of its own).

## History

Loop 5 (simplicity and correctness), one pass:

- **The reply holds its call strongly.** `Ended` held the reply's call
  weakly from the first kernel (0020 step 3), so outputs dropped unread
  ended it at once, `abandoned`, and `Ended(Err(f))` then found nothing to
  end. An output wrap drops them whenever it stops reading, so a deadline
  that ended its stream with `max_length` ended the reply with it and the
  call `abandoned`, while the same cut made on the inputs said
  `max_length`. `Ended` now holds the call: `Ended(Err(f))` ends it with
  `f` unless it ended, and dropped outputs whose end is not ready leave it
  to end with the request (Format rule 4). Nothing else changes: a call
  ends once (its end hooks are guarded), the request's end still abandons
  every open call before its own end hooks start, and `Ended` dropped
  drops the call with the request, so no call outlives it and no cycle
  forms (`Ended` is held by the edge, never by the call).
