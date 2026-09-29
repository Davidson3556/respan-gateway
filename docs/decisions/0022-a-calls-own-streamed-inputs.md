# 0022. A call's own streamed inputs

- Status: accepted
- Date: 2026-09-29

## Context

A plugin could not do async work on each input of a session. Three use
cases need it: a remote classifier that vets each turn before the provider
sees it (use case 3), a remote reserve before each turn (9), and translating
inputs mid-session (7). A filter is synchronous and cannot be woken later.
A call carried either the session's own inputs or one input of its own
(`with_input`). So the only way was to wrap the provider's `Format` and map
`dial`'s `later` asynchronously (loop 3's `proof_session_gate`). That
wrapper had three costs:

- the plugin's logic lived in a provider's registration;
- the plugin reached it through a call extension;
- the wrapper had to forward every method with a default (`accepts`,
  `is_content`, `opening`). One it missed still compiled and silently
  misbehaved.

The alternatives, and why they lose:

1. **An async filter.** Filters run synchronously under the layer engine's
   locks. That is what makes a provider's batch whole to the call's filters
   before the client's see its first (rule 5), and what lets a filter end
   exactly once on any path. An async edit would need wakers in every layer
   and a second filter trait, which is a new public type. It would also
   break the batch guarantee for every filter so that a few can await.
2. **The `Format` wrapper as the accepted pattern.** It needs no core
   change, but it keeps all three costs above. Its forwarding hazard is a
   trap that no type catches.
3. **`with_input` taking a stream as well.** Whether a call is one-shot or
   streamed is decided before any input is read: `dial` gets `later: None`
   or `Some`, and a chat format refuses a streamed body. So one input and a
   stream are different calls (loop 2), and overloading one method would
   need a trait, which is a new public type.
4. **An existing concept.** `Response::stream` already lets a plugin answer
   with outputs of its own. Its mirror on the input side fills the gap
   without a new name.

## Decision

**`Call::with_inputs(stream)` streams the plugin's own inputs in place of
the client's.** It is `Response::stream` for inputs: one method on `Call`,
generic over any `Stream<Item = Result<T, Fault>>`, with no new type or
alias. Typically the stream is `cx.take_inputs()` mapped asynchronously:

```rust
let check = cx.call("classifier");
let inputs = cx.take_inputs::<Turn>().and_then(move |turn| {
    let check = check.clone().with_input(turn.clone());
    async move {
        let verdict: Vec<Verdict> = check.await?.outputs().try_collect().await?;
        match verdict.iter().any(Verdict::flags) {
            true => Err(Fault::bad_request("flagged")),
            false => Ok(turn),
        }
    }
});
Ok(Some(cx.call("vendor").with_inputs(inputs).await?))
```

The inputs are streamed the way a session's are, on the same engine
(`leg::inputs`):

- they are pulled as the provider edge reads them;
- they run through the call's input filters;
- the stream's end, or its first `Err`, ends what the call sends;
- the request's end cuts them with `Err(abandoned)`, never with a clean end
  (B1).

Like `with_input`, they are none of the client's. The call never relays, so
neither the client's origin nor its credential reaches the provider
(`relay_origin`), and only their type is checked against the provider's
input type, not the client's kind. Translating mid-session is therefore a
plugin that maps one kind's inputs to another's.

## Consequences

- The core still has 10 public types and 4 aliases. `Call` gains one
  method.
- Internally, `Opening::bring` became `Opening::own`, and `Opening::own`
  (the field) went from a format to a `bool`, because the format is the
  base's. A call's inputs now come from the request's `Later` or its own,
  with the same reader.
- Semantics follow session inputs, with no new rule:
  - The stream is read once. A clone of the call shares it. A call that
    read none gives it back, so a retry of the same call sends every
    input.
  - `until_content` and `until_end` return at once, since waiting could
    deadlock a peer that waits for input.
  - `ProviderContext::input` is `Err`, as it is for a session.
  - The client's input filters do not run on them, as they do not on a
    `with_input`. The call's filters do.
- The stream is bound to one `Call`. A fallback to another provider needs
  the inputs again, and a stream the plugin read cannot be read twice. No
  use case needs this.
- The fault split (0021) applies as to any input: the kind classifies an
  input it cannot write.
- The `Format` wrapper is no longer needed for per-input async work, and
  no document recommends it.
- Pinned by: `crates/core/tests/own_inputs.rs`. It covers:
  - a classifier refusing a flagged input after an async check;
  - order kept whatever each check takes;
  - backpressure: an input is checked only once read;
  - a cut, of the client's or of a plugin's own stream, ending
    `abandoned`;
  - a failing stream;
  - inputs of the wrong type;
  - a retry getting back inputs that were never read.
- Amends 0020: a call carries the request's inputs, one input of its own,
  or its own stream.
