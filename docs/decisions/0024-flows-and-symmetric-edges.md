# 0024. Symmetric boundaries with standard futures, streams and sinks

- Status: accepted
- Date: 2026-09-29
- Format orientation amended by [0025](0025-direction-independent-format.md).
  The two methods and associated message types below describe the preceding
  API; 0025 replaces them with `Format<T, R, W>::bind`.

## Context

0020 called the edges symmetric, but exposed different interfaces:
`accept` returned an origin and optional inputs while receiving a future
of a response tuple and completion callback; `dial` took three independent
input parameters and returned nested results. Understanding `Origin`,
`Headed` and `Ended` required memorizing tuple positions and ownership rules.
WebSocket makes the common structure clear: each boundary receives events
and writes events concurrently.

An intermediate implementation introduced public `Edge`, `Flow` and
`Writer` types. Those names repeated Rust's existing async contracts and
expanded the vocabulary without simplifying its use. The final contract
uses standard `Future`, `Stream` and `Sink` directly. The existing `Events`
name carries the event-specific semantics that a bare boxed stream lacks.

## Decision

**`Format<T>` consumes the same owned I/O type on both sides.** Its only
associated types are `ToProvider` and `ToClient`; there are no associated
`Wire` or `Dialer` types. The protocol supplies `name`, `matches(&T)`,
`is_content`, and two methods returning the same pair with reversed types:

```rust,ignore
fn client(&self, io: T) -> Result<(
    BoxFuture<'static, Result<Events<Self::ToProvider>, Fault>>,
    Pin<Box<dyn Sink<Events<Self::ToClient>, Error = Fault> + Send>>,
), Fault>;

fn provider(&self, io: T) -> Result<(
    BoxFuture<'static, Result<Events<Self::ToClient>, Fault>>,
    Pin<Box<dyn Sink<Events<Self::ToProvider>, Error = Fault> + Send>>,
), Fault>;
```

`T: Send + 'static` is I/O for one exchange or attempt. The adapter creates
incoming `T`. `Gateway::provider(name, format, factory)` stores a
`Fn(Extensions) -> T` factory, invoked after provider hooks for every
attempt. It receives the call's final extensions and can capture shared
transport configuration, while constructing new per-call I/O. This removes
the previous borrowed shared-dialer versus consumed incoming-wire asymmetry.

`Format` remains in the core API; protocol identity, recognition and the
two conversion methods are its responsibility. Both methods return before
connection establishment or input reading necessarily completes. One
format can implement multiple `Format<T>` instances. Registrations sharing
a format name must agree on their event kind, but may use different I/O
types. A received I/O type selects compatible client implementations before
`matches` is called.

HTTP has its own `Io` and `Transport`: the adapter uses `Io::accept`;
`connect(transport)` produces the provider's I/O factory. Their private
incoming/outgoing states belong to HTTP. The kernel carries no enum of
protocol connection states.

**Opening is an ordinary future.** It resolves without waiting for a first
event. Its failure remains distinct from a provider's classified refusal
and a failure after its stream opened. There is no deferred variant or
`open()` method on the event stream.

**`Events<T>` implements `Stream<Item = Result<T, Fault>>`.** Its constructors
are `single`, `lazy`, `stream` and `refused`. It retains the distinction
between a replayable single input and a live stream. Lazy decoding is
memoized across snapshots; a live input has one reader, and a call that
reads none returns it for a retry. The first event error is its last item.
Flattening these semantics into an unadorned boxed stream would lose replay,
lazy byte-exact forwarding, and refusal classification.

**The sink accepts one `Events<T>` stream as its item.** Preparation can
fail before sending. Flushing completes after delivery, including protocol
closing work. A consumed final event does not prove delivery. The free
`write(prepare)` helper constructs such a sink from synchronous preparation
that returns an asynchronous delivery future. It is a function, not another
public boundary type.

The directions progress concurrently and remain pull-driven. The kernel
polls provider writing alongside opening, then alongside response reads;
it does not spawn autonomous tasks for each direction. Holding a response
unread also pauses that kernel-driven progress. A transport can manage
separate tasks for its I/O. Clean incoming EOF waits for an outstanding
provider write; a late write failure remains a call failure. A provider
refusal cancels an unfinished upload without waiting.

**Protocol metadata stays inside `Events`.** `with_metadata(format, value)`
and `metadata::<T>(format)` let protocol implementations attach and read
private concrete types. Storage is dynamically erased internally; the
kernel only carries or removes it. This is encapsulation, not protection
against trusted Rust code that knows the type and format name.

Only calls of the application's inputs to the same named format retain
metadata, including when filters or wraps transform the values. Translation
and `with_input` calls lose it. Provider metadata returns only on eligible
relays. An unchanged lazy input can retain its original bytes; an edited
single value must be encoded. Cached/plugin-created responses start with
fresh transfer metadata. Credential policy remains HTTP's responsibility.

**Completion is structured.** `Origin`, `Headed`, public `Ended` and
`Format::input`, the associated `Wire`/`Dialer` types, and the old
`accepts`/`accept`/`dial` methods are removed. Public `Edge`, `Flow` and `Writer` from the
intermediate implementation are removed too. The gateway owns lifetime
guards and drives sinks to completion. Adapter-specific completion channels
remain private to their adapters.

`Gateway::handle` resolves after client writing finishes. Direct adapters
must drive it while consuming the response supplied through their wire;
awaiting it before reading that response can deadlock. It still counts
for `idle` from invocation, before its first poll. Detached end hooks can
still be running when it resolves; `idle` includes those hooks. A client
sink's preparation failure takes precedence over an unread provider ending
buffered for delivery.

## Consequences

- The named public boundary vocabulary is reduced from the intermediate
  `Edge`, `Flow`, `Writer`, and `Events` to `Events`. The existing stream
  alias becomes a struct implementing the same standard `Stream` trait.
- Handwritten formats and direct adapters need migration. Code that supplied
  a boxed stream where `Events` is expected wraps it with `Events::stream`.
  Plugins continue to transform events with standard stream combinators.
  Provider registrations supply an I/O factory; HTTP callers use `connect`.
  These are intentional API changes without a compatibility facade.
- HTTP responses may carry `Exchange::write_completion` when their transport
  returns before writing finishes. Incoming events open independently; the
  outgoing sink waits for completion. Without that future, `Transport::send`
  is taken to have completed outgoing handoff. WebSocket supplies its session
  completion, including clean remote closure and late write failures.
- The kernel still distinguishes one incoming exchange from its outgoing
  calls for hooks, provenance, retries and cancellation. Symmetric I/O does
  not remove those responsibilities.
- Batch accounting, hook order, refusal classification, lazy relay,
  single-reader inputs and cancellation guarantees remain required.
- The owner requested this boundary replacement. It supersedes the old
  signature and public-type-count constraints where necessary, rather than
  treating the historical count in 0015 as a design requirement.
- Amends 0003, 0004, 0006, 0008, 0013, 0015, 0016, 0020 and 0023 where they
  describe the old signatures or `handle` completion. Their feature-neutral
  and protocol behavior guarantees remain in force.
