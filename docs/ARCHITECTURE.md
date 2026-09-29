# Architecture

The kernel (`crates/core`) is an event router. It runs every plugin in
registration order, routes a session's events between a client edge and
provider edges, and ends each request and call once. It never sees a byte,
a head or a key: those belong to the edges, which are formats. The
[README](../README.md#glossary) defines the terms; the
[decisions](decisions/) give the reasons, [0020](decisions/0020-symmetric-edges.md)
for this shape. A change that contradicts an accepted one needs a new
record.

```
                control plane: on_client · cx.call · on_provider · end hooks
                   (decide, race, fall back, reply, refuse, splice)
                                        │
 client ══ Wire ══╗                     ▼                    ╔══ Dialer ══ provider
                  ║ client edge   ┌───────────┐  provider edge║
                  ║ Format::accept│ inputs  ─▶ input filters ─│─▶ Format::dial
                  ║             ◀─│ outputs ◀─ output filters◀│─
                  ╚═ heads, bytes └───────────┘ heads, bytes ═╝
                     and keys        the core:      and keys
                     sealed          events only    sealed
```

Every session and every call is a two-way stream of one kind's events:
inputs toward the provider, outputs back. Filters wrap those streams (the
data plane). Plugins decide where a session goes (the control plane). An
edge is a [`Format`](../crates/core/src/format.rs) speaking over a
connection: the client edge is accepted, the provider edge is dialed.

## The request path

1. The adapter (`crates/http`: `serve`, `handle`) strips a request's
   connection headers, lifts its credentials into a `Credential` extension,
   adds an `Asked` view of its head, and hands an `Accepted` wire to
   `Gateway::handle(wire, extensions)`, which takes the caller's wire type.
2. `handle` gives the wire to the first served format whose `accepts` says
   yes. None: the wire is dropped and the end is `Err(not_found)` (or
   `internal` if a format panicked), for the adapter to write plainly. The
   request is in flight from the call on, before its future is polled, so
   `Gateway::idle` waits for a body still being read and for a job's request
   handed from an end hook; the future borrows nothing.
3. `Format::accept` resolves once the request is in: a one-shot request
   after its body is read (no inputs follow), a session at once (inputs
   follow). It gives the client's origin and its inputs, if any. `Err` (a body that broke) is refused before any plugin.
   The result is a `ClientContext`.
4. Each plugin's `on_client`, in registration order: pass, reply (the rest
   are skipped) or refuse. A plugin may add filters, `set_target`, `call`
   and `take_inputs`; no plugin touches a head.
5. If all pass, `cx.call(target)`: each plugin's `on_provider` (an `Err`
   fails the call), then `Format::dial` on the provider's format with its
   dialer. What it resolves is the call's `Response`: open, or refused at
   its head.
6. The call's provider-edge filters edit each output as decoded. A plugin
   may hold the response (`until_content`, `until_end`), read it
   (`outputs`), or send it.
7. The client-edge filters edit what is sent; the gateway resolves the
   `reply` future it gave `accept` with the reply (the provider's origin on
   a relay, and the outputs or a refusal) and `Ended`. The client edge
   writes the reply's head and body and calls `Ended` with how the reply
   ended.
8. `Ended` ends any open calls `abandoned`, then each call's
   `on_provider_end` runs, then `on_client_end`. `handle`'s end future
   resolves once the reply's end is out, before end hooks finish.

Output filters run provider edge first and the last added first; input
filters the first added first: a plugin sees inputs after earlier plugins,
outputs before them.

## The core's ten types

| Type | Why it exists |
|---|---|
| `Gateway` | Built by value (`provider`, `serve`, `plugin`, `target`); `handle` and `idle` are its runtime. |
| `Plugin` | Four hooks: `on_client`, `on_client_end`, `on_provider`, `on_provider_end`. |
| `Filter<T>` | The one way to edit inputs or outputs as they stream. |
| `ClientContext` | What `on_client` and its end hook see: the request, its extensions, its filters, `call`, `take_inputs`, and `gateway` for a job that outlives the request. |
| `ProviderContext` | What a call's hooks see: the provider, format, first input, extensions, filters, `is_sent`, `is_refused`; the request by `&`, so it cannot `call`. |
| `Call` | A `'static`, `Clone`, `Sync` future, sent when awaited; `with_input` (one input of its own), `with_inputs` (a stream of its own) and `with_extension` shape it. |
| `Response` | A call's answer, or `Response::stream` of a plugin's own; held (`until_content`, `until_end`), read (`outputs`) or sent. |
| `Fault` | The one error: a code, message, retry and typed hints (`with`, `get`), such as an HTTP `Status` or a format's `Details`; the kernel reads neither ([0019](decisions/0019-status-out-of-the-kernel.md)). |
| `Extensions` | A clonable type map that carries what the adapter and plugins share, unopened by the core. |
| `Format` | One protocol's view of a kind and the edge contract, below. |

Also aliases (`Events`, `Origin`, `Half`, `Ended`), the
`codes` module and re-exports of the crates the contracts use. The core
depends on no `http` crate; `Gateway::handle` needs a tokio runtime, which
runs end hooks detached.

## The Format contract

A format is one protocol's view of a kind, plus its two edges. Its kind is
its pair `Input`/`Output`, through which formats of one name relay and of
one kind translate; `Wire` is the client connection its adapter hands over,
`Dialer` how calls reach one provider. Formats of one name share a kind,
wire and dialer, checked as they are registered.

```text
trait Format { type Input; type Output; type Wire; type Dialer;
  fn name(&self) -> &str;
  fn accepts(&self, wire: &Wire) -> bool { false }
  fn is_content(&self, output: &Output) -> bool { true }
  fn accept(&self, wire: Wire, reply: BoxFuture<(Half<Output>, Ended)>)
      -> BoxFuture<Result<(Option<Origin>, Option<Events<Input>>), Fault>>; // refuses
  fn opening(&self, origin: &Origin) -> Option<Result<Input, Fault>> { None }
  fn dial(&self, dialer: &Dialer, extensions: Extensions, relay: Option<&Origin>,
          opening: Option<&Input>, later: Option<Events<Input>>)
      -> Result<BoxFuture<Result<Half<Output>, Fault>>, Fault>; }
// Events<T>   = BoxStream<Result<T, Fault>>
// Origin      = Arc<dyn Any>: what came with a head, sealed
// Half<T>     = (Option<Origin>, Result<Events<T>, Fault>)
// Ended       = Box<dyn FnOnce(Result<(), Fault>)>
```

The rules, in full in the rustdoc of `Format`:

1. **`accept` resolves when the request is in**, its origin kept, and
   never awaits `reply`. Its inputs are `Some(events)` if any follow the
   head (a session) and `None` if none do (a one-shot request); `dial` is
   told the same with `later: None`. `opening` gives the input that came
   with the head, only for a request whose inputs are `None`; the gateway
   calls it only when a value is needed, never on an unchanged relay.
2. **A provider edge classifies its answer at its head.** `dial` returns
   `Err` now (not sent), a future's `Err` (sent, not answered),
   `Ok((origin, Err(fault)))` (answered and refused, which
   `is_refused` reports) or `Ok((origin, Ok(outputs)))` (open).
3. **The client edge owns its head timing**; see below.
4. **The client edge calls `Ended` exactly when its reply's end is out**,
   or drops it: dropped, the session ends `abandoned`. `Ended(Err(f))` also
   ends the reply's call with `f`. Outputs dropped unread end their call as
   its end, if ready, says, else `abandoned`.
5. **What a provider edge's outputs have ready at once is one batch**: the
   call's filters see all of it before the client's see its first, so a
   meter sees usage that shares a frame with an output a client filter
   fails on. Outputs to be read apart return `Pending` between.

The client edge has defaults (`accepts` is false, `accept` refuses,
`opening` is `None`), so a format that keeps them is dial-only: an
in-process model is a provider whose `Dialer` is the model
(`examples/custom_transport.rs`).

An input a format cannot write is `bad_request` if it says what the
client's bytes said, else `config`, a plugin's. The kind tells them apart,
as each value keeps what it was decoded to (`Relayed::whose` in `formats`),
and the format applies it where it writes the input, out of the gateway's
sight: an HTTP body's frames reach the transport, not the gateway
([0021](decisions/0021-the-kind-owns-the-fault-split.md)). The model
belongs to the kind (`Conversation.model`); its place on the wire is the
format's.

**Head timing, per edge.** A refusal is written as one, a complete reply.
HTTP's client edge writes a one-shot request's head with its first bytes, so
a failure before any is still a refusal; `Encoder::open` opens a quiet
stream at once; an upgrade's head goes when `reply` resolves. HTTP's
provider edge reads a non-2xx answer whole before `dial` resolves (a 101 to
a call that asked to switch is open), so a refusal is known as one however
slowly its body comes.

**A fault says whose it is:** `bad_request` the client's, `config` the
gateway's, `upstream` and `truncated` a provider's, `internal` a bug,
`abandoned` that no one waits.

### HTTP formats

An HTTP format implements `Codec` (`crates/http`), one protocol's bytes:
`accepts` a head, `request_decoder`, `request_encoder`, `response_decoder`,
`response_encoder`, `refusal` (the words of a refusal; the driver writes a
provider's answer as it came instead), `is_content`, each body getting its
own `Decoder` and `Encoder`, which keep that stream's state.
`impl_format!(Type)` makes it a
`Format` on the `Accepted` wire and `Dialer`. One symmetric driver runs
every codec: a reader turns frames into events through a `Decoder`, a
writer events into frames through an `Encoder`, on both edges. A
`Dialer` is any `Transport`, whose `send(Exchange<RequestHead>)` returns the
provider's `Exchange<ResponseHead>`; the driver decides head timing, framing,
batches and the verdict at the head. A format on another wire implements
`Format` itself.

## Relay and credentials

One function decides relay (`relay_origin` in `call.rs`): the client's origin
reaches a provider only when the call carries the request's own inputs (it
has none of its own, `with_input` or `with_inputs`, which no plugin can
forge) and goes to a format of the client's name. Then `dial` gets `relay`, and a relay whose opening no plugin
changed gets `opening: None`: the driver writes the origin's head and raw
body unread, and a changed value is written into its own bytes by the
format. The provider's origin returns to the client edge only on a relay, so
the client's format writes the provider's head; a translated call is written
from a fresh head both ways. A cached reply (`Response::stream` of stored
values) keeps each value's bytes for a client of that format under a fresh
head.

The client's key rides along the same way. The adapter lifts it into a
`Credential` extension, which the core carries unopened. The driver sets
`Exchange::relayed` if and only if the call carries the session's own
inputs to a same-name format, even as filters edited them (a call with
`with_input` or `with_inputs` does not), and
`http::credentials` restores the client's key only then, after the call's
`CallHeaders` and the transport's own. A plugin that removes `Credential`
stops pass-through. A provider's error answer keeps its format's name
(`Fault::with_answer`); a client edge of that name writes it as it came,
another in its own words.

## Streaming

**SSE** is one event per message: a comment or keep-alive is a value that
says nothing yet (`is_content` false) and reaches every filter; an
unchanged value is written as its bytes. **WebSocket** is an `Upgrade`
exchange on the HTTP wire, one text frame per message (`sdks/ws`): the
client edge resolves `accept` at once with frames as inputs and writes its
head when `reply` resolves. A WebSocket relayed to a same-name format is
one call, with `on_provider` once, its inputs read once through the call.
A bridge (WebSocket in, SSE out) keeps `call = cx.call(p)`, returns
`Response::stream`, and per message from `take_inputs()` sends
`call.clone().with_input(chat)` and reads `outputs()`.

**Async work on each input** of a session, such as a remote classifier, a
reserve per turn or translating mid-session, is a call with inputs of the
plugin's own: `cx.call(p).with_inputs(stream)`, typically
`cx.take_inputs()` mapped asynchronously
([0022](decisions/0022-a-calls-own-streamed-inputs.md)). They stream as a
session's do: pulled as the provider reads, through the call's input
filters, cut `abandoned` at the request's end, never relayed. Filters stay
synchronous, so a batch stays whole to them.

`until_content` reads until the first content (`Format::is_content`, before
filters), a failure or the end, and holds what it read unsent; `until_end`
reads to the end. Both return at once for a call that streams inputs, the
request's or its own, as waiting could deadlock a peer that waits for input. A call ends
when the batch holding its end is sent, read or dropped
([0011](decisions/0011-read-until-content-or-end.md)). A fallback:

```rust
let mut last = Err(Fault::upstream("no provider to try"));
for name in &self.0 {
    last = match cx.call(name).await {
        Ok(resp) => resp.until_content().await,
        Err(fault) => Err(fault),
    };
    if last.is_ok() { break }
}
last.map(Some)
```

Routing by model reads the input, so streaming requests route by the
adapter's `Asked` view of their head, and opaque traffic by its body.

## Crates

| Crate | What it is |
|---|---|
| `core` | The kernel: ten types, events only; no bytes, heads, keys, HTTP or kinds. |
| `http` | HTTP's wire: `Codec`, `Decoder`, `Encoder`, `Transport`, `Exchange`, `Frames`, `Accepted`, `Dialer`, `impl_format!`, the driver; the adapter (`serve`, `handle`), `Credential`, `Asked`, `Status`, and a transport's rules (`proxy_call`, `credentials`, `CallHeaders`). |
| `formats` | The chat kind (`conversation`), `Usage`, `OpenAiChat`, `AnthropicMessages`, `opaque`, `sse`, `whole`, helpers for codecs. |
| `server` | `router` (axum; bodies up to 32 MiB, no upgrades) and `HttpTransport`. |
| `testkit` | `MockTransport`, `serve`, run helpers, and tests through the real formats. |
| `plugins` | First-party plugins: `ModelRouter`. |
| `respan-gateway` | The binary: YAML config, routing by model, `check`. |
| `sdks/routing` | Retry, fallback, race and a stall watchdog. |
| `sdks/text` | Read and rewrite chat text across stream deltas. |
| `sdks/ledger` | Reserve before a call, settle once from what your filter priced. |
| `sdks/mcp` | The MCP kind, streamable HTTP codec and stdio transport. |
| `sdks/ws` | WebSocket: `WsTransport`, an axum adapter (`serve_ws`), the `JsonEvents` codec. |
| `examples` | One runnable, tested example per use case ([list](../examples/README.md)). |

Every crate but `core` uses only the public API of the crates below it.

## Where each guarantee lives

| Guarantee | Where | Pinned by |
|---|---|---|
| Each hook that started gets its end once, on drop or panic too | `lifecycle.rs`; `Ended` owns the end hooks | `crates/core/tests`: `cancellation` (a drop at every await), `panic_matrix` (a panic at every place user code runs), `stress` |
| `Gateway::idle` covers a request from the call to `handle`, a body still being read and a job's request | `InFlight`, entered when `handle` is called | core `lifecycle`; e2e |
| A parked call ends `abandoned` with its request | `call.rs` (`Sending`, `OpenCalls`) | core `cancellation`; testkit `streams` |
| `until_content`, `until_end`; a call ends when its end is read | `Response`, `Batch` | core `until`; `sdks/routing/tests` |
| A cut or broken upload never looks complete, a call's own streamed inputs too | events: `leg.rs`; bytes: the driver's writer | core `upload`, `own_inputs`; `crates/http/tests/driver_toy.rs` |
| Edge contract: refusal at the head, outputs dropped unread | `call.rs`, `session.rs` | core `contract`; `sdks/ws/tests/kernel.rs` |
| The fault split | the kind (`Relayed::whose`), where the format writes an input | testkit `codec`, `line`, `custom`, `translate` |
| Lazy decode; a big body relays unread | `opening.rs` | core `stress`; e2e |
| Byte-exact relay; a key only on a same-name relay | `relay_origin`; the driver; `http::credentials` | testkit `relay`, `translate`; `crates/http/tests/rules.rs`; e2e; `sdks/ws/tests` (`passthrough`, `credentials`) |
| Head timing, framing, batches, keep-alives | the driver, the formats | `crates/http/tests` (`driver_toy`, `driver_formats`); testkit `streams`, `line` |
| Panic guards | `guarded`, `leg.rs`; the driver for codecs | core `panic_matrix`; `driver_toy` |
| Backpressure, WebSocket closes | pull everywhere; `sdks/ws` | `sdks/ws/tests` (`backpressure`, `close`, `concurrency`) |
| Metering through filters | `Filters`, on both legs | testkit `line`; `examples/tests/billing.rs`; `sdks/ledger/tests` |
| HTTP's rules at the edges | `crates/http`, `crates/server` | `crates/http/tests/rules.rs`; `crates/respan-gateway/tests/e2e.rs` |

The kernel alone, on a toy format and its public API, is tested in
`crates/core/tests` (`edge/` holds the toy).
