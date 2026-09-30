# Architecture

The kernel (`crates/core`) routes typed events, runs plugins, and manages
lifetimes. Protocol implementations encode and decode data, retain original
bytes and headers, establish connections, and report delivery completion.
The current boundary is defined by
[0024](decisions/0024-flows-and-symmetric-edges.md) and
[0025](decisions/0025-direction-independent-format.md). A change that contradicts
an accepted [decision](decisions/) needs a new record.

## One shape at both edges

```text
Application-facing boundary                 Provider-facing boundary

Future<Events<Input>> ── plugins ──▶ Sink<Events<Input>>
Sink<Events<Output>>  ◀─ plugins ── Future<Events<Output>>
```

`Input` travels from the application toward the provider; `Output` travels
back. These are the core's roles for the registered message types. Each boundary supplies an ordinary future that opens its incoming
events and an ordinary sink that writes outgoing events. The types reverse
at the provider. Opening, streaming and writing use Rust's existing
`Future`, `Stream` and `Sink` contracts.

Both directions advance concurrently. They are pull-driven, not autonomous
background tasks: the kernel polls the provider sink alongside opening,
then alongside response reads. Holding an unread response pauses this
kernel-driven progress. A transport can own separate I/O tasks when its
protocol requires them.

An incoming exchange is the lifetime represented by `ClientContext`. For
HTTP it includes the input body and complete response; for WebSocket it
includes the session's messages and closure. There is no core `Request`
type. One incoming exchange can create many outgoing `Call`s, such as
retries, fallbacks, or a bridge that makes a call per WebSocket message.

`Events<T>` implements `Stream<Item = Result<T, Fault>>`. It can retain a
single value, a lazy single value, a live stream, or a refusal. The
boundary's opening future resolves before the first event must be read.
Its failure differs from a returned refused stream and from a fault after
a stream opened. Reading stops at the first fault.

A single value is replayable. A lazy value is decoded at most once, when
read or transformed, and snapshots share that result. Live inputs have one
reader; a call that read none returns them for another attempt. Plugins
use `input()` for a single value, `take_inputs()` to consume live inputs,
filters for synchronous edits, and `wrap()` for asynchronous stream work.

A sink accepts the whole `Events<T>` stream as its one item. Preparation
may fail before sending; flushing waits until delivery finishes, including
protocol closing work. Consuming the last event alone does not establish
successful delivery. Clean EOF on the provider's incoming stream waits for
its outstanding write; a final write failure remains a call failure.
A provider refusal cancels an unfinished upload immediately.

The free `write(prepare)` helper implements this sink contract from a
closure that synchronously prepares a delivery future. No public `Edge`,
`Flow`, or `Writer` type is needed.

## Constructing the boundaries

`Format<T, R, W>` binds a protocol to owned I/O `T`, receiving `R` and
sending `W`. It has no associated types and no client/provider methods.
It recognizes incoming I/O with `matches`, names its protocol with `name`,
and binds the transport to typed streams. Routing policies choose which events matter.

```rust,ignore
pub type Connection<R, W> = (
    BoxFuture<'static, Result<Events<R>>>,
    Pin<Box<dyn Sink<Events<W>, Error = Fault> + Send>>,
);

pub trait Format<T, R, W>: Send + Sync + 'static {
    fn name(&self) -> &str;
    fn matches(&self, io: &T) -> bool { false }
    fn bind(&self, io: T) -> Result<Connection<R, W>>;
}
```

The gateway assigns the orientation at registration. Both registration
methods take generic parameters `<I, O, T>` in gateway input/output order:
`client` selects `Format<T, I, O>`; `provider` selects `Format<T, O, I>`.
For example, `.client::<Conversation, Reply, _>(OpenAiChat)` receives a
conversation and sends a reply, while `.provider::<Conversation, Reply, _>(
"openai", OpenAiChat, connect(transport))` receives replies and sends
conversations. The latter uses the same `Transport` and format types. A message
type must be specified when both implementations would otherwise fit;
remaining type parameters can be inferred with `_`.

The adapter constructs incoming I/O and passes it to `Gateway::handle`.
A provider is registered with `Gateway::provider(name, format, factory)`,
where `factory: Fn(Extensions) -> T` constructs fresh I/O after the provider
hooks on every attempt. The factory receives that call's final extensions.
It can capture shared connection pools or model handles while each `T`
owns the attempt's state. There are no associated `Wire` or `Dialer` types.

`bind` returns before connection establishment or input reading necessarily
finishes. Starting a provider sink initiates the I/O
that eventually opens its incoming stream. Formats sharing a name must
share their event kind, but may implement `Format<T, R, W>` for different I/O
types. The input's concrete `T` selects compatible registered client
implementations before their `matches` methods run.

There is no separate core transport interface. HTTP implements its own
`Transport` handle: adapters use `Transport::accept`, and provider registration
uses `connect(transport)` to supply a handle with each attempt’s extensions.
Outbound senders use `Transport::new(async_callback)` or convert an existing
HTTP, WebSocket, or stdio backend with `.into()`. An in-process
model uses a cloned shared model handle as its per-call I/O
([example](../examples/custom_transport.rs)).

## Gateway execution

1. The adapter hands its I/O value and extensions to `Gateway::handle`. The
   exchange counts for `idle` immediately, before its future is polled.
2. The first registered client format that recognizes the I/O value creates
   the pair. The gateway awaits its incoming opening future. An opening
   failure is written through the outgoing sink before any client plugin
   starts. Sink readiness progresses alongside opening; receiving can open
   before readiness, and successful readiness is retained until submission.
3. Each `on_client` runs in registration order. It can pass, produce a
   response, refuse, set a target, make calls, or install filters and wraps.
4. A call runs `on_provider` hooks, constructs fresh provider I/O, creates its pair, supplies its
   input stream to that sink, and drives writing and incoming opening
   together. Preparation failure, sent failure, refusal,
   and an opened response retain distinct meanings.
5. Provider output filters run before client output filters. The selected
   response becomes an event stream supplied to the application-facing sink.
6. The gateway awaits writing completion. It ends outstanding calls and
   waits for their end hooks before running client end hooks. Its private
   lifetime guard also settles cancellation and panics.

**An adapter must drive `handle` while consuming what its edge writes.**
The handle now lasts through delivery. Waiting for it before reading an
HTTP response or WebSocket messages can deadlock. HTTP's adapter owns this
coordination; direct embedders must implement it too. `handle` reports the
writing result; detached end hooks may still be running when it resolves.
`idle` waits for those hooks as well as outstanding exchanges.

Hooks and end hooks run in registration order. Input filters and wraps run
first added first; output filters and wraps run last added first. A
plugin therefore sees inputs after earlier plugins and outputs before them.
Every started hook receives its end once, including on cancellation and
panic. An incoming exchange's end waits for its calls' end hooks.

## Public API

| Type | Responsibility |
|---|---|
| `Gateway` | Registers formats, providers, plugins and a target; `handle` and `idle` drive execution. |
| `Format<T, R, W>` | Binds per-call I/O to receive `R` and send `W`; the gateway assigns its role. |
| `Connection<R, W>` | Tuple alias for the receive future and send sink. |
| `Result<T>` | Alias for `std::result::Result<T, Fault>`. |
| `Events<T>` | Standard event stream retaining lazy values, refusal state and protocol metadata. |
| `Plugin` | Client/provider start and end hooks. |
| `Filter<T>` | Synchronous edits, insertion, dropping, and holding of typed values. |
| `ClientContext` | Incoming exchange's typed input, extensions, target, filters, wraps and calls. |
| `ProviderContext` | One call's input, extensions, filters, sent/refused state and client snapshot. |
| `Call` | Lazy, clonable provider attempt, optionally with its own input or extensions. |
| `Response` | Call outputs or plugin outputs, read, held, or returned to the application. |
| `Fault` | Error code, message, cause, retry delay and typed hints. |
| `Extensions` | Clonable type map shared with adapters and plugins. |

`Events<T>` implements `Stream`; opening is a standard future and writing
is a standard sink, optionally constructed with `write(prepare)`.
`Origin`, `Headed`, `Ended`, and `Format::input` are removed, along with
the associated `Wire` and `Dialer` types. The intermediate
`Edge`/`Flow`/`Writer` design is also removed from the public API.
The core imports no HTTP types, byte buffers, credentials, or model kinds.

## Protocol metadata and relay

An event stream can retain a protocol implementation's private metadata with
`with_metadata(format, value)`. Only code knowing the concrete type and
format name can read it through `metadata`; the kernel carries or removes
it without interpretation. The storage is internally type-erased. This
API is encapsulation, not protection against trusted Rust code that knows
the type and name.

Metadata crosses to a provider only when the call carries the application's
inputs to a format of the same name. Input filters and wraps preserve that
provenance. A translation or `with_input` call gets fresh transfer metadata,
even when its value was cloned from the application's input. The provider's
metadata returns only on an eligible relay. A plugin-created response
starts fresh as well.

For HTTP, metadata retains the original head and, for a whole input, its
body. An unchanged lazy stream can relay those bytes without decoding. An
edited single value is encoded. Individual kind values can also retain
their original bytes, allowing unchanged messages to relay exactly.
Metadata is not an event and is never exposed through plugin event filters.

Credentials remain an HTTP extension. Its driver marks `Exchange::relayed`
only on eligible relays. The transport uses explicit call headers, then
its configured credentials, then the client's credential only on a relay.
A plugin that removes the credential extension stops pass-through.

## HTTP and WebSocket

An HTTP format implements `Codec`, with request/response decoders and
encoders, protocol recognition, refusal formatting and content detection.
`impl_format!` supplies both `Format<Transport, Input, Output>` and
`Format<Transport, Output, Input>` implementations. `Transport::accept`
constructs incoming I/O from the HTTP exchange; `connect(transport)` returns
a factory for outgoing I/O. One reader/writer driver handles both edges,
with a fresh codec state for each stream.

A transport that returns response headers before outgoing writing finishes
can supply `Exchange::write_completion`. The driver opens the incoming
stream immediately and keeps the sink pending on that future. `None`
means `Transport::send` completed its outgoing handoff; the driver cannot
infer a transport's later socket writes. WebSocket supplies its session
completion, including late write failures and clean remote closure.

The HTTP driver owns head timing. A one-shot response can fail as a refusal
before its first bytes; quiet streams can open immediately. WebSocket
handshakes wait for the selected response's opening, so provider refusal
can still be delivered before upgrade. A provider's non-success response
is classified before its incoming stream opens; a later stream failure is
not a refusal. Status, close codes, framing, redirects, pings and keys are
protocol concerns.

A relayed WebSocket uses one provider call and one pair of live event streams.
A WS-to-SSE bridge reads application inputs and makes a separate call per
message. `buffer_until` and `until_end` return immediately for calls with
live inputs, because holding a response could deadlock a peer waiting to
send its next message.

Provider events are coalesced into batches of at most 128 ready stream
items. A full batch without an end yields before delivery so hot streams
cannot monopolize the task. Provider filters see the entire batch before client filters see
its first event; a large decoded frame can span several batches. This lets
a provider meter observe usage in the same batch as output rejected by a
client filter, but does not promise to read later batches after failure. Wraps
remain pull-driven asynchronous transformations; they compose without
having to make the call themselves.

A cut input ends with `Fault::abandoned`, never a clean EOF. An input
filter/wrap failure ends the call with that fault. A wrap that cleanly
stops reading output can finish the client response cleanly while the
unfinished call is abandoned; a deadline that needs a reason emits a fault.

## Crates and verification

Every crate outside `core` uses only the public APIs of the crates below it.

| Crate | Responsibility |
|---|---|
| `core` | Typed routing, plugins, symmetric edges and lifecycle. |
| `http` | Codecs, transport, frames, adapters, head timing and credential policy. |
| `formats` | Conversation kind, OpenAI/Anthropic codecs, usage and relay helpers. |
| `server` | Axum router and HTTP transport. |
| `testkit` | Mock transport and real-format integration helpers. |
| `plugins` | First-party plugins using the public API. |
| `respan-gateway` | YAML configuration, model routing, `check` and `proxy`. |
| `sdks` | Routing, text, accounting, MCP and WebSocket support. |
| `examples` | Runnable integrations of the public contracts. |

| Guarantee | Evidence |
|---|---|
| Concurrent read/write progress and delivery completion | Core stream/sink unit tests, `symmetric`, and `edge_runtime`. |
| Each started hook ends once, including panic and cancellation | Core `cancellation`, `panic_matrix`, `stress`. |
| `idle` counts from handle invocation and includes jobs | Core `lifecycle`, `l6_core`. |
| Lazy decoding, replay provenance, unchanged large-body relay | Core `stress`, `lifecycle`; testkit `relay`, `translate`. |
| Refusal distinct from failed opening and failed first event | Core stream unit tests, `contract`, and `edge_runtime`; HTTP driver and WS tests. |
| Live inputs read once, unused inputs returned for retry | Core `upload`, `wrap`, `l5_wrap`. |
| No clean EOF after interrupted upload | Core `upload`, `wrap`; HTTP `driver_toy`. |
| Batch accounting and response holding | Core `contract`, `until`; ledger and routing suites. |
| Byte-exact relay and credential isolation | Testkit `relay`, `translate`; HTTP rules; WS credentials tests. |
| WS backpressure, closure and handshake behavior | `sdks/ws/tests` and HTTP driver tests. |
