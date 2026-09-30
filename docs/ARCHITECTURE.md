# Architecture

The kernel (`core/`) routes typed events, runs plugins, and manages
lifetimes. Protocol implementations encode and decode data, retain original
bytes and headers, establish connections, and report delivery completion.
This document is the source of truth for the current architecture. Changes
to these contracts must update this document and their tests together.

## Design constraints

- **Feature-neutral kernel.** Routing policy, fallback, guardrails, redaction,
  caching, logging and accounting belong outside the core. Do not add kernel
  traits or types named after features. The kernel does not count usage or
  interpret HTTP headers, bytes, credentials or model kinds.
- **Public API parity.** First-party extensions use the same public APIs as
  external authors. If a helper needs private kernel access, improve the
  public contract rather than granting special access.
- **Compiled-in plugins only.** Configuration selects and orders implementations
  already built into the application. No dynamic plugin loading: shared
  libraries, WASM plugin runtimes, embedded scripting as a plugin mechanism,
  and stable-ABI plugin interfaces are excluded. A planned Python embedding
  adapter hosts the gateway; it is not a dynamic plugin-loading mechanism.
- **Evolvable contracts.** Prefer defaulted trait methods and context methods
  when extending public contracts. Evolving data types use private fields or
  `#[non_exhaustive]` with constructors.

## Repository layers

The construction direction is **core → SDKs/helpers → concrete plugins and
providers**; dependencies point back toward the contracts they use. Adapters
host the gateway, and applications choose what to compose.

| Path | Responsibility |
|---|---|
| `core/` | Typed routing, plugins, symmetric edges and lifecycle. |
| `sdks/messages/` | HTTP-free conversation/usage types, per-value byte provenance and the canonical JSON fault hint `Details`. |
| `sdks/http/` | Codecs, transport, frames, exchange coordination, credentials, watchdog and explicit HTTP retry classification; optional Reqwest backend. |
| `sdks/formats/` | HTTP-aware framing/error helpers and generic opaque passthrough, using the shared messages. |
| `sdks/routing/` | Transport-neutral fallback, race, response selection and backoff timing; callers supply retry classification. |
| `sdks/text/` | Reusable text filtering, holdback and rewriting helpers. |
| `sdks/ledger/` | Ledger storage, reservations, settlement primitives and shared usage-cost tallies. |
| `sdks/mcp/` | MCP protocol and stdio transport mechanisms. |
| `sdks/ws/` | WebSocket events and outbound transport, without hosting. |
| `providers/` | Concrete OpenAI Chat and Anthropic Messages codecs and their translation machinery. |
| `plugins/` | Concrete first-party policies: model routing and conservative per-attempt accounting. |
| `adapters/axum/` | Axum HTTP hosting and optional WebSocket ingress. |
| `gateway/` | YAML configuration and explicit composition, `check` and `proxy`. |
| `testkit/` | Mock transport and public-API integration helpers. |
| `examples/` | Runnable, tested integrations of the public contracts. |

Every crate outside `core` uses only public APIs. SDK production dependencies
must not point to concrete providers, plugins, adapters or the binary.
Adapters must not depend on the CLI or choose application policy. Test-only
dependencies can use concrete implementations to verify these contracts.

Providers consume the same public conversation/codec helpers as external
implementations; the formats SDK does not re-export vendor codecs. Its
helpers remain HTTP-aware. The initial providers share one package; layering
does not require a separate package per vendor. Text, tool calls and tool
results translate between the implemented chat formats; images and files
remain format-specific rather than natively translated.

`HttpTransport` belongs to the HTTP SDK behind its default-off `reqwest`
feature. Axum's default-off `ws` feature hosts WebSockets; the WS SDK retains
its outbound transport and event codec without depending on Axum. A planned
first-party PyO3 adapter belongs at `adapters/python/`, independently of Axum;
no Python adapter is implemented yet.

SDKs supply building blocks, not production `Plugin` implementations. Those
implementations live in `plugins/` or application/example code; test-only
plugins can exercise SDK contracts. Ledger remains an SDK for storage,
reservations, settlement and `Tally`. The first-party `Accounting` plugin owns
`Quote` and the conservative charging policy for refusals, failures and
cancellation. Application-specific prices, accounts and billing integrations
remain in examples.

Messages and routing have no production HTTP dependency. Formats depends on
messages and HTTP; HTTP depends on neither messages, formats nor routing.
Text and ledger still attach HTTP status hints to their existing domain
errors (422 and 401/402 respectively); this does not make all SDKs HTTP-free.

## One shape at both edges

```text
Application-facing boundary                 Provider-facing boundary

Future<Events<Input>> ── plugins ──▶ Sink<Events<Input>>
Sink<Events<Output>>  ◀─ plugins ── Future<Events<Output>>
```

`Input` travels from the application toward the provider; `Output` travels
back. These are the core's roles for the registered message types. Each
boundary supplies an ordinary future that opens its incoming events and an
ordinary sink that writes outgoing events. The types reverse
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
closure that synchronously prepares a delivery future. Client-sink preparation
failure takes precedence over an unread buffered provider ending.

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
owns the attempt's state.

`bind` returns before connection establishment or input reading necessarily
finishes. Starting a provider sink initiates the I/O
that eventually opens its incoming stream. Formats sharing a name must
share their event kind, but may implement `Format<T, R, W>` for different I/O
types. The input's concrete `T` selects compatible registered client
implementations before their `matches` methods run.

A **kind** is the input/output type pair, defined outside the kernel.
Registered message types implement `Clone + Send + Sync + 'static`; input
and output must be distinct types. Formats of one kind translate through
these typed values. A normal call to an incompatible kind fails with
`config`; `with_input` can call another kind by supplying that provider's
input type. Typed plugin access with the wrong type fails with `internal`,
rather than silently skipping traffic.

There is no separate core transport interface. HTTP implements its own
`Transport` handle: adapters use `Transport::accept`, and provider registration
uses `connect(transport)` to supply a handle with each attempt’s extensions.
Outbound senders use `Transport::new(async_callback)` or convert an existing
HTTP, WebSocket, or stdio backend with `.into()`. An in-process
model uses a cloned shared model handle as its per-call I/O
([example](../examples/custom_transport.rs)). HTTP outbound handle clones
share resources; accepted-exchange clones can bind only once collectively.
Constructing and binding these handles performs no I/O; the returned futures
and sinks drive it.

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
4. A call runs `on_provider` hooks, constructs fresh provider I/O, creates its
   pair, supplies its input stream to that sink, and drives writing and incoming opening
   together. Preparation failure, sent failure, refusal,
   and an opened response retain distinct meanings.
5. Provider output filters run before client output filters. The selected
   response becomes an event stream supplied to the application-facing sink.
6. The gateway awaits writing completion. It ends outstanding calls and
   waits for their end hooks before running client end hooks. Its private
   lifetime guard also settles cancellation and panics.

**An adapter must drive `handle` while consuming what its edge writes.**
The handle lasts through delivery. Waiting for it before reading an
HTTP response or WebSocket messages can deadlock. HTTP's adapter owns this
coordination; direct embedders must implement it too. `handle` reports the
writing result; detached end hooks may still be running when it resolves.
`idle` waits for those hooks as well as outstanding exchanges.

Hooks and end hooks run in registration order. Input filters and wraps run
first added first; output filters and wraps run last added first. A
plugin therefore sees inputs after earlier plugins and outputs before them.
Every started hook receives its end once, including on cancellation and
panic. An incoming exchange's end waits for its calls' end hooks.

The kernel has one global plugin registry, not endpoint-specific routes or
plugin chains. A response or refusal skips later client hooks. Mandatory
checks must therefore precede short-circuiting plugins. Register input-producing
plugins before checks or redactors that must inspect their additions. Plugins
explicitly choose whether to pass or refuse other kinds; endpoint-specific
policy can use adapter-supplied extensions.

If every client hook passes, the selected target is called. There is one
default target, replaced by subsequent `Gateway::target` calls; `set_target`
changes the exchange's target without skipping later hooks. No target yields
`not_found`. YAML `after:` ordering is resolved by the application before
registration, not by a kernel scheduler.

A call snapshots the client context when created and starts when first polled.
Later whole-input edits or newly installed wraps do not change that snapshot;
live-input readers and their existing filter layers remain shared. Its
extensions combine the snapshot's values, call-specific values and
provider-hook changes; provider-hook changes affect that call alone. Every
provider call runs provider hooks, including calls made by plugins.

New provider hooks do not wait for earlier calls' end hooks. Record a
reservation before an await and settle it in the matching end hook:
cancellation can occur at any await.

A plugin can obtain the gateway through `ClientContext::gateway()` to launch
an independent exchange. It inherits no extensions and runs the plugin chain
again. There is no nesting-depth limit, so plugins must mark and recognize
their own re-entry. Keep the gateway in the job, not in the registered plugin,
to avoid a reference cycle.

## Filters and asynchronous wraps

A whole input remains exactly one value: filters edit it synchronously, and
later hooks read the edited value through `input()`. Dropping or expanding
that input is an `internal` fault. Input wraps apply only to live inputs;
wrapping a whole input is also an `internal` fault. Await whole-input checks
in the hook, then edit with a filter.

Filters receive all values, including keep-alives and usage-only events.
Each filter receives its end exactly once, including failure, cancellation
and panic; a filter added after completion sees that end immediately. A
filter panic becomes an `internal` fault. Metering is a provider-filter
policy: register a meter after filters whose rejection or dropping must not
hide values from it, since output filters run in reverse order.

Provider events are coalesced into batches of at most 128 ready stream
items. A full batch without an end yields before delivery so hot streams
cannot monopolize the task. Provider filters see the entire batch before
client filters see its first event; a large decoded frame can span several
batches. A provider meter can observe usage in the same batch as output
rejected by a client filter, but need not see unread later batches after
failure. Wraps remain pull-driven asynchronous transformations; they compose
without having to make the call themselves.

A cut input ends with `Fault::abandoned`, never a clean EOF. An input
filter/wrap failure ends the call with that fault. A wrap that cleanly
stops reading output can finish the client response cleanly while the
unfinished call is abandoned; a deadline that needs a reason emits a fault.

## Response holding and routing policy

`Response::buffer_until(predicate)` inspects provider-filtered events and
retains buffered events, protocol metadata and call ownership for replay.
An error before a match is returned; an error after a match remains buffered.
Clean completion returns the response even without a match. Repeated calls
recheck buffered events with the new predicate.

`until_end` holds the entire response and returns it with its terminal result.
Errors before any output are returned directly; later errors are retained
for replay and returned alongside the response. Calling policy must bound
buffering and waiting where needed. Both operations leave live-input calls
unread to avoid duplex deadlock.

The routing SDK offers `Until::Head`, `Until::content(predicate)` and
`Until::End`. There is no universal content predicate: the plugin supplies
the event type and criterion. Provider-filtered-out events cannot commit a
content-based fallback, and synthetic responses obey the same predicate.
For calls without live inputs, `End` selects only clean completion.

`first_success` requires a retry classifier `Fn(&Fault, bool) -> bool`, called
for every failed attempt. The bool is `response_opened`: the call returned
`Ok(Response)` before applying `Until`, not whether it was sent, refused or
delivered. Returning false skips further tries of that target but still
allows fallback. `Backoff` controls timing and the attempt budget, not fault
classification. A response-opened failure's retry delay overrides exponential
backoff; a delay above the maximum skips the retry. Pre-opening retry delays
are not used.

HTTP callers can explicitly select `http::retryable`: before opening it
accepts only `upstream`; after opening it excludes `internal`, `config` and
`abandoned`, then accepts statuses 408, 425, 429, 500, 502, 503, 504 and 529.
Non-HTTP callers supply their own classification. Neither classifier nor a
clonable call grants replayability or guarantees idempotency.

The HTTP SDK's `watchdog` wraps transport opening and polled next-frame waits.
It retains exchange metadata and outgoing write completion. Its idle timeout
is not an autonomous deadline while downstream leaves the body unpolled.

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

## Protocol metadata and relay

An event stream can retain a protocol implementation's private metadata with
`with_metadata(format, value)`. Only code knowing the concrete type and
format name can read it through `metadata`; the kernel carries or removes
it without interpretation. The storage is internally type-erased. This
API is encapsulation, not protection against trusted Rust code that knows
the type and name.

Metadata crosses to a provider only when the call carries the application's
inputs to a format of the same name and transport type. Input filters and wraps preserve that
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

## Fault ownership

`Fault` carries a code, not an HTTP status. Typed hints are interpreted by the
relevant SDK or boundary; the kernel does not inspect them. HTTP defines
`Status` and maps codes to default status codes. Provider error answers and
in-stream errors can carry format-specific original answers through
`with_answer`; only eligible relay responses retain those original answers.

Input encoding failures are classified by the format and kind, not recoded
by the kernel or HTTP driver. The shared `Relayed` helper classifies unchanged
decoded input as `bad_request` and edited or newly constructed input as
`config`. This is independent of transfer provenance: an unchanged clone
passed through `with_input` can still be a client-input error even though
the call does not relay.

`ProviderContext::is_sent` and `is_refused` describe the call, not its fault.
Sent means the call may have reached the provider; refused means its response
was classified as refused before opening, not that an opened stream later
failed.

## HTTP and WebSocket

An HTTP format implements `Codec`, with request/response decoders and
encoders, protocol recognition and refusal formatting. Plugins decide which
decoded events count as content.
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

## Verification

Examples are workspace members using only public APIs. Every runnable example
has tests under `examples/tests/` that run with `cargo test --workspace`.
Protocol tests use local provider fakes; credentialed real-provider tests
are opt-in and ignored by the normal suite.

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Also check all features and isolated no-default-feature packages when changing
optional dependencies: workspace feature unification can hide missing feature
boundaries. Build public documentation with
`RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --all-features`.

| Guarantee | Evidence |
|---|---|
| Concurrent read/write progress and delivery completion | Core stream/sink unit tests, `symmetric`, and `edge_runtime`. |
| Each started hook ends once, including panic and cancellation | Core `cancellation`, `panic_matrix`, `stress`. |
| `idle` counts from handle invocation and includes jobs | Core `lifecycle`, `l6_core`. |
| Lazy decoding, replay provenance, unchanged large-body relay | Core `stress`, `lifecycle`; testkit `relay`, `translate`. |
| Refusal distinct from failed opening and failed first event | Core stream unit tests, `contract`, and `edge_runtime`; HTTP driver and WS tests. |
| Live inputs read once, unused inputs returned for retry | Core `upload`, `wrap`, `l5_wrap`. |
| No clean EOF after interrupted upload | Core `upload`, `wrap`; HTTP `driver_toy`. |
| Batch accounting and response holding | Core `contract`, `until`; plugin accounting, ledger and routing suites. |
| Byte-exact relay and credential isolation | Testkit `relay`, `translate`; HTTP rules; WS credentials tests. |
| WS backpressure, closure and handshake behavior | WS SDK, Axum adapter and HTTP driver tests. |
