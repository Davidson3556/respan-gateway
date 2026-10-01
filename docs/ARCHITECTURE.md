# Architecture

The kernel (`core/`) routes typed events, runs plugins, and manages
lifetimes. Protocol implementations encode and decode data, retain original
bytes and headers, establish connections, and report delivery completion.
This document is the source of truth for the current architecture. Changes
to these contracts must update this document and their tests together.

## Design constraints

- **RC-like core stability.** The core is half-frozen: its public contracts are
  stable by default. Changes require a demonstrated correctness issue or an
  unavoidable contract gap, not an outer-layer cleanup. Preserve compatibility
  where possible and update the contract documentation and tests together.
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

The kernel owns feature-neutral contracts. One plugin-author SDK reexports
those contracts and adds the common LLM message vocabulary. Protocol authors
use separate explicit namespaces; adapters host the gateway, and applications
choose what to compose. Dependencies point back toward the contracts they use.

| Path | Responsibility |
|---|---|
| `core/` | Typed routing, plugins, symmetric edges and lifecycle. |
| `sdk/` | `respan-gateway-sdk`: public core contracts, HTTP-free conversation/usage types, basic typed text accessors and JSON fault hint `Details`. |
| `http/` | `respan-gateway-http`: owned HTTP edges, native request/response bodies, stream/sink helpers, credentials, watchdog and retry classification; `formats` framing/error helpers and opaque passthrough. Optional Reqwest backend. |
| `providers/` | Concrete OpenAI Chat and Anthropic Messages codecs and their translation machinery. |
| `plugins/` | `ModelRouter` and public `routing` attempt orchestration: fallback, race, selection and backoff with caller-supplied retry classification. |
| `mcp/` | MCP protocol and stdio transport mechanisms. |
| `ws/` | WebSocket events and outbound transport, without hosting. |
| `adapters/axum/` | Axum HTTP hosting and optional WebSocket ingress. |
| `gateway/` | YAML configuration and explicit composition, `check` and `proxy`. |
| `tests/support/` | Private source-only integration helpers; provider scenarios use pinned mockllm over real HTTP. |
| `examples/` | Runnable, tested integrations; private application-specific billing and text rewriting support. |

Every crate outside `core` uses only public APIs. The SDK depends on core,
`bytes` and `serde_json`, never protocol infrastructure, concrete providers,
plugins, adapters or the binary. Core has no SDK or infrastructure dependency.
Other authors can use core directly; first-party consumers use the SDK facade.
Adapters must not depend on the CLI or choose application policy. Test-only
dependencies can use concrete implementations to verify these contracts.

The SDK's basic accessors (`Message::text`, `Message::is_plain`,
`Reply::texts` and `Reply::texts_mut`) operate on typed content; accessors that
skip other content document that limitation. They do not claim to inspect
opaque tool arguments. Specialized `Readable`, holdback, JSON-fragment rewriting
and placeholders live together in private `examples/support/text.rs`, shared by
guardrail, PII and realtime examples. Their provider-aware parsers and 422 error
hints are not SDK APIs. All four dedicated text regression suites remain
explicit example-package test targets.

Decoded conversation values still embed transport-neutral `Relayed<T>`/`Came<T>`
provenance in the SDK to preserve byte-exact relay and input-fault attribution.
Moving these representations into HTTP would create an SDK/HTTP cycle
or change the message API. They remain support for codec authors, not HTTP
plumbing or the main plugin-author entry point.

Providers consume the same public conversation/codec helpers as external
implementations. `respan_gateway_http::formats` does not reexport vendor codecs; its
helpers remain HTTP-aware. HTTP and format helpers share one package, with
HTTP APIs at the crate root and framing helpers in the public `formats` module,
rather than compatibility facade packages. The initial
providers also share one package; layering does not require one package per
vendor. Text, tool calls and tool results translate between the implemented chat
formats; images and files remain format-specific rather than natively translated.

`respan_gateway_http::HttpClient` is behind the default-off `reqwest` feature.
Axum's default-off `ws` feature hosts WebSockets; the WebSocket integration
retains its outbound transport and event codec without depending on Axum. A
planned first-party PyO3 adapter belongs at `adapters/python/`, independently of
Axum; no Python adapter is implemented yet. MCP and WebSockets are opt-in
integration dependencies, not plugin SDK crates.

The SDK supplies contracts and message types, not production `Plugin`
implementations. Those live in `plugins/` or application/example code. Billing
is application policy, not a supported SDK or first-party plugin API.
`examples/billing/accounting.rs` owns `Accounting`, `Quote`, priced `Tally` and
conservative charging for refusals, failures and cancellation.
`examples/billing/ledger.rs` supplies only private example-local reservations and
settlement, shared by wallet, cost-cap and realtime examples through source
modules, not exported by the examples library. Applications own prices,
accounts, durable persistence and settlement recovery; expiry releases a
reservation but does not recover a failed charge. Generic `Usage` stays in the
SDK. There is no ledger trait in core or replacement billing SDK.

The SDK and routing plugin have no production HTTP dependency. Routing
orchestration lives with its concrete plugin owner and accepts a caller-supplied
retry classifier. Protocol infrastructure owns HTTP retry classification;
private text and billing examples choose their own HTTP status hints.

## One shape at both edges

```text
Application-facing boundary                 Provider-facing boundary

Future<EventStream<Input>> ── plugins ──▶ EventSink<Input>
EventSink<Output> ◀─ plugins ── Future<EventStream<Output>>
```

`Input` travels from the application toward the provider; `Output` travels
back. These are the core's roles for the registered message types. Each
boundary supplies a future that opens its incoming events and a standard
sink for its outgoing events. The types reverse at the provider.
`EventStream<T>: Stream<Item = Result<T, Fault>>` and
`EventSink<T>: Sink<Result<T, Fault>, Error = Fault>` are symmetric per-event
interfaces. A source fault is an item delivered to the sink, distinct from a
sink error reporting failed delivery.

Both directions advance concurrently. They are pull-driven, not autonomous
background tasks: the kernel polls the provider stream-to-sink pump alongside
opening, then alongside response reads. Holding an unread response pauses this
kernel-driven progress. A transport can own separate I/O tasks when its
protocol requires them.

An incoming exchange is the lifetime represented by `ClientContext`. For
HTTP it includes the input body and complete response; for WebSocket it
includes the session's messages and closure. There is no core `Request`
type. One incoming exchange can create many outgoing `Call`s, such as
retries, fallbacks, or a bridge that makes a call per WebSocket message.

`EventStream<T>` implements `Stream<Item = Result<T, Fault>>`. It can retain a
single value, a lazy single value, a live stream, or a refusal. The
boundary's opening future resolves before the first event must be read.
Its failure differs from a returned refused stream and from a fault after
a stream opened. Reading stops at the first fault.

A single value is replayable. A lazy value is decoded at most once, when
read or transformed, and snapshots share that result; inspection failure
alone does not prevent an unchanged original request from relaying its raw
body. Each whole-input edit replaces only the current snapshot, leaving
previously created calls' snapshots intact. Live inputs share one reader; a
call that read none returns them for another attempt. Input metadata has one
owner and follows original-input provenance. Plugins use `input()` for a
single value, `take_inputs()` to consume live inputs, filters for synchronous
edits, and `wrap()` for asynchronous stream work.

`EventSink::new` wraps a standard sink. `EventSink::preparing` accepts a
synchronous factory `FnOnce(&mut EventStream<T>) -> Result<S>` returning a
real per-item sink. The kernel calls `prepare` before marking a provider call
sent. Preparation may inspect metadata, encode a finite value, stop an
already-encoded or explicitly bodyless source, or wrap a live source that
remains owned by the caller. It must not initiate I/O or retain a live source
to read inside the sink. No event queue or whole-stream delivery callback is
introduced by the kernel. `EventStream::stop()` prevents further reads without
decoding or releasing the source: its shape, metadata and ownership survive
until the stream is dropped, after delivery completes or is cancelled. This
lets bodyless transfers retain unread call ownership through delivery without
a separate hold-only wrapper.

The pump checks readiness before reading potentially nonempty sources,
submits every item (including a source fault), flushes when the source is
pending, and closes at source EOF or after its first fault. Known-empty and
explicitly stopped sources close without waiting for readiness; a live stream
must still establish readiness before it is polled, even if it later yields
EOF. It yields after at most 128 ready items. Flushing
must advance quiet transfers even before their first item; it is not final
transport completion. Closing reports actual delivery completion, including
protocol closing work. Consuming the last event alone does not establish
successful delivery. A submitted source fault is terminal: later items are
rejected, but flushing and closing still reach the concrete sink. Completed
close results are cached, so repeated close/flush cannot repeat finalization. Clean EOF on the provider's incoming
stream waits for its outstanding delivery; a final write failure remains a
call failure. A provider refusal cancels an unfinished upload immediately,
even when routing retains the refused response.

Direct users can use standard `SinkExt` operations on `EventSink::new`;
deferred sinks require `prepare` first. A protocol can wrap its live source
to end it when actual independent transport completion occurs; the wrapper
remains caller-owned, and closing must still report that completion's result.
Client delivery preparation failure takes precedence over an unread buffered
provider ending.

## Constructing the boundaries

`Format<T, R, W>` binds a protocol to owned I/O `T`, receiving `R` and
sending `W`. It has no associated types and no client/provider methods.
It recognizes incoming I/O with `matches`, names its protocol with `name`,
and binds the transport to typed streams. Routing policies choose which events matter.

```rust,ignore
pub type Connection<R, W> = (
    BoxFuture<'static, Result<EventStream<R>>>,
    EventSink<W>,
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
"openai", OpenAiChat, connect(send))` receives replies and sends
conversations. HTTP uses distinct owned `Incoming` and `Outgoing` I/O types
for these orientations, with the same format value. A message type must be
specified when multiple implementations would otherwise fit; remaining type
parameters can be inferred with `_`.

The adapter constructs incoming I/O and passes it to `Gateway::handle`.
A provider is registered with `Gateway::provider(name, format, factory)`,
where `factory: Fn(Extensions) -> T` constructs fresh I/O after the provider
hooks on every attempt. The factory receives that call's final extensions.
It can capture shared connection pools or model handles while each `T`
owns the attempt's state.

`bind` establishes state without initiating I/O. Provider delivery is prepared
synchronously before the call is marked sent; polling its sink through the
kernel pump initiates outgoing I/O and progresses alongside incoming opening.
Formats sharing a name must share their event kind, but may implement
`Format<T, R, W>` for different I/O types. The input's concrete `T` selects
compatible registered client implementations before their `matches` methods run.

A **kind** is the input/output type pair, defined outside the kernel.
Registered message types implement `Clone + Send + Sync + 'static`; input
and output must be distinct types. Formats of one kind translate through
these typed values. A normal call to an incompatible kind fails with
`config`; `with_input` can call another kind by supplying that provider's
input type. Typed plugin access with the wrong type fails with `internal`,
rather than silently skipping traffic.

There is no separate core transport interface. HTTP supplies move-only
`Incoming` and `Outgoing` handles. An accepted exchange has one owner; it cannot
be cloned or bound twice. `connect(send)` shares an async sending function and
constructs an outgoing handle with each attempt's extensions. The function
accepts a native `http::Request<Body>` and returns a native
`http::Response<Body>` plus optional write completion. Backends can share their
connection pools without sharing an attempt's binding state. An in-process
model uses a cloned shared model handle as its per-call I/O
([example](../examples/custom_transport.rs)). Constructing and binding I/O
performs no network work; synchronous preparation sets up the sink, and polling
the stream-to-sink pump drives delivery.

## Gateway execution

1. The adapter hands its I/O value and extensions to `Gateway::handle`. The
   exchange counts for `idle` immediately, before its future is polled.
2. The first registered client format that recognizes the I/O value creates
   the pair. The gateway awaits its incoming opening future. An opening
   failure is submitted to its outgoing sink before any client plugin
   starts; binding establishes any shared state needed by either half.
3. Each `on_client` runs in registration order. It can pass, produce a
   response, refuse, set a target, make calls, or install filters and wraps.
4. A call runs `on_provider` hooks, constructs fresh provider I/O, creates its
   pair, synchronously prepares its sink from the input stream, and drives
   its event pump with incoming opening and subsequent response reads.
   Preparation failure, sent failure, refusal, and an opened response retain
   distinct meanings. Sent becomes true only after preparation succeeds.
5. Provider output filters run before client output filters. The selected
   response becomes an event stream pumped to the application-facing sink.
6. The gateway awaits delivery completion. It ends outstanding calls and
   waits for their end hooks before running client end hooks. Its private
   lifetime guard also settles cancellation and panics. Failed or cancelled
   delivery cuts the exchange before releasing its source; successful delivery
   releases stopped sources before settling their unread ready endings.

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

`Response::buffer_until(&mut self, predicate) -> Result<()>` inspects
provider-filtered events and retains buffered events, protocol metadata and
call ownership for replay. An error before a match is returned **without
consuming the response**; an error after a match remains buffered. Clean
completion succeeds even without a match. Repeated calls recheck buffered
events with the new predicate.

`Response::until_end(&mut self) -> Result<()>` holds the entire response,
including terminal failures with zero or partial content. The caller still
owns that response after success, failure or cancellation and may replay it.
`buffered_end()` distinguishes an actual terminal result held in that response
from an inspection fault that cannot be replayed (such as a wrong predicate
type). A bounded hold yields only after storing each read batch, so cancelling
inspection loses no outputs. Calling policy must bound buffering and waiting
where needed. Both operations leave live-input calls unread to avoid duplex
deadlock.

The routing module offers `Until::Head`, `Until::content(predicate)` and
`Until::End`. There is no universal content predicate: the plugin supplies
the event type and criterion. Provider-filtered-out events cannot commit a
content-based fallback, and synthetic responses obey the same predicate.
For calls without live inputs, `End` selects only clean completion.

`first_success` requires a retry classifier `Fn(&Fault, bool) -> bool`, called
for every dispatched failed attempt. A zero-attempt `Backoff` dispatches none.
The bool is `response_opened`: the call returned `Ok(Response)` before applying
`Until`, not whether it was sent, refused or delivered. Returning false skips
further tries of that target but still allows fallback. A retry-discarded
response is dropped **before** backoff so its end hooks and reservations settle;
a fallback candidate remains owned until a better one replaces it. The
selected opened attempt is marked `Winner` before its buffered end is replayed,
including a final failed/refused answer. A final inspection fault without a
matching buffered terminal failure is returned as that fault, not silently
replayed as healthy output. `Backoff` controls timing and the attempt budget,
not fault classification; it doubles beyond a `u32` multiplier and caps work
for extreme budgets. A response-opened failure's retry delay overrides
exponential backoff; a delay above the maximum skips the retry. Pre-opening
retry delays are not used.

HTTP callers can explicitly select `http::retryable`: before opening it
accepts only `upstream`; after opening it excludes `internal`, `config` and
`abandoned`, then accepts statuses 408, 425, 429, 500, 502, 503, 504 and 529.
Non-HTTP callers supply their own classification. Neither classifier nor a
clonable call grants replayability or guarantees idempotency.

The HTTP protocol module's `watchdog` wraps transport opening and polled next-frame waits.
It retains exchange metadata and outgoing write completion. Its idle timeout
is not an autonomous deadline while downstream leaves the body unpolled.

## Public API

| Type | Responsibility |
|---|---|
| `Gateway` | Registers formats, providers, plugins and a target; `handle` and `idle` drive execution. |
| `Format<T, R, W>` | Binds per-call I/O to receive `R` and send `W`; the gateway assigns its role. |
| `Connection<R, W>` | Tuple alias for the receive future and outgoing event sink. |
| `Result<T>` | Alias for `std::result::Result<T, Fault>`. |
| `EventStream<T>` | Standard event stream retaining lazy values, refusal state and protocol metadata. |
| `EventSink<T>` | Standard per-item sink with optional synchronous preparation and actual delivery completion on close. |
| `Plugin` | Client/provider start and end hooks. |
| `Filter<T>` | Synchronous edits, insertion, dropping, and holding of typed values. |
| `ClientContext` | Incoming exchange's typed input, extensions, target, filters, wraps and calls. |
| `ProviderContext` | One call's input, extensions, filters, sent/refused state and client snapshot. |
| `Call` | Lazy, clonable provider attempt, optionally with its own input or extensions. |
| `Response` | Call outputs or plugin outputs, read, held, or returned to the application. |
| `Fault` | Error code, message, cause, retry delay and typed hints. |
| `Extensions` | Clonable type map shared with adapters and plugins. |

`EventStream<T>` implements `Stream`; `EventSink<T>` implements `Sink`.
Opening remains a standard future. The kernel owns the source while pumping
items, and both directions retain independent backpressure and cancellation.

## Protocol metadata and relay

An event stream can retain a protocol implementation's private metadata with
`with_metadata(format, value)`. Only code knowing the concrete type and
format name can read it through `metadata`; the kernel carries or removes
it without interpretation. The storage is internally type-erased. This
API is encapsulation, not protection against trusted Rust code that knows
the type and name.

Metadata crosses to a provider only when the call carries the application's
original inputs to a format of the same name. Input filters and wraps preserve
that provenance; the client and provider may use different transport types.
The receiving format still reads metadata only when its concrete metadata
type and name match. A translation or `with_input` call gets fresh transfer
metadata, even when its value was cloned from the application's input. The
provider's metadata returns only on an eligible relay. A plugin-created
response starts fresh as well.

For HTTP, metadata retains the original head and, for a whole input, its
body. An unchanged lazy stream can relay those bytes without decoding. An
edited single value is encoded. Individual kind values can also retain
their original bytes, allowing unchanged messages to relay exactly.
Metadata is not an event and is never exposed through plugin event filters.

Credentials remain an HTTP extension. The binding records eligible relay
provenance privately on the native outgoing request, independently of its body
bytes. The backend uses explicit call headers, then its configured credentials,
then the client's credential only on a relay. A plugin that removes the
credential extension stops pass-through.

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

An HTTP format implements core directly in both orientations:
`Format<Incoming, Input, Output>` and `Format<Outgoing, Output, Input>`.
There is no HTTP-specific format trait, implementation macro or prepared-body
framework. Binding helpers connect native HTTP messages to core event streams
and sinks. Protocol callbacks choose paths, headers, parsing, success predicates
and refusal formatting; plugins decide which decoded events count as content.
Each body has fresh framing state.

HTTP structure and byte storage use `http` and `bytes` directly: request/response
builders, `http::request::Parts`, `http::response::Parts`, `Bytes` and `BytesMut`.
Standard `http-body-util` adapters handle external body data streams.
Converting an external body to a data stream intentionally discards trailers;
HTTP gateway framing carries data only. There are no parallel head
aliases or request/response conversion helpers. The gateway's `Body` retains
only the distinctions needed for inspectable finite bytes, initial-prefix
snapshots and upload ownership; it implements the standard HTTP body contract.
Boxing an arbitrary body is not a substitute for its synchronous finite-byte
access during preparation.

`Incoming` owns a request and its response handoff. Ordinary HTTP requests are
collected before opening and decoded lazily. Collection copies each streamed
frame into `BytesMut` and releases it before polling again, allowing producers
to recycle their buffers; finite bodies retain their clone-only fast path.
Explicitly live/message inputs remain streaming; network chunk boundaries never
select semantic input shape.
The read callback returns an `EventStream`, so an advanced protocol can supply
its own parser instead of using the common message reader.

`Outgoing` owns one attempt and its sending function. Its synchronous
preparation callback returns an actual `Request<Body>` and `EventSink`, not a
plan for later interpretation. Untouched lazy input can relay eligible raw
bytes without decoding; generated or edited finite input is encoded and
finalized before `is_sent`. Live sources stay caller-owned, and their values
are encoded per sink submission. Request heads and finite bodies can be
modified before the binding snapshots the actual sent request.

Body encoders implement only `Encoder<T>::encode` and `end`. They neither
choose heads nor control when a response opens. Simple formats use encoder
construction functions; stateful protocols retain their own framing state.
The incoming response handle provides three operations:

- `write` constructs a real sink and normally delays publication until bytes
  or completion are available. Explicitly live/message ingress retains immediate
  opening so duplex peers do not wait for their own first reply.
- `open` publishes a quiet stream immediately, with an optional prefix.
- `empty` writes a bodyless response and stops its source without reading
  values or invoking an encoder. Source ownership survives until delivery
  completes or is cancelled.

Response readers return standard streams. Whole-body and SSE helpers share
framing mechanisms without imposing a decoder trait. Stream adapters must
preserve the distinction between semantic decoder errors and broken wire
bodies, bounded refusal inspection, and cooperative frame-level progress; they
must not prefetch arbitrary later frames. Synchronous offline encoding and
decoding reuse framing/parser operations directly, never blocking on an async
stream. This permits stored-response conversion inside synchronous database
transactions.

The response reader receives the actual sent request head and initial bytes,
not the original client request. This matters for rewritten correlation IDs
and protocol framing. Client negotiation and refusal formatting still use the
original request. The ordinary success predicate accepts HTTP 2xx or a requested
101; a false result requests bounded decoding to classify the refusal. Strict
WebSocket formats require their handshake, without globally rejecting ordinary
HTTP or opaque upgrade fallback responses.

A backend returns `(Response<Body>, Option<Completion>)`. When response headers
arrive before writing finishes, `Some(completion)` reports the independent
writing result: the binding opens the incoming stream immediately but delivery
waits for that future, not response EOF. `None` means the send operation
completed its outgoing handoff; the binding cannot infer later socket writes.
A retained request-body reader may still pull input, with the existing
reader/response termination coordination. WebSocket supplies its session
completion, including late write failures and clean remote closure. Core waits
for delivery at clean receive EOF and cancels unfinished upload on refusal.

The HTTP driver owns head timing. A one-shot response can fail as a refusal
before its first bytes; quiet streams can open immediately. WebSocket
handshakes wait for the selected response's opening, so provider refusal
can still be delivered before upgrade. A provider's non-success response
is classified before its incoming stream opens; a later stream failure is
not a refusal. Status, close codes, framing, redirects, pings and keys are
protocol concerns. Outbound WebSocket handshake refusals are collected before
handoff so malformed or incomplete bodies fail rather than relay a partial
answer. Standard `http-body-util::Limited` enforces a hard 1 MiB body limit;
exceeding it fails as `upstream`. The existing 10-second handshake deadline
includes this collection. Neither bound applies to an upgraded live session.

A relayed WebSocket uses one provider call and one pair of live event streams.
A WS-to-SSE bridge reads application inputs and makes a separate call per
message. `buffer_until` and `until_end` return immediately for calls with
live inputs, because holding a response could deadlock a peer waiting to
send its next message.

## Verification

Examples are workspace members using only public APIs. Every runnable example
has tests under `examples/tests/` that run with `cargo test --workspace`.
Provider scenarios use the pinned `mockllm` fakes over real HTTP, with explicit
request counts. Generated tool-call, text, usage and native reasoning responses
supplement hand-authored wire events; named-model cases exercise the fake's
request validation. Exact byte splits, malformed input, pending streams, injected
faults and lifetime probes use small private in-process fixtures where a network
would obscure the contract. Source-only helpers under `tests/support/` build
requests and record hooks; they are not a public transport-mocking framework.
Credentialed real-provider tests are opt-in and ignored by the normal suite.

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
| Concurrent read/write progress and delivery completion | Core stream/write unit tests, `symmetric`, and `edge_runtime`. |
| Each started hook ends once, including panic and cancellation | Core `cancellation`, `panic_matrix`, `stress`. |
| `idle` counts from handle invocation and includes jobs | Core `lifecycle`, `l6_core`. |
| Lazy decoding, replay provenance, unchanged large-body relay | Core `stress`, `lifecycle`; provider `relay`, `translate`. |
| Refusal distinct from failed opening and failed first event | Core stream unit tests, `contract`, and `edge_runtime`; HTTP driver and WS tests. |
| Live inputs read once, unused inputs returned for retry | Core `upload`, `wrap`, `l5_wrap`. |
| No clean EOF after interrupted upload | Core `upload`, `wrap`; HTTP `driver_toy`. |
| Batch accounting and response holding | Core `contract`, `until`; wallet's private accounting, wire, tally and ledger tests; cost-cap and routing suites. |
| Byte-exact relay and credential isolation | Provider `relay`, `translate`; HTTP rules; WS credentials tests. |
| Synchronous raw/value/live preparation, explicit response opening and offline encoding | HTTP binding and framing tests; formats and custom-format examples. |
| Edited usage totals, SSE envelopes and bounded content provenance | Provider unit tests and `translate`; text filtering suites. |
| Supported text/tool block order and explicit failure for unrepresentable finish reasons | Provider unit tests and generated-mock `translate` cases. |
| WS backpressure, closure and handshake behavior | WebSocket integration, Axum adapter and HTTP driver tests. |
