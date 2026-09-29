> Design document; the implemented architecture is in ARCHITECTURE.md and decision 0020. Where they differ (contract fixes made during the spike, final names), ARCHITECTURE.md wins.

I have what I need. I read the core (all 13 source files), the formats, http and server crates, sdks/ws and sdks/mcp, skimmed routing, ledger, text and the examples, and read the e2e suite, crates/core/tests, the Stage C draft, both core reviews and BUGS.md. Nothing was edited or built. I counted 846 `#[test]`/`#[tokio::test]` attributes, which matches the stated suite size. Line numbers cite HEAD d58a6b2. Everything marked *(inference)* is my estimate and was not measured.

---

# respan-gateway: target architecture of the core

Status: a draft for the owner's approval. It changes no code. Base: branch `two-edges`, HEAD d58a6b2.

## 1. Summary

```
                 control plane: on_client · cx.call · on_provider · end hooks
                        (decides, races, falls back, answers, refuses, splices)
                                            │
 client ══ Wire ══╗                         ▼                          ╔══ Dialer ══ provider
      (accepted)  ║ client edge      ┌─────────────────┐   provider edge║   (dialed)
                  ║ Format::accept ──│ inputs ─▶ in-filters ─▶  ────────│─▶ Format::dial
                  ║                ◀─│ outputs ◀─ out-filters ◀ ────────│◀─
                  ╚══ heads, bytes,  └─────────────────┘  heads, bytes, ═╝
                      keys sealed        the core: events            keys sealed
                                         + opaque Origins
```

The core becomes an event router. It never sees a byte, a head or a key. Every session, and every call, is a two-way stream of the kind's events. Filters wrap that stream (the data plane). Plugins decide where it goes (the control plane).

An **edge** is a dialect (a `Format`) speaking over a connection:
- **Client edge.** The adapter hands the gateway an accepted connection (a `Wire`). The gateway picks the first served `Format` whose `accepts` matches, without reading the connection itself.
- **Provider edge.** The gateway dials it by name through the provider's `Dialer`.

Both are the same shape seen from the core: events in, events out, with the types swapped.

Heads travel only as sealed `Origin`s. The core hands the client's origin to a provider edge in exactly one case: the call carries the session's own inputs *and* goes to a format of the same name. So a byte-exact relay, and the client's key, ride along only then. Head timing is the edge's business, which fixes W1.

The core drops from 14 public types to 10. It drops from about 4,075 lines to about 2,950 *(inference)*. The byte machinery (codecs, framing, head timing) moves into one symmetric driver in `crates/http`: one Reader and one Writer serve both sides.

## 2. The edge contract

### 2.1 Terms

| Term | Meaning | Where it lives |
|---|---|---|
| Dialect | One protocol's view of a kind: `Format`. Formats of one name relay; formats of one kind translate. | core (trait); implementations in formats and sdks |
| Wire | A client connection as the adapter accepted it: `Format::Wire`, opaque to the core | wire crate (`http::Accepted`) |
| Dialer | How calls reach one provider: `Format::Dialer`, opaque to the core | wire crate (`http::Dialer`, from any `http::Transport`) |
| Edge | A dialect over a connection. What the core sees is a `Half` coming in, plus what it gives going out. | produced by `Format::accept` and `Format::dial` |
| Origin | What came with a head (for a client, the head plus a whole body), sealed. Only its dialect opens it. | alias in core; concrete type in the wire crate |
| Half | One direction of a two-way stream: the origin of its head, and its events or why there are none | alias in core |

### 2.2 Exact signatures (core)

```rust
// crates/core/src/format.rs — aliases, not types
pub type Events<T> = BoxStream<'static, Result<T, Fault>>;
pub type Origin    = Arc<dyn Any + Send + Sync>;
pub type Half<T>   = (Option<Origin>, Result<Events<T>, Fault>);
pub type Ended     = Box<dyn FnOnce(Result<(), Fault>) + Send>;

pub trait Format: Send + Sync + 'static {
    type Input:  Clone + PartialEq + Send + Sync + 'static;
    type Output: Clone + PartialEq + Send + Sync + 'static;
    /// A client connection as its adapter hands it over.
    type Wire:   Send + 'static;
    /// How calls reach one provider: a URL and keys, a process, a socket.
    type Dialer: Send + Sync + 'static;

    fn name(&self) -> &str;
    /// Whether this dialect speaks on `wire`. It may look inside; the gateway does not.
    fn accepts(&self, wire: &Self::Wire) -> bool;
    /// What `until_content` stops at. By default, every output.
    fn is_content(&self, _output: &Self::Output) -> bool { true }

    /// The client edge. Resolves once the client's request is in, with its origin
    /// and the inputs that follow (Err: the request broke; it is refused before
    /// any plugin). `reply` resolves with what to send back: the session's
    /// extensions, the provider's origin if the answer is a relay, and the outputs
    /// or a refusal, plus `Ended`, to call once written. When to write the head is
    /// this edge's decision.
    fn accept(
        &self,
        wire: Self::Wire,
        reply: BoxFuture<'static, (Extensions, Half<Self::Output>, Ended)>,
    ) -> BoxFuture<'static, Half<Self::Input>>;

    /// The input that came with a client's head, read from its origin; `None` if
    /// none came. The gateway calls it only when a value is needed.
    fn opening(&self, origin: &Origin) -> Option<Result<Self::Input, Fault>>;

    /// The provider edge.
    /// - `relay`: the client's origin, only on a relay.
    /// - `opening`: the first input to write from its value. `None` on a relay
    ///   means "the origin's own, as it came".
    /// - `later`: the inputs that follow.
    /// - Each input's bool is true if it is the client's as decoded, which decides
    ///   whose fault an input this dialect cannot write is (`Fault::of_input`).
    ///
    /// Sync `Err`: not sent. Future `Err`: sent, no answer. Future
    /// `Ok((origin, Err))`: answered and refused. Future `Ok((origin, Ok))`: open.
    fn dial(
        &self,
        dialer: &Self::Dialer,
        extensions: Extensions,
        relay: Option<&Origin>,
        opening: Option<(&Self::Input, bool)>,
        later: Events<(Self::Input, bool)>,
    ) -> Result<BoxFuture<'static, Result<Half<Self::Output>, Fault>>, Fault>;
}
```

Every piece has a reason:

- **The `reply` future.** A client's answer exists only after the hooks run.
- **`Ended`.** The edge alone knows whether its end was written: an encoder can fail at the end, a refusal may be unwritable, the peer may leave. Dropping `Ended` uncalled means `abandoned`. It wraps the session's `EndHooks`, so the exactly-once guarantee is kept by construction.
- **`Option<Origin>` in both directions.** Relay heads go both ways (today `deliver.rs:65-66` filters the provider's head on `relayed`).
- **The `Result` inside `Half`.**
  - On the client's incoming side it is a broken body (today `gateway.rs:199-205`).
  - On the client's outgoing side it is a refusal.
  - On the provider's incoming side it is a refusal detected at the head (the W1 fix).
- **`opening()`.** Lazy decode (today `input.rs:104-109`).
- **The staging of `dial`.** It is today's `sent`, `answered` and head-only facts: `call.rs:246` and `call.rs:248`, `provider.rs:149-151`.
- **The `(T, bool)` pairs.** The fault split for later inputs, which is pinned by `codec.rs::r8_the_fault_of_content_that_cannot_be_written_is_whose_it_is`.
- **`extensions` in both directions.**
  - Going out to a provider: keys, `CallHeaders`, `Owner`.
  - Going back to a client: room for a response-hint extension later, without changing this signature again (principle 11).

### 2.3 Rules of the contract

1. **An `Events` that nothing will follow is already ended**: its first poll is `Ready(None)`. The core polls a client's `later` once when the session opens, and a driver polls a call's `later` once in `dial`. That single fact replaces `Exchange::streams` (`transport.rs:35-42`) and `ClientContext::later`.
2. **`accept` resolves when the request is in.** A one-shot HTTP edge reads its body first; the origin carries it and `later` is ended. A session edge resolves at once, with `later` being its frames. The core runs `on_client` when `accept` resolves, so `idle` counts the body read (the core enters `InFlight` before `accept`).
3. **A provider edge classifies its answer at the head.** For HTTP, a non-2xx answer (other than a 101 to a call that asked to switch) is read whole and resolves `Ok((origin, Err(fault)))`. Such a response fails its first read.
4. **The client edge owns its head timing.**
   - A refusal is written as one.
   - An open answer is written when the edge decides: HTTP one-shot at its first bytes, with a failure before any bytes becoming a refusal (today's `deliver.rs:106-127`); an upgrade at once.
5. **The edge calls `Ended` exactly when the answer's end is out**, or drops it.
   - `Ended(Err(f))` also ends the answer's call with `f`, as `deliver.rs:120` does today.
   - When the edge drops the output stream unread, the core ends that call `Ok` if its end is already ready, else `abandoned`. That replaces `close_unread` (`response.rs:370-393`).

### 2.4 Format selection without opening the connection

```rust
pub async fn handle<W: Send + 'static>(&self, wire: W, extensions: Extensions)
    -> BoxFuture<'static, Result<(), Fault>>;
```

- `handle` erases `&wire` as `&dyn Any`. Each served format's erased `accepts` downcasts it to its own `F::Wire` and returns false for any other type. The first true wins. This is today's rule (`gateway.rs:187-196`, `format.rs:294-297`), keyed on the wire type instead of a head type.
- If no format accepts, the core drops the wire and the end resolves `Err(not_found)`. The adapter, seeing its reply slot dropped, writes that end plainly (as `http/lib.rs:213-218` does today).
- There is one generic. The caller built `W`, so it knows its type (principle 10).

### 2.5 Accepted and dialed

The two sides are the same shape; three things stay asymmetric by nature:
- **Who initiates.** The adapter builds a `Wire` and calls `handle`. The core calls `dial` with the provider's stored `Dialer` after every `on_provider` passed.
- **The direction of the types.** A client edge gets `Half<Input>` in and gives `Half<Output>` out; a provider edge the reverse.
- **Laziness and the fault split.** These exist only on the input side, hence `opening` and the `bool`.

### 2.6 One-shot versus streaming, with no flag

There is no `streams` field in the core. One input path and one output path serve both cases; only the data differs:

| | One-shot HTTP | WebSocket |
|---|---|---|
| Client's `accept` resolves | after the body is read | at once |
| Origin | head plus body | head only |
| `later` | already ended | the frames |
| `cx.input()` | `opening(origin)` | `None`, so an `Err` (as today) |
| Call's `later` | ended, so the driver sends whole | open, so the driver sends streamed |
| Client's head | at first bytes (the edge's rule) | when the reply resolves |

The only data-dependent rule in the core is that `until_content` and `until_end` return at once for a call that carries session inputs still flowing. Waiting for output there can deadlock a peer that waits for input. This is today's rule (`response.rs:183`), derived instead of flagged.

### 2.7 HTTP's composition (`crates/http`)

```rust
// Moved from core; signatures unchanged except that the head types are HTTP's.
pub trait Codec: Send + Sync + 'static {
    type Input: Clone + PartialEq + Send + Sync + 'static;
    type Output: Clone + PartialEq + Send + Sync + 'static;
    fn name(&self) -> &str;
    fn accepts(&self, head: &Ask) -> bool;
    fn request_decoder(&self, head: &Ask) -> Box<dyn Decoder<Self::Input>>;
    fn request_encoder(&self, client: Option<&Ask>, body: &mut Bytes, streams: bool)
        -> Box<dyn Encoder<Self::Input, Ask>>;
    fn response_decoder(&self, answer: &Answer, call: &Ask, sent: &Bytes)
        -> Box<dyn Decoder<Self::Output>>;
    fn response_encoder(&self, answer: Option<&Answer>, ask: &Ask, body: &Bytes,
        refusal: Option<&Fault>) -> Box<dyn Encoder<Self::Output, Answer>>;
    fn is_content(&self, _: &Self::Output) -> bool { true }
}
pub trait Decoder<T>    { /* unchanged, format.rs:118-126 */ }
pub trait Encoder<T, H> { /* unchanged, format.rs:129-165 */ }
#[async_trait] pub trait Transport: Send + Sync + 'static {
    async fn send(&self, call: Exchange<Ask>) -> Result<Exchange<Answer>, Fault>;
}
pub struct Exchange<H> { pub head: H, pub body: Frames, pub extensions: Extensions,
                         pub relayed: bool, pub streams: bool }   // moved from core
pub type Frames = BoxStream<'static, Result<Bytes, Fault>>;       // was core::Body

pub struct Accepted { /* head, frames, streams, reply slot */ }
impl Accepted {
    pub fn new(head: Ask, body: Frames, streams: bool)
        -> (Accepted, impl Future<Output = Option<http::Response<Body>>>);
    pub fn head(&self) -> &Ask;
}
pub struct Dialer(Arc<dyn Transport>);
impl<T: Transport> From<T> for Dialer { /* so .provider(name, OpenAiChat, HttpTransport::new(u)?) is unchanged */ }

pub fn accept<C: Codec>(c: &C, w: Accepted, reply: /* as Format::accept */) -> BoxFuture<'static, Half<C::Input>>;
pub fn opening<C: Codec>(c: &C, o: &Origin) -> Option<Result<C::Input, Fault>>;
pub fn dial<C: Codec>(c: &C, d: &Dialer, /* the rest as Format::dial */) -> /* as Format::dial */;

#[macro_export] macro_rules! dialect { ($t:ty) => {
    impl $crate::core::Format for $t {
        type Input = <$t as $crate::Codec>::Input; type Output = <$t as $crate::Codec>::Output;
        type Wire = $crate::Accepted; type Dialer = $crate::Dialer;
        fn name(&self) -> &str { $crate::Codec::name(self) }
        fn accepts(&self, w: &$crate::Accepted) -> bool { $crate::Codec::accepts(self, w.head()) }
        fn is_content(&self, o: &Self::Output) -> bool { $crate::Codec::is_content(self, o) }
        fn accept(&self, w: $crate::Accepted, r: _) -> _ { $crate::accept(self, w, r) }
        fn opening(&self, o: &$crate::core::Origin) -> _ { $crate::opening(self, o) }
        fn dial(&self, d: &$crate::Dialer, e: _, r: _, o: _, l: _) -> _ { $crate::dial(self, d, e, r, o, l) }
    }
} }
```

The driver is one symmetric pair:
- **`Reader<T>`** turns frames into events through a `Decoder`. It serves the client's later inputs and the provider's outputs.
- **`Writer<T, H>`** turns events into a head plus frames through an `Encoder`, with `open`, framing, an end on `Err`, and panic guards. It serves the provider's call and the client's answer.

They absorb today's mirrored pairs:
- `deliver.rs` Pipe with `call.rs:265-398`, into `Writer`.
- `response.rs:320-365` with `input.rs:275-407` and `:180-196`, into `Reader`.

Head timing (rule 4) and the verdict at the head (rule 3) live in the driver's `accept` and `dial`.

### 2.8 WebSocket's composition (sdks/ws)

- `JsonEvents` becomes `impl Codec` plus `impl_format!(JsonEvents)`; its method bodies are unchanged.
- `WsTransport` becomes `impl http::Transport`. It drops `type Ask/Answer` (`transport.rs:173-176`).
- `serve_ws` builds `Accepted::new(head, frames, true)`, calls `handle`, and awaits its reply. On a 101 it upgrades and pumps; otherwise it returns the answer as it is. The `answer()` workaround (`serve.rs:100-115`) goes, because W1 is fixed in the driver.

## 3. The core

### 3.1 Public signatures

```rust
impl Gateway {
    pub fn new() -> Gateway;
    pub fn serve<F: Format>(self, format: F) -> Result<Gateway, Fault>;
    pub fn provider<F: Format>(self, name: impl Into<String>, format: F,
                               dialer: impl Into<F::Dialer>) -> Result<Gateway, Fault>;
    pub fn plugin(self, name: impl Into<String>, plugin: impl Plugin) -> Result<Gateway, Fault>;
    pub fn target(self, provider: impl Into<String>) -> Result<Gateway, Fault>;
    pub async fn idle(&self);
    /// Serve `wire` in the first format that accepts it; resolves once the client
    /// edge has its reply, with a future of how the session ended.
    pub async fn handle<W: Send + 'static>(&self, wire: W, extensions: Extensions)
        -> BoxFuture<'static, Result<(), Fault>>;
}

impl ClientContext {           // unchanged but for three removals
    pub fn format(&self) -> &str;
    pub fn extensions(&self) -> &Extensions;
    pub fn extensions_mut(&mut self) -> &mut Extensions;
    pub fn is_kind<T: Any>(&self) -> bool;
    pub fn input<T: Any>(&self) -> Result<&T, Fault>;
    pub fn take_inputs<T: Any + Clone + Send>(&mut self) -> Events<T>;
    pub fn target(&self) -> Option<&str>;
    pub fn set_target(&mut self, provider: impl Into<String>);
    pub fn filter<T: Clone + PartialEq + Send + Sync + 'static>(&mut self, f: impl Filter<T>) -> Result<(), Fault>;
    pub fn call(&mut self, provider: impl Into<String>) -> Call;
    // removed: body() (context.rs:141), edit_response_head() (:220), relay() (:284)
}

impl ProviderContext {         // unchanged but for one removal
    pub fn client(&self) -> &ClientContext;   pub fn provider(&self) -> &str;
    pub fn format(&self) -> &str;             pub fn is_kind<T: Any>(&self) -> bool;
    pub fn input<T: Any>(&self) -> Result<&T, Fault>;
    pub fn filter<T: Clone + PartialEq + Send + Sync + 'static>(&mut self, f: impl Filter<T>) -> Result<(), Fault>;
    pub fn is_sent(&self) -> bool;       // dial returned Ok
    pub fn is_refused(&self) -> bool;  // answered, and refused at the head
    pub fn extensions(&self) -> &Extensions;  pub fn extensions_mut(&mut self) -> &mut Extensions;
    // removed: edit_call() (provider.rs:173)
}

impl Call {  // unchanged: Future<Output = Result<Response, Fault>> + Clone + Send + Sync
    pub fn with_input<T: Any + Send + Sync>(self, input: T) -> Self;
    pub fn with_extension<T: Clone + Send + Sync + 'static>(self, value: T) -> Self;
}
impl Response {  // unchanged
    pub fn stream<T, S>(stream: S) -> Self where T: Any + Send + Sync, S: Stream<Item = Result<T, Fault>> + Send + 'static;
    pub async fn until_content(self) -> Result<Response, Fault>;
    pub async fn until_end(self) -> Result<(Response, Result<(), Fault>), Fault>;
    pub fn outputs<T: Any + Clone + Send>(self) -> Events<T>;
}
// Plugin (4 hooks), Filter<T> (edit, end), Extensions: unchanged.
// Fault: unchanged, plus two public constructors:
impl Fault {
    pub fn abandoned() -> Fault;                    // was pub(crate), fault.rs:185 (BUGS W4)
    pub fn of_input(self, clients: bool) -> Fault;  // bad_request if the client's, else config (was input.rs:52-58)
}
```

### 3.2 The public types: 10

| Type | Why it must exist |
|---|---|
| `Gateway` | The one entry point and registry. |
| `Format` | The dialect. The core holds the served ones and picks one (principle 3); it is the only thing that turns a wire into events. |
| `Plugin` | The control-plane extension point. |
| `Filter<T>` | The data-plane extension point: a stateful stream edit with one end. |
| `ClientContext` | The session as `on_client` sees it, by `&mut`, so it may call and take inputs. |
| `ProviderContext` | A call as `on_provider` sees it. It is a separate type so that no provider hook can call, which is checked at compile time (the `lib.rs:31-55` doctests). |
| `Call` | A lazily sent, cloneable, parkable call. `Clone` gives retries and races; the slot gives parked-call abandonment (`call.rs:117-130`). |
| `Response` | An answer that can be held (`until_*`), read (`outputs`) or made (`stream`). |
| `Fault` | The one error type. |
| `Extensions` | The opaque bag between the adapter, plugins and edges. |

**Why not 8.** Merging the two contexts, or folding `Call` into `Response`, would change every plugin signature and lose the compile-time guarantee of 0004. That conflicts with principle 11. It is Open question 1.

### 3.3 Today's 14, one by one

| Today | Becomes |
|---|---|
| Call, ClientContext, ProviderContext, Response, Filter, Plugin, Fault, Extensions, Gateway | Stay. Removals and additions are listed above. |
| Format | Stays, reshaped into the dialect plus the edge contract (§2.2). |
| Decoder, Encoder | Move to `crates/http`, signatures unchanged. |
| Exchange | Moves to `crates/http` (its `relayed` and `streams` become HTTP's). |
| Transport | Moves to `crates/http` (`Ask`/`Answer` are fixed to HTTP). |
| `Body`, `Head` (aliases) | Leave the core. `Body` becomes `http::Frames`. |

New core aliases: `Events`, `Origin`, `Half`, `Ended`.

The core also stops depending on `bytes`: it can no longer touch a byte.

### 3.4 Inside the core *(module sizes are inference)*

| Module | Content | Lines, about |
|---|---|---|
| `gateway.rs` | builder, `handle`, `idle` | 230 |
| `format.rs` | the trait and its erasure (`AnyFormat`: kind TypeIds, downcasts) | 230 |
| `session.rs` | turns the answer into the client's reply: checks, refusal, relay origin, `Ended` | 160 |
| `leg.rs` | **one** layer engine: pull, run filter layers, end each layer once with its tail flowing on, cut gives `Err(abandoned)`, guarded polls. Used for session inputs to a call, provider events to a `Response`, and a `Response` to the client. | 260 |
| `opening.rs` | the lazy opening input and whose it is | 120 |
| `call.rs` | `Call`, `Sending`/`OpenCalls` (unchanged), `send` (hooks, then `dial`), `relay_origin` | 320 |
| `response.rs` | `Response`, `Batch`/`Unread` (A1), `until_*`, `outputs` | 280 |
| `context.rs`, `provider.rs` | accessors | 360 |
| `plugin.rs`, `lifecycle.rs`, `fault.rs`, `extensions.rs` | unchanged, and the `Extensions` part of `transport.rs` | 1,000 |

Total: about 2,950, against 4,075 today (about −28%).

## 4. Data plane

- **Per session.** Inputs go from the client edge's `later` through the client input filters (first added, first applied) and the call's input filters to the provider edge. Outputs go from the provider edge through the call's output filters and `Response` (with `ahead` under `until_*`) to the client output filters (last added, first applied) and the client edge. The order is today's (ARCHITECTURE "The response pipeline" §4).
- **Per call.** Same shape. A call's own input (`with_input`) gives `opening = Some(own, false)` and an ended `later`.
- **Splicing.**
  - Returning a call's `Response` from `on_client`, or the default target call, splices it: the call's inputs read the session's `later` (read once, given back if unread: `input.rs:410-419`), and its outputs become the session's.
  - A bridge (`take_inputs`, then `call.clone().with_input(..)`, then `outputs()`, then `Response::stream`) splices by hand, as today.
- **`until_content` / `until_end`.** Unchanged (`response.rs:174-194`), with two differences:
  - A refused `Response` (rule 3) fails at once.
  - "Return at once" applies when the call carries session inputs that were still flowing when it was made (§2.6).
- **Filters, both directions.**
  - Adding an input filter applies it to the opening now. If the inputs have ended, it also ends it at once and requires exactly one input (today's whole-body rule, `input.rs:129-158`). Otherwise it joins the shared layer for later inputs (today's streaming rule).
  - It is one function whose behaviour depends on the data, not on the request type.
  - Every value, keep-alives included, passes every filter (0010).
- **Backpressure.** Every stage pulls. `leg.rs` holds at most one batch. Only `until_*` buffers, as documented (0011). The driver pulls a provider only as the client edge writes. `WsTransport` keeps one frame in flight (`transport.rs:228-229`), and `serve_ws` uses a 0-capacity input channel (`serve.rs:71`).

## 5. Control plane

- **Hooks.** `on_client` (pass, answer, refuse), `on_provider` (before every call; `Err` fails it), and the end hooks. The semantics are unchanged (0004, 0018).
- **Calls.** `cx.call(p)` → `send`: run every `on_provider`, compute `relay_origin`, pick opening and later, then `format.dial`.
  - Sync `Err`: the call fails, not sent.
  - Future `Err`: `Call` is `Err` (sent).
  - `Ok`: `Call` is `Ok(Response)`, refused or open.
  - Routing's "responded" (`routing/src/lib.rs:145-156`) and the ledger's facts (`accounting.rs:110-118`) keep their meaning.
- **Races and fallback.** Unchanged helpers. A dropped call ends `abandoned` through `OpenCalls`.
- **Cache answers.** `Response::stream` of stored values. Values keep their origins, so a client of the same dialect gets their bytes; the head is written fresh. `cx.relay` goes.
- **Refusals.** `on_client`'s `Err`, a broken request, a plugin panic, or a refused answer: the core resolves the reply `(ext, (None, Err(f)), ended)`. On a non-relay, `f` loses the provider's words (`Fault::unrelayed`, today at `deliver.rs:121-125`).
- **Fault split.**
  - The core decides whose each input is: equal to its decoding means the client's (`opening.rs`, and the later leg with `format.same`).
  - The driver applies `Fault::of_input(clients)` at its one `write_input` call site.

**What each SDK changes:**
- **routing**: `Watchdog<T: http::Transport>`; drop the `type Ask/Answer` lines (`lib.rs:285-286`). `first_success`, `race`, `Until` and `Attempt`: nothing.
- **ledger**: nothing. `is_sent` and `is_refused` keep their names and their tested values (§11 lists two untested shifts).
- **text** (Holdback, Placeholders, guard, PII): nothing; they are filters only.
- **mcp**: `StreamableHttp` becomes `impl Codec` plus `impl_format!`; `StdioTransport` becomes `impl http::Transport`. It keeps reading `Credential` for ownership (`stdio.rs:322-330`).
- **ws**: see §2.8.

**Plugin code changes, all of them:**
1. `examples/jobs.rs:139` hashes the conversation's origin bytes instead of `cx.body()`: `cx.input::<Conversation>()?.origin(cx.format()).map_or(&[][..], |o| &o.bytes)`. It is already chat-only.
2. `edit_call`, `edit_response_head` and `relay` go. No SDK or example uses them; about 30 tests do (§10).
3. Custom transports import `http::Transport` and `http::Exchange`: `fallback.rs` `Slow`, `custom_transport.rs` `Local`, `mcp_proxy`, `local_proxy`.
4. Custom formats: `impl Codec` plus `impl_format!(X)`.
5. Embedders use `Accepted::new` and `handle`, or just `http::serve`.

## 6. Relay and credentials

- **The provenance fact.** A call carries the session's inputs if and only if it has no `with_input`. `Call` has no other setter (`call.rs:69-82`).
- **One function decides relay:**

  ```rust
  fn relay_origin<'a>(cx: &'a ClientContext, call_own: bool, p: &Provider) -> Option<&'a Origin> {
      (!call_own && cx.format.name() == p.format.name()).then(|| &cx.origin)
  }
  ```

  This is today's rule (`call.rs:169`), moved from a flag on `Exchange` to the only way an `Origin` reaches a provider.
- **The origin is unforgeable by a plugin.**
  - No accessor returns an `Origin`, and only the core calls `dial`.
  - A plugin cloning the client's `Conversation` into `with_input` copies the value's *body* origin, whose `Conversation::came` is public (`conversation.rs:72`, `relayed.rs:48`). The body may then be written into the client's bytes, as today (`relay.rs::a_calls_own_input_copied_from_the_clients_keeps_its_bytes`). But `relay` is `None`, so the call gets no client head and no key.
- **Byte-exact relay, requests.** On a relay with an opening never changed, `opening` is `None`: the driver writes the origin's head and raw body, and nothing is parsed. A changed value is patched into its own origin bytes by the dialect (`common.rs:119-128`).
- **Byte-exact relay, responses.** The provider's origin goes back to the client edge only on a relay, and its dialect writes the provider's head. Values write their own bytes (`openai.rs`, `anthropic.rs` relay paths).
- **Error passthrough.** A refusal fault carries `with_answer(name, http::Response<Bytes>)` (`whole.rs:34-45`). A client edge of that name writes it as it came (`http/lib.rs:334-346`): a relayed 429 keeps its body and `Retry-After`. Otherwise the refusal is written in the client's own words.
- **Credentials.**
  - The adapter still lifts `Credential` into `Extensions` (`http/lib.rs:205`). The core carries the bag unopened.
  - The driver sets `http::Exchange::relayed` if and only if it wrote from an origin. `http::credentials()` (`http/transport.rs:86-111`) is unchanged: transport or call keys first, else the client's `Credential` only when `relayed`.
  - A plugin removing `Credential` still stops pass-through (`rules.rs::a_plugin_removes_the_gateways_own_credential`).
- **Use case 12**, a same-dialect relay with no plugins: the origin's head (unknown and beta headers kept, hop headers stripped), the raw body unread, the key restored, and the provider's head and bytes back as they came.
- **Security argument.** The client's head and key can reach a provider only through (a) `relay_origin`, which needs a core-held provenance bit and a name match fixed at registration, and (b) the driver restoring `Credential` only when it wrote from that origin. Plugins can neither set (a) nor reach (b). A translated or own-input call is written from a fresh head. What a plugin can forge is `Relayed<T>` body bytes it could write anyway; plugins are compiled-in, trusted code (0002). A custom `Format` or `Transport` can still ignore the rule, as today.

## 7. Streaming and heads

- **SSE.** The dialect decodes each event (a comment is a non-content value, 0010). The client dialect writes unchanged values as their bytes. `Encoder::open` (an SSE comment, or done for HEAD/204/205/304) is applied by the driver's `Writer`.
- **WebSocket frames, both ways.** One text frame is one message. Binary frames, closes, pings and `CloseCode` stay in `serve_ws` and `WsTransport`, unchanged (the table at `sdks/ws/src/lib.rs:20-33`).
- **Head timing, per edge.** HTTP one-shot writes at first bytes, with a failure before them refused. An upgrade writes when the reply resolves. A refusal is written as one.
- **The W1 fix, traced:**
  1. The provider answers 403 to a relayed upgrade.
  2. `WsTransport` returns the answer (`transport.rs:201-214`).
  3. The driver's `dial`: not a 101 to a call that asked, so it reads the body through the codec's decoder (`whole::decoder` fails with the answer), giving `Ok((origin, Err(fault)))`.
  4. `Call` is `Ok(refused Response)` and `is_refused` is true.
  5. The core resolves the client's reply as `Err(fault)`, keeping the answer because this is a relay.
  6. The WS client edge writes the refusal through `JsonEvents::response_encoder` (`events.rs:277-291`): 403 plus body, as it came. Then it calls `Ended(Err)`.
  7. `serve_ws` sees no 101 and returns it.

  `a_refused_upgrade_is_a_complete_answer` passes; `today_a_refused_upgrade_is_a_head_and_a_failing_body` is inverted.
- **Closes.** The client edge decides the session's end through `Ended` (W5 semantics: a client Close ends `Ok`, a drop is `abandoned`). Keep-alives are pings answered per hop, not carried (`close.rs::pings_...`).

## 8. Where each guarantee lives

| Guarantee | The one place | Pinned by |
|---|---|---|
| End hooks exactly once, on drop and panic too | core `lifecycle.rs` `EndHooks`; `Ended` owns them | core `cancellation`, `panic_matrix`, `stress` (ported); ws `concurrency.rs` |
| `Gateway::idle` | core `InFlight`, entered in `handle` before `accept` | core `lifecycle::idle_waits_for_a_request_still_reading_its_body`; e2e `sigterm_...` |
| Cancellation; parked calls end abandoned | core `call.rs` `Sending`/`OpenCalls`, unchanged | core `cancellation`; testkit `streams::a_pending_call_left_unpolled_...`; `leaks.rs` |
| A call ends when its end is read (A1) | core `response.rs` `Batch`/`Unread` | core `until.rs`; routing `a_whole_body_attempt_ends_with_its_role_set` |
| A cut or broken upload ends `Err` (B1) | events: core `leg.rs` (cut gives `Err(abandoned)`, never a clean end). Bytes: driver `Writer` (`Encoder::end(&Err)`, then the body's `Err`) | core `upload.rs` (5 ported); http `driver.rs::a_body_ends_at_its_first_failure` (moved); ws `transport.rs::a_call_that_ends_in_an_err_...` |
| `until_content` commit, `until_end` | core `Response::read_until` | core `until.rs`; routing tests |
| Fault split | core decides (`opening.rs`, `leg.rs`); driver applies `Fault::of_input` once | testkit `codec::r8_...`, `line::v9_1_...`, `custom::an_input_that_cannot_be_translated_...` |
| Lazy decode | core `opening.rs` (`OnceLock` over `Format::opening`); relays pass `opening: None` | core `stress::a_big_body_relays_whole` (toy counts decodes: 0); e2e `a_request_the_provider_refuses_is_relayed_as_it_refused` |
| Byte-exact relay, both ways | dialect origins plus the driver writing origins | e2e §1–2; ws `passthrough.rs`; testkit `relay.rs` |
| Key only on a same-dialect relay | core `relay_origin`; `http::credentials` | e2e `an_openai_clients_key_does_not_reach_...`; testkit `translate::only_a_relay_is_marked_...`; `http/transport.rs` tests; `rules.rs`; ws `credentials.rs` |
| Keep-alives, framing | dialects plus driver `Writer` | testkit `streams`, `line`; ws `close::pings_...` |
| Backpressure | pull everywhere; `leg.rs` holds one batch | ws `backpressure.rs` |
| Metering through filters | core `plugin.rs` Filters on the output leg | examples `billing`; testkit `translate::usage_counts_even_when_...` |
| Panic guards | core `guarded` (plugins, filters, `Format` methods) plus `leg.rs` guarded polls of every edge stream and future; driver guards codec calls, including detached body reads (B2) | core `panic_matrix`, `panics` (ported); http `driver.rs` B2 test (moved) |
| W1 head timing | driver: verdict at the head, upgrade head at the reply | ws `kernel::a_refused_upgrade_is_a_complete_answer` (un-ignored) |

## 9. Crates

| Crate | After |
|---|---|
| core | Events-only kernel, the 10 types. Depends on async-trait, futures, serde_json, tokio. No `bytes`, no `http`. |
| http | The HTTP wire, both ends: codec traits, one driver (`Reader`, `Writer`, `accept`, `opening`, `dial`), `impl_format!`, `Transport`, `Exchange`, `Frames`, `Accepted`, `Dialer`, the adapter (`serve`, `handle`, unchanged API), rules, `Credential`, `Asked`, `Status`. About 982 lines now, about 2,100 after *(inference)*. |
| formats | Dialects: `impl Codec` plus `impl_format!`; bodies unchanged. |
| server | axum router plus `HttpTransport`. reqwest stays here and is untouched. |
| sdks | ws (axum ws, tungstenite), mcp, routing, ledger, text: as in §5. |
| testkit | `MockAt: http::Transport`; `serve` re-export unchanged, so about 400 scenario tests keep their calls. |

reqwest stays in `server`; axum in `server` and `ws`. No new crate.

## 10. Migration: every step compiles and stays green

1. **Spike** (throwaway, time-boxed to 2–3 days). The kernel on the contract with a toy edge only. Port `cancellation`, `until`, `upload` (B1), the relay part of `lifecycle`, and a `panic_matrix` subset, and add a toy W1 scenario.
   - Exit criteria: core ≤ 3,000 lines; `Ended` passes the drop sweep; no watcher tasks needed.
   - Size: about 1,200 lines, not merged.
2. **Remove the head hooks and `body()`** (decision 0020a).
   - Core −150.
   - `jobs.rs` switches to origin bytes.
   - About 22 tests rewritten to the same path through a `MockTransport` reply (for example `a_204_ends_complete_...`, `a_205_is_sent_with_no_body`, `leaks.rs` relay, `codec::a_call_says_whether_it_was_handed_...` using a hook refusal).
   - About 8 tests deleted with the feature, the head edits that set a status (`relay.rs::a_head_edit_sets_a_refusals_status_...`, `streams.rs::a_plugins_head_edit_sets_its_status`, …); their panic cases move to a filter panic.
   - Low risk.
3. **The kernel on the contract, legacy inside the core.**
   - Add the new trait under a temporary name (`Dialect`) and `Gateway::open<W>`, and rewrite the internals on it.
   - Serve the old `Format`, `Transport` and `handle(Exchange)` through a private `core/src/legacy/` driver: today's byte code, moved and not rewritten, implementing `Dialect` for every old `Format` by a blanket impl.
   - W1 falls out here: invert the `sdks/ws/tests/kernel.rs` pair.
   - Add the 22 core tests ported to the toy edge beside the old ones.
   - Core temporarily about 4,000 src plus 900 test lines.
   - **This is the riskiest step.**
4. **The HTTP driver.** Add codec traits, driver, `Accepted`, `Dialer` and `impl_format!` to `crates/http`, with its own tests (the 6 byte-level core tests moved here onto a toy codec). Nothing uses it yet. http +about 1,200.
5. **The flip** (one compiler-driven PR, about 80 files, mostly 2–5-line edits):
   - Delete the old `Format`, `Transport`, `Exchange`, `Body`, `Head`, `Decoder`, `Encoder`, `handle` and the legacy driver.
   - Rename `Dialect` to `Format` and `open` to `handle`.
   - Formats: 39 `impl Format` become `impl Codec` plus `impl_format!`.
   - Transports: 34 `impl Transport` change imports.
   - `http::serve` builds `Accepted`; `config.rs:10-95` changes its bounds.
   - The old toy tests go; their ports landed in step 3.
   - Core −1,300; tests ±1,000.
6. **Clean-up.** Drop `bytes` from the core (import churn from `respan_gateway_core::bytes` to `respan_gateway_http::bytes`); ARCHITECTURE; decision 0020 and its amendments.

**How the 28 core tests port:**
- About 22 stay on the toy edge: `cancellation` (4), `lifecycle` (5), `until` (3), `stress` (2), `panic_matrix` (1, points redrawn onto `Format` methods, edge streams, plugins and filters), 2 of `panics`, and 5 of `upload`.
- 6 move to `crates/http` driver tests: the encoder panic on a later frame (B2), the 3 opening-bytes tests, "a body ends at its first failure", and "an empty frame is a frame".
- The new toy has no codec: the dialer returns events. About 300 lines, against 667 today.

**De-risking step 3:**
- the spike first;
- keep all 846 old tests as the oracle;
- move the legacy byte code rather than rewrite it;
- run the ws real-socket suite and both the panic matrix and the cancellation sweeps on both APIs in CI;
- land the step as one reviewed PR.

**The cost of steps 4 and 5:** the driver exists twice for one step. Merging steps 4 and 5 avoids that at the price of a bigger PR.

## 11. Costs, risks and what gets worse

- **`Format` is heavier.** Four associated types, three edge methods, and a hairy `dial` signature. HTTP dialects hide it behind `impl_format!`, a macro in the public API.
- **Guarantees span two layers.** B1 and the panic guards have an event half in the core and a byte half in the driver. A second wire must write its own driver. A faulty third-party edge can break what the core used to enforce alone.
- **The workspace is roughly flat.** Core −1,125, http +1,100, net about −100 to +200 *(inference)*.
- **Rule 1 is a readiness contract.** "Ended at once" relies on a single non-blocking poll. The explicit `Option<Events>` alternative is Open question 7.
- **Timing and billing shifts.**
  - A provider's non-2xx `Call` resolves after its (small) error body is read, not at its head.
  - `is_refused` is false for a 200 carrying an error and for an SSE stream whose first event is an error. The ledger then charges at least the reserve, not 0, against 0015's stated consequence. Neither case is tested; every tested case (`codec::a_call_says_whether_its_response_was_head_only`, `ledger every_call_settles_exactly_once`) keeps its value.
- **Lost plugin powers.** Plugins lose head edits and `cx.relay`: no custom success status, no header edits on calls beyond `CallHeaders`, no replaying a cached head.
- **A refusal before the first bytes, with held values.** When filters release held values at a failing end and those encode to bytes, the client may now get them plus an error event where today it gets a clean refusal (today's `deliver.rs:243` subtlety). No test pins it *(inference)*.
- **The flip is a flag day**: compiler-driven, but about 80 files. The legacy driver temporarily grows the core in step 3.
- **Not addressed.** The per-call `ClientContext` copy (`context.rs:193-209`).
- **Test changes.** About 8 tests deleted with head edits, about 22 rewritten, 6 moved. New toy and driver tests keep the count at 846 or more *(inference)*.

## 12. Open questions for the owner

1. **Ten types, or eight by merging the contexts and folding `Call`?** Recommend 10: eight breaks every plugin and the compile-time "only `on_client` calls" guarantee.
2. **The edge contract on `Format` plus a `impl_format!` macro (10 types), or a separate core `Wire<F>` trait implemented by `http::Accepted` (11 types, no macro, a smaller `Format`)?** Recommend `Format` plus `impl_format!`, unless you prefer 11 types to a macro.
3. **Remove `edit_call`, `edit_response_head`, `relay` and `body()`?** Recommend yes. If plugins later need response headers, add an http extension read by the client edge; the contract already passes the extensions to it.
4. **Credentials.** Keep lifting them into `Credential` and restoring on relay (recommended: tests and plugins read and remove it), or leave them in the origin.
5. **Accept the two `is_refused` shifts, or have the driver read 2xx unary bodies before resolving?** The second is exact, but `Call` then resolves later. Recommend accepting, and noting it in 0020.
6. **Make `Fault::abandoned` and `Fault::of_input` public?** Recommend yes: edges outside the core need both.
7. **"Ended at once" convention, or `Option<Events>` for "nothing follows"?** Recommend the convention: no mode appears in the types.
8. **Drop `bytes` from the core?** Recommend yes; it is cheap proof of sealing.
9. **Codec traits in `http`, or a new `codec` crate?** Recommend `http`: every current wire is HTTP.
10. **Merge migration steps 4 and 5?** Recommend separate, for review size.

### Critical Files for Implementation
- /home/mimi/respan/respan-gateway/crates/core/src/format.rs
- /home/mimi/respan/respan-gateway/crates/core/src/call.rs
- /home/mimi/respan/respan-gateway/crates/core/src/deliver.rs
- /home/mimi/respan/respan-gateway/crates/core/src/input.rs
- /home/mimi/respan/respan-gateway/crates/http/src/lib.rs