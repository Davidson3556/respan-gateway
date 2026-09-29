# Architecture

The kernel (`crates/core`) relays requests, translates them between
formats of one kind, runs every plugin in registration order, and ends each
request and call once. The [README](../README.md#glossary) defines its terms.
Tests state its promises. The kernel alone, on a toy format and its public
API, is tested in `crates/core/tests`: `cancellation` (a drop at every await),
`panic_matrix` and `panics` (a panic at every place user code runs), `upload`
(a cut request body), `until` (`until_content` and `until_end`), `lifecycle`
(what is in flight) and `stress`. The rest run through the real formats in
`crates/testkit/tests` (`relay`, `translate`, `streams`, `line`, `events`,
`codec`, `leaks`, `custom`); HTTP's rules are in `crates/http/tests/rules.rs`.

## Decisions

| # | Rule |
|---|---|
| [0001](decisions/0001-neutral-core.md) | Every feature is a plugin on one API |
| [0002](decisions/0002-no-dynamic-plugin-loading.md) | Plugins are compiled in |
| [0003](decisions/0003-kernel-contracts.md) | A minimal kernel typed by kind, grown without breaking |
| [0004](decisions/0004-two-edges-typed-formats.md) | Two edges; typed formats; a provider is a name, format and transport |
| [0005](decisions/0005-no-routes-global-plugins.md) | Formats recognise requests; plugins in registration order; one default target |
| [0006](decisions/0006-streams-both-ways.md) | Inputs and outputs may both stream |
| [0007](decisions/0007-format-is-a-codec.md) | A format is one protocol's codec |
| [0008](decisions/0008-events-carry-their-bytes.md) | Each message is one value that keeps its bytes |
| [0009](decisions/0009-core-cleanups.md) | Less policy in the kernel |
| [0010](decisions/0010-usage-out-of-core.md) | The kernel counts no usage; filters see every message |
| [0011](decisions/0011-read-until-content-or-end.md) | Read a response `until_content` or `until_end`, held unsent |
| [0012](decisions/0012-http-out-of-the-kernel.md) | HTTP's rules live in the adapter, transports and formats |
| [0013](decisions/0013-no-http-in-core.md) | No `http` types in the core: a wire is a pair of head types |
| [0014](decisions/0014-api-trims.md) | API trims: one way to say each thing |
| [0015](decisions/0015-core-trims.md) | Core trims: `relayed` and `streams` on `Exchange`; an end hook takes the end itself; `is_head_only` |
| [0016](decisions/0016-core-fixes.md) | Core fixes: a cut request body never looks complete; open's bytes first; linear reads |
| [0017](decisions/0017-untyped-handle.md) | `Gateway::handle` is untyped: the adapter erases and downcasts its wire's heads |
| [0018](decisions/0018-plugin-order-is-registration-order.md) | Plugin order is registration order; no ordering solver in the kernel |
| [0019](decisions/0019-status-out-of-the-kernel.md) | HTTP status numbers out of the kernel: a typed hint on `Fault`, read by `crates/http` |

## Two edges

```
 client ⇄ client edge ⇄ gateway ⇄ provider edge ⇄ provider
          on_client               on_provider
          filters                 filters
          on_client_end           on_provider_end
          once per request        once per call
```

`on_provider` runs before every call, so a check there covers them all.
It waits for no earlier call's end hooks, so a limit reserves there and
settles in `on_provider_end`. Each hook that started gets its end hook
once, a request's calls first. Order is registration order on both edges
([0018](decisions/0018-plugin-order-is-registration-order.md)); a plugin that
answers skips those registered after it. Only `on_client` may `call` or `take_inputs`.

## The request path

1. The adapter (`crates/http`) strips connection headers, lifts the
   `Credential`, adds `Asked`, sets `streams` for an `Upgrade`, and calls
   `Gateway::handle(Exchange<Head>)` with the head erased. `handle` gives the
   request to the first served format that `accepts` its head; none is
   `Err(not_found)` for the adapter to write plainly.
2. The body: whole, it is read in full and is the one input, decoded by
   `request_decoder` when first read; streaming, each frame is one input as it
   arrives. A body that breaks is refused. The result is a `ClientContext`.
3. Each plugin's `on_client`, in registration order: pass, answer (the rest
   are skipped) or refuse. A plugin may add filters, `set_target`, edit the
   response head, `call` and `take_inputs`.
4. If all pass, `cx.call(target)`: each plugin's `on_provider` (an `Err` fails
   the call), then `send_call`. A relay's call starts from the client's head
   and unread bytes; a translated one is written from the inputs' values.
   `request_encoder` writes the head (`head`), then `open` and the inputs
   (`encode`); the plugins' `edit_call` follow; `Transport::send` returns the
   provider's `Exchange<Answer>`. A streaming request's later frames pass
   input filters and the encoder as the transport reads the body.
5. `response_decoder` turns the answer's bytes into values (an error answer
   ends `Err`), and the call's provider-edge filters edit each. A plugin now
   holds it (`until_content`, `until_end`), reads it (`outputs`), or sends it.
6. `deliver`: `response_encoder` writes the head (`head`, then
   `edit_response_head`), `open`, and each value that the client-edge filters
   left (`encode`); the head goes with the first byte. At the end, `end`.
7. The end hooks: open calls end `abandoned`, each call's `on_provider_end`
   runs, then `on_client_end`; `handle`'s end future resolves.

```text
trait Format { type Ask; type Answer; type Input; type Output;   // Ask/Answer: the wire's heads
  fn name(&self) -> &str;   fn accepts(&self, head: &Ask) -> bool;
  fn request_decoder(&self, head: &Ask) -> Box<dyn Decoder<Input>>;
  fn request_encoder(&self, client: Option<&Ask>, body: &mut Bytes, streams: bool)
      -> Box<dyn Encoder<Input, Ask>>;
  fn response_decoder(&self, answer: &Answer, call: &Ask, sent: &Bytes)
      -> Box<dyn Decoder<Output>>;
  fn response_encoder(&self, answer: Option<&Answer>, ask: &Ask, body: &Bytes,
      refusal: Option<&Fault>) -> Box<dyn Encoder<Output, Answer>>;
  fn is_content(&self, output: &Output) -> bool { true } }
trait Decoder<T> { fn decode(&mut self, bytes: Bytes, out: &mut Vec<T>) -> Result<(), Fault>;
                   fn end(&mut self, out: &mut Vec<T>) -> Result<(), Fault>; }
trait Encoder<T, H> { fn head(&mut self, first: Option<&T>) -> H;
  fn open(&mut self, out: &mut BytesMut) -> Result<bool, Fault>;   // Ok(true): body done
  fn encode(&mut self, value: &T, out: &mut BytesMut) -> Result<(), Fault>;
  fn end(&mut self, end: &Result<(), Fault>, out: &mut BytesMut) -> Result<(), Fault>; }
trait Transport { type Ask; type Answer;
  async fn send(&self, call: Exchange<Ask>) -> Result<Exchange<Answer>, Fault>; }
trait Plugin { async fn on_client(&self, cx: &mut ClientContext) -> Result<Option<Response>, Fault>;
  async fn on_client_end(&self, cx: &ClientContext, end: &Result<(), Fault>);
  async fn on_provider(&self, px: &mut ProviderContext) -> Result<(), Fault>;
  async fn on_provider_end(&self, px: &ProviderContext, end: &Result<(), Fault>); }
trait Filter<T> { fn edit(&mut self, value: T, out: &mut Vec<T>) -> Result<(), Fault>;
                  fn end(&mut self, end: &Result<(), Fault>, out: &mut Vec<T>) -> Result<(), Fault>; }
```

- Two edges: `on_client` once per request, `on_provider` once per call.
- Plugins see values of a kind, not bytes; only a format reads bytes.
- `until_content` is the commit point: before it a failure is still a refusal
  or a fallback; after it the head and bytes are out.
- A fault says whose fault it is: `bad_request` the client's, `config` the
  gateway's, `upstream` and `truncated` a provider's, `internal` a bug,
  `abandoned` that no one waits.

Flows:

```
 chat POST    1 input, maybe empty ─▶ call(target) ─▶ SSE
 quiet GET    Response::stream; Encoder::open (sse::comment,
              whole::Relay::with_open) ─▶ head at once
 WS→WS relay  call(target), its encoder told it streams ─▶ 101 at once;
              client frames ─▶ input filters ─▶ that call's encoder
 WS→SSE       on_client keeps call = cx.call(p), returns Response::stream;
 bridge       take_inputs() ─▶ per message call.clone().with_input(chat)
              ─▶ outputs() ─▶ one frame per output
```

## Formats, kinds and wires

A format is one protocol's codec: a decoder and an encoder per body,
either way, each keeping its stream's state
([0007](decisions/0007-format-is-a-codec.md)). A body is a stream of
messages, each decoded into one value of the kind, even one the kind
cannot read. Each value keeps the bytes it came in, so the format writes
it as them while it says the same, an edit written into them
([0008](decisions/0008-events-carry-their-bytes.md)). Formats of one name
relay; formats of one kind translate through their values; traffic the
gateway does not read is a kind of one (`formats::opaque`).

A wire is a pair of head types, `Format::Ask` and `Format::Answer`, that a
format and its transport share; HTTP's live in `crates/http`
([0013](decisions/0013-no-http-in-core.md)). The kernel reads no head and
keeps no value's bytes; it keeps only the client's request body, unread
(`ClientContext::body()`, public), for a relay to send as it came. It hands
a head, like bytes, only to a format of the name
it came in, and only for a relay: a relay's call starts from the client's
head and a relayed response's head is the provider's; a translated one
starts anew. `Gateway::handle` is untyped: the caller knows its wire,
erases its request's head and downcasts the response's
([0017](decisions/0017-untyped-handle.md)); `Gateway::provider` checks a
transport's heads against its format's at compile time. The encoder writes the head (`Encoder::head`) once, before
any bytes.

A call's request body ends `Err`, never clean, if the client's body broke,
an input could not be written, or the call or request ended first, and its
encoder's `end` is told that failure, not `Ok`
([0016](decisions/0016-core-fixes.md)). `Encoder::open`'s bytes come first
on either side, and `Ok(true)` means nothing more follows.

A whole body is decoded only when read, so a plain relay's call carries
the client's bytes, which its request encoder may edit; a streaming
request's frames are decoded as they pass. The adapter says whether
inputs stream (`Exchange::streams`), and the kernel says it of a call. A
provider's error answer is a `Fault` carrying its
status (an `http::Status` hint), retry and the answer, which the client's format writes as it came
if it read it, else in its own words. The model belongs to the kind
(`Conversation.model`); its place on the wire is the format's.

## Core types

14 public types, plus `Body`, `Head` (an alias), `codes` and re-exports; `handle` needs a
tokio runtime, which runs end hooks detached. The core depends on no
`http` crate.

- `Gateway`; `Transport`; `Exchange<H>`, a head, a body, `Extensions` and
  two flags (`streams`: later frames follow; `relayed`, kernel-set on a
  call that relays the client's message), which `handle` and
  `Transport::send` take and give.
- `Plugin`, whose hooks get a `ClientContext` (a request) or a
  `ProviderContext` (a call), and an end, `&Result<(), Fault>`; a fact
  about a call, such as `is_sent` or `is_head_only`, is a method of its
  context; `Call`, a `'static`, `Clone`, `Sync` future,
  sent when awaited. A call is a slot its request can empty (`Sending`,
  `OpenCalls` in `call.rs`), not a future driven in place: a call polled once
  and then parked, never polled again, must still end `abandoned` when its
  request ends, so the request needs a synchronous handle to drop the future.
  Driving `Call::poll` with `take_until` cannot; it was tried and reverted,
  and `a_pending_call_left_unpolled_still_ends_with_its_request`
  (`crates/testkit/tests/streams.rs`) pins it.
- `Filter<T>` edits inputs (the only way) or outputs; `Response` is a call's, `cx.relay`'s or
  `Response::stream`'s, held with `until_content` or `until_end`, or read
  with `outputs`; `Fault` is the one error type: a code and message, retry, and typed hints (`with`, `get`) that
  an edge reads; the kernel knows no HTTP status, `crates/http` maps code and `Status` hint to one
  ([0019](decisions/0019-status-out-of-the-kernel.md)).
- `Format`, `Decoder<T>`, `Encoder<T, H>`: a protocol's codec.

## The response pipeline

1. **Head.** Relayed, the provider's; translated, empty. The client's
   encoder writes it, and a refusal's status and retry. A provider's
   error answer fails the call, as its format decodes it.
2. **First byte.** A head waits for its body's first byte, so a response
   failing before any is refused; `Encoder::open` opens a quiet stream at
   once. A streaming request's head goes at once. A body done before it
   began (`Encoder::open` is `Ok(true)`, as HTTP formats say of a `HEAD`,
   204, 205 or 304) goes at once and nothing more is read: its call ends
   `Ok` if its decoder says the body is complete, else `abandoned`.
3. **Decode.** Each message is one value, run through its call's
   provider-edge filters as decoded, whoever reads it. The kernel counts
   no usage ([0010](decisions/0010-usage-out-of-core.md)): a plugin
   meters a call with a provider-edge filter of its kind and reads the sum
   in `on_provider_end`, registered after the plugins whose filters may
   fail. A plugin's own bytes (`cx.relay`) or outputs reach no provider
   edge.
4. **Edit.** Output filters run provider edge first, the last added
   first; input filters the first added first: a plugin sees inputs after
   earlier plugins, outputs before. Filters see every value, one that
   says nothing yet, such as a keep-alive, included, and pass on at once
   what they have no reason to hold. An `Err` stops writing; a failed
   filter releases nothing (`plugin.rs` unit tests, `line.rs`).
5. **End.** A body ends at the end of its bytes; one cut short ends
   `Err(truncated)`. Every filter gets one end, even after a failure,
   drop or panic elsewhere. A filter's panic is an internal fault, written
   in the client's format even after bytes are out (`panic_matrix`,
   `panics`).
6. **Close.** A request closes when its last bytes are out or the client
   leaves, ending its open calls `abandoned` first; a call when it fails,
   its body ends, or its reader stops. Open filters cannot change the end.
   A request counts as in flight for `Gateway::idle` from `handle`, while
   its body is read. Every user-code call, a request encoder's later
   frames included, is guarded: a panic ends its call `internal`. Dropping
   a request or response at any await still ends everything once
   (`cancellation`, `stress`, `leaks`).

## Routing

A router sets the target and passes. A fallback answers with the first
success read to its content, else the last failure, so a client sees the
last 429 as it came, or in its own words with its retry. `until_content`
reads until the first content (`Format::is_content`, before filters, so
not a value that says nothing yet, such as a keep-alive; any output a
plugin made is content), a failure or the end, and holds what it read,
unsent; it is `Err` for a response that fails first, a provider's error
answer included (`crates/core/tests/until.rs`):

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

A response with no content is read, and held, to its end. `until_end` reads
to the end and holds it all, returned with how it
ended; sent, it goes out as it would have unread. Either way the
call ends when the batch holding its end is sent, read or dropped
([0011](decisions/0011-read-until-content-or-end.md)). `sdks/routing`
takes the stopping point as `Until { Head, Content, End }`.

A judge's call brings its own input and is read:
`cx.call(p).with_input(prompt).await?.outputs::<Reply>()`. A change for
one call alone is made in `on_provider`, keyed on `px.provider()` or on a
mark the call carries (`with_extension`). Routing by format or kind is a
plugin that sets the target. Routing by model reads the input, so
streaming requests route by the adapter's view of their head
(`http::Asked`), and opaque traffic by its body.

## Crates

| Crate | What it is |
|---|---|
| `core` | The kernel: no HTTP, config or kinds; heads of any wire, unread. |
| `http` | HTTP at the edges: `Ask`, `Answer`, the adapter (`serve`, `handle`), `Credential`, `Asked`, a transport's rules (`proxy_call`, `strip_client_origin_headers`, `credentials`, `CallHeaders`), conversions (`ask`, `answer`, `call`), and `Status` and `status` (a fault's HTTP status), and helpers for HTTP formats (`error_head`, `answered`, `bodiless`). |
| `formats` | The `conversation` kind, `Usage`, `OpenAiChat`, `AnthropicMessages`, `relayed`, `opaque`, `sse`, `whole`, helpers such as `with_head`. |
| `plugins` | `ModelRouter<I>`, given an accessor for the kind's model. |
| `server` | `router` (axum; bodies up to 32 MiB, no upgrades) and `HttpTransport` (its own keys, else the client's `Credential` on a relay). |
| `testkit` | `MockTransport` and the kernel's tests. |
| `respan-gateway` | The binary: YAML config, routing by model, `check`. |
| `sdks/routing` | Retry, fallback, race and a stall watchdog. |
| `sdks/text` | Read and rewrite chat text across stream deltas. |
| `sdks/ledger` | Reserve before a call, settle once from what your filter priced. |
| `sdks/mcp` | The MCP kind, streamable HTTP format and stdio transport. |
| `sdks/ws` | WebSocket sessions: `WsTransport`, an axum adapter (`serve_ws`), `JsonEvents`. |
| `examples` | One runnable, tested example per use case ([list](../examples/README.md)). |

SDKs and examples use only the public API of the crates above.
