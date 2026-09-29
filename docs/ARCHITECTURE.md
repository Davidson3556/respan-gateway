# Architecture

The kernel (`crates/core`) relays requests, translates them between
formats of one kind, runs every plugin in one order, and ends each request
and call once. Tests state its promises; the
[README](../README.md#glossary) defines its terms.

## Decisions

| # | Rule |
|---|---|
| [0001](decisions/0001-neutral-core.md) | Every feature is a plugin on one API |
| [0002](decisions/0002-no-dynamic-plugin-loading.md) | Plugins are compiled in |
| [0003](decisions/0003-kernel-contracts.md) | A minimal kernel typed by kind, grown without breaking |
| [0004](decisions/0004-two-edges-typed-formats.md) | Two edges; typed formats; a provider is a name, format and transport |
| [0005](decisions/0005-no-routes-global-plugins.md) | Formats recognise requests; one plugin order; one default target |
| [0006](decisions/0006-streams-both-ways.md) | Inputs and outputs may both stream |
| [0007](decisions/0007-format-is-a-codec.md) | A format is one protocol's codec |
| [0008](decisions/0008-events-carry-their-bytes.md) | Each message is one value that keeps its bytes |
| [0009](decisions/0009-core-cleanups.md) | Less policy in the kernel |
| [0010](decisions/0010-usage-out-of-core.md) | The kernel counts no usage; filters see every message |
| [0011](decisions/0011-read-until-content-or-end.md) | Read a response `until_content` or `until_end`, held unsent |
| [0012](decisions/0012-http-out-of-the-kernel.md) | HTTP's rules live in the adapter, transports and formats |
| [0013](decisions/0013-no-http-in-core.md) | No `http` types in the core: a wire is a pair of head types |
| [0014](decisions/0014-api-trims.md) | API trims: one way to say each thing |
| [0015](decisions/0015-core-trims.md) | Core trims: `relayed` and `streams` on `Exchange`, no `Outcome`, `is_head_only` |
| [0016](decisions/0016-core-fixes.md) | Core fixes: a cut request body never looks complete; open's bytes first; linear reads |

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
once, a request's calls first. Order is explicit (`after`); a plugin that
answers skips those after it. Only `on_client` may `call` or `take_inputs`.

## The request path

```
 adapter (crates/http): strip connection headers, lift the Credential,
   add Asked; Upgrade? streams; http::Request ─▶ Exchange<Ask>
                  ▼
 Gateway::handle(req.erase()) ─▶ first format that accepts
   (none: Err, the adapter's plain 404; the adapter downcasts the answer)
   whole: body read in full, its one input; streaming: one input per frame
                  ▼
 on_client, each plugin ─Some─▶ own response ─────┐
                  │ all pass  └─Err─▶ refusal ────┤
 cx.call(target) ─▶ on_provider ─Err─▶ refusal ───┤
                  ▼                               │
 input filters ─▶ request encoder writes the head │
   (a relay's from the client's) ─▶ edit_call ─▶  │
   Transport::send (strips what a proxy must)     │
                  ▼                               │
 response decoder (an error answer: a Fault) ─▶   │
   provider-edge filters                          │
                  ▼                               │
 client-edge filters ◀────────────────────────────┘
                  ▼
 response encoder (a refusal: its answer or its words): head,
   edit_response_head; the head goes with the first byte, or at once
   for a streaming request or a body done before it began
                  ▼ adapter: strip hop-by-hop headers
                  ▼ at the end
 on_provider_end, each call ─▶ on_client_end; the end future resolves
```

Four flows:

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
([0013](decisions/0013-no-http-in-core.md)). The kernel reads no head.
It hands a head, like bytes, only to a format of the name
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
status, retry and the answer, which the client's format writes as it came
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
  sent when awaited.
- `Filter<T>` edits inputs (the only way) or outputs; `Response` is a call's, `cx.relay`'s or
  `Response::stream`'s, held with `until_content` or `until_end`, or read
  with `outputs`; `Fault` is the one error type.
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
   earlier plugins, outputs before. Filters see every value, keep-alives
   and usage alone included, and pass on at once what they have no reason
   to hold. An `Err` stops writing; a failed filter releases nothing.
5. **End.** A body ends at the end of its bytes; one cut short ends
   `Err(truncated)`. Every filter gets one end, even after a failure,
   drop or panic elsewhere. A filter's panic is an internal fault, written
   in the client's format even after bytes are out.
6. **Close.** A request closes when its last bytes are out or the client
   leaves, ending its open calls `abandoned` first; a call when it fails,
   its body ends, or its reader stops. Open filters cannot change the end.
   A request counts as in flight for `Gateway::idle` from `handle`, while
   its body is read. Every user-code call, a request encoder's later
   frames included, is guarded: a panic ends its call `internal`.

## Routing

A router sets the target and passes. A fallback answers with the first
success read to its content, else the last failure, so a client sees the
last 429 as it came, or in its own words with its retry. `until_content`
reads until the first content (`Format::is_content`, before filters, so
neither a keep-alive nor usage alone; or any output a plugin made), a
failure or the end, and holds what it read, unsent; it is `Err` for a
response that fails first, a provider's error answer included:

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
| `http` | HTTP at the edges: `Ask`, `Answer`, the adapter (`serve`, `handle`), `Credential`, `Asked`, a transport's rules (`proxy_call`, `strip_client_origin_headers`, `credentials`, `CallHeaders`), conversions (`ask`, `answer`, `call`), and helpers for HTTP formats (`error_head`, `answered`, `bodiless`). |
| `formats` | The `conversation` kind, `Usage`, `OpenAiChat`, `AnthropicMessages`, `relayed`, `opaque`, `sse`, `whole`, helpers such as `with_head`. |
| `plugins` | `ModelRouter<I>`, given an accessor for the kind's model. |
| `server` | `router` (axum; bodies up to 32 MiB, no upgrades) and `HttpTransport` (its own keys, else the client's `Credential` on a relay). |
| `testkit` | `MockTransport` and the kernel's tests. |
| `respan-gateway` | The binary: YAML config, routing by model, `check`. |
| `sdks/routing` | Retry, fallback, race and a stall watchdog. |
| `sdks/text` | Read and rewrite chat text across stream deltas. |
| `sdks/ledger` | Reserve before a call, settle once from what your filter priced. |
| `sdks/mcp` | The MCP kind, streamable HTTP format and stdio transport. |
| `examples` | One runnable, tested example per use case ([list](../examples/README.md)). |

SDKs and examples use only the public API of the crates above.
