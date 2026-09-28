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
| [0004](decisions/0004-two-edges-typed-formats.md) | Two edges; typed formats; providers are name, format, transport |
| [0005](decisions/0005-no-routes-global-plugins.md) | Formats recognise requests; one plugin order; default targets |
| [0006](decisions/0006-streams-both-ways.md) | Inputs and outputs may both stream |
| [0007](decisions/0007-format-is-a-codec.md) | A format is one protocol's codec |
| [0008](decisions/0008-events-carry-their-bytes.md) | Each message is one value that keeps its bytes; usage counts as decoded |
| [0009](decisions/0009-core-cleanups.md) | Only providers' responses count; one default target; less policy in the kernel |

## Two edges

```
 client ⇄ client edge ⇄ gateway ⇄ provider edge ⇄ provider
          on_client               on_provider
          filters                 filters
          on_client_end           on_provider_end
          once per request        once per call
```

`on_provider` runs before every call, so a check there covers them all;
it waits for no earlier call's end hooks, so a limit reserves there and
settles in `on_provider_end`. Each hook that started gets its end hook
once, a request's calls first.
Order is explicit (`after`); a plugin that answers skips those after it.
Only `on_client` may `call` or `take_inputs`
([0004](decisions/0004-two-edges-typed-formats.md#two-edges)).

## The request path

```
 http::Request ─▶ handle ─▶ first format that accepts (none: plain 404)
   Upgrade? no:  whole: body read in full, its one input
            yes: upgrading: no input with the head; one per data frame after
                  ▼
 on_client, each plugin ─Some─▶ own response ─────┐
                  │ all pass  └─Err─▶ refusal ────┤
 cx.call(target) ─▶ on_provider ─Err─▶ refusal ───┤
                  ▼                               │
 input filters ─▶ fill call ─▶ request encoder   │
   ─▶ edit_call ─▶ Transport::send(Request<Body>) │
                  ▼                               │
 response decoder ─▶ outputs ─▶ provider filters  │
                  ▼                               │
 client-edge filters ◀────────────────────────────┘
                  ▼
 response encoder: open ─▶ head with the first byte, or at
          once for upgrading, HEAD, 204, 205, 304
                  ▼ at the end
 on_provider_end, each call ─▶ on_client_end;  the Outcome future resolves
```

Four flows:

```
 chat POST    1 input, maybe empty ─▶ call(target) ─▶ SSE
 quiet GET    Response::stream; Encoder::open (sse::comment,
              whole::Relay::with_open) ─▶ head at once
 WS→WS relay  call(target): its head sets Upgrade ─▶ 101 at once;
              client frames ─▶ input filters ─▶ that call's encoder
 WS→SSE       on_client keeps call = cx.call(p), returns
 bridge       Response::stream, which the encoder makes a 101;
              take_inputs() ─▶ per message call.clone()
              .with_input(chat) ─▶ outputs() ─▶ one frame per output
```

## Formats and kinds

A format is one protocol's codec: a decoder and an encoder per body,
either way, each keeping its stream's state
([0007](decisions/0007-format-is-a-codec.md)). A body is a stream of
messages, each decoded into one value of the kind, even one the kind
cannot read; each value keeps the bytes it came in, so the format writes
it as them while it says the same, an edit written into them
([0008](decisions/0008-events-carry-their-bytes.md)). Formats of one name
relay; formats of one kind translate through their values; traffic the
gateway does not read is a kind of one (`formats::opaque`).

The kernel keeps no bytes. It relays only heads (a translated head keeps
its status and `Retry-After`) and fills a relay's call with the client's
headers and query as a proxy does. A whole body is decoded only when
read, so a plain relay's call carries the client's bytes, which its
request encoder may edit; an upgrading request's frames are decoded as
they pass. `Upgrade` (RFC 9110 7.8), not the format, says whether inputs
stream. The model belongs to the kind (`Conversation.model`); its place on
the wire is the format's, whose request encoder reads it from the first
input ([0003](decisions/0003-kernel-contracts.md#kinds)).

## Core types

14 public types, plus `Body`, `codes` and re-exports; `handle` needs a
tokio runtime, which runs end hooks detached.

- `Gateway`, `Transport`; `Plugin`, whose hooks get a `ClientContext` (a
  request) or `ProviderContext` (a call); `Call`, a `'static`, `Clone`,
  `Sync` future, sent when awaited.
- `Filter<T>` edits inputs (the only way) or outputs; `Outcome` says how
  an edge ended; `Response` is a call's, `cx.relay`'s or
  `Response::stream`'s, read with `ready` or `outputs`; `Fault` is the one
  error type.
- `Format`, `Decoder<T>`, `Encoder<T>`, `Usage`: a protocol's codec, bytes
  to values of its kind and back; amounts.

## The response pipeline

1. **Head.** Relayed, the provider's minus connection headers;
   translated, its status and `Retry-After`, plus what the client's
   encoder sets. A provider's unasked 101, or a redirect to translate,
   fails the call `UPSTREAM`.
2. **First byte.** A head waits for its body's first byte, so a success
   failing before any is refused; `Encoder::open` opens a quiet stream at
   once. An upgrading request's head goes at once, so the encoder makes a
   plugin's success a 101. A 204, 205, 304 or response to a `HEAD` goes at
   once with no body and nothing more is read: a call whose end was
   decoded, as a provider's bodiless response's is, ends so; one with a
   body left ends `abandoned`.
3. **Decode.** Each message is one value. Only a provider's response
   counts: its usage counts as decoded, on its call's provider edge,
   whoever reads it, and a request's `Outcome.usage` sums its calls'. A
   plugin's own bytes (`cx.relay`) or outputs count nothing; a cache
   carries a hit's stored usage in an extension for billing to read. A
   cost cap is a reservation in `on_provider`,
   settled from `Outcome.usage`, plus a provider-edge filter that
   estimates from text to cut a stream; a meter once saw each increment,
   but providers report output usage at a stream's end.
4. **Edit.** Output filters run provider edge first, the last added
   first; input filters the first added first: a plugin sees inputs after
   earlier plugins, outputs before. A value that says nothing, such as a
   keep-alive, passes no filter and is written where it came; `outputs`
   still yields it, for its reader to filter with the kind's own check. An `Err`
   stops writing, never counting, and a failed filter
   releases nothing.
5. **End.** A body ends at the end of its bytes; one cut short ends
   `Err(truncated)`. Every filter gets one end, even after a failure,
   drop or panic elsewhere, so what it keeps back is released. A panic is
   an internal fault, written in the client's format even after bytes are
   out.
6. **Close.** At close the end is known, so open filters cannot change
   it. A request closes when its last bytes are out or the client leaves;
   a call when it fails, its body ends, or its reader stops. The request
   ends its open calls `abandoned` first.

## Routing

A router sets the target and passes. A fallback answers with the first
ready success, else the last answer, so a client sees the last 429 and its
`Retry-After`. `ready` reads to the first content (a provider's output
that says something, as sent, before filters, or any output a plugin
made); it is `Err` for a success that fails first, and passes an error
status:

```rust
let mut last = Err(Fault::upstream("no provider to try"));
for name in &self.0 {
    last = match cx.call(name).await {
        Ok(resp) => resp.ready().await,
        Err(fault) => Err(fault),
    };
    if last.as_ref().is_ok_and(|r| r.head().status.is_success()) { break }
}
last.map(Some)
```

A judge's call brings its own input and is read:
`cx.call(p).with_input(prompt).await?.outputs::<Reply>()`. A change for
one call alone is made in `on_provider`, keyed on `px.provider()` or on a
mark the call carries (`with_extension`). The gateway has one default
target; routing by format or kind is a plugin that sets the target. Routing by
model reads the input, so upgrading requests route by their head, and
opaque traffic by its body
([0003](decisions/0003-kernel-contracts.md#kinds)).

## Crates

| Crate | What it is |
|---|---|
| `core` | The kernel: no HTTP, config or kinds. |
| `formats` | The `conversation` kind, `OpenAiChat`, `AnthropicMessages`, `relayed`, `opaque`, helpers. |
| `plugins` | `ModelRouter<I>`, given an accessor for the kind's model. |
| `server` | `router` (bodies up to 32 MiB, no upgrades), `HttpTransport` (strips the client's credentials when it has its own, cookies and forwarding headers), `CallHeaders`. |
| `testkit` | `MockTransport` and the kernel's tests. |
| `respan-gateway` | The binary: YAML config, routing by model, `check`. |
| `sdks/routing` | Retry, fallback, race and a stall watchdog for plugins. |
| `sdks/text` | Read and rewrite chat text across stream deltas; refuse what cannot be read. |
| `sdks/ledger` | Reserve before a call, settle once from its usage. |
| `sdks/mcp` | The MCP kind, streamable HTTP format and stdio transport. |
| `examples` | One runnable, tested example per use case ([list](../examples/README.md)). |

SDKs and examples use only the public API of the crates above.
