# Architecture

The kernel relays requests, translates them between formats of one kind,
runs every plugin in one order, and closes each request exactly once.
The tests state what it promises: in `crates/testkit/tests/`, `relay.rs`
covers the kernel and OpenAI relay, `translate.rs` OpenAI and Anthropic
both ways, and `custom.rs` a kind the kernel has never seen.
`crates/server/tests/http.rs` covers HTTP.

## Decisions

| # | Rule |
|---|---|
| [0001](decisions/0001-neutral-core.md) | The core is feature-neutral: every feature is a plugin on one public API |
| [0002](decisions/0002-no-dynamic-plugin-loading.md) | Plugins are compiled in; nothing is loaded at runtime |
| [0003](decisions/0003-kernel-contracts.md) | A minimal kernel, typed by kind, whose contracts grow without breaking; what is promised |
| [0004](decisions/0004-two-edges-neutral-events.md) | Two edges on a kind's input and output; a typed `Format`; a provider is a name, a format and a transport |
| [0005](decisions/0005-no-routes-global-plugins.md) | Formats recognise their requests; one plugin registry ordered by `after`; a default target |

## Two edges

A request goes to the first format served that accepts it. Its plugins
run on two edges, in the gateway's one order:

```
 client ⇄ client edge ⇄ gateway ⇄ provider edge ⇄ provider
          on_client               on_provider
          filters                 filters
          on_client_end           on_provider_end
          once per request        once per call
```

- `on_client` runs once per request. The first plugin to answer or
  refuse ends it; otherwise the target answers.
- `on_provider` runs before every call the request makes: the target's,
  a fallback's, a judge's.
- Each hook that started gets its end hook once. A request's calls
  finish theirs before its `on_client_end` starts.

Guardrails, PII and routing belong to the client edge; billing, limits
and signing to the provider edge.

## Formats and kinds

A format is one wire protocol. Its kind is its pair of types, `Input`
and `Output`, such as `Conversation` and `conversation::Reply`; a format
of no kind declares `()` for both. Formats of one name relay to each
other; formats of one kind translate through `decode` and `encode`.
`accepts` recognises a request from its head, `model` reads the model,
`prepare` builds a call's request, and `decoder` and `encoder` turn
response bytes into Parts and back.

A call's body is the client's, patched by `encode` if the input
changed, or the input encoded anew for another format of the kind
([0003](decisions/0003-kernel-contracts.md)). Within one format the
client's end-to-end headers pass on, credentials included; the transport
decides whose credentials reach the provider.

## Core types

22 public types, plus the `Body` alias, the `body` function, the `codes`
module and re-exports of the crates its contracts use.

- `Gateway`: providers, formats served, plugins and a default target,
  each setting checked as given; `handle`, `check_target`, `idle`.
- `Closed`: a future of a request's `Outcome`, from any response.
- `Provider`, `Target`, `Transport`: a name, a format and a transport;
  the name of the provider that answers; how one provider is reached.
- `Plugin`, `Context`, `ProviderContext`, `Call`: the four hooks; one
  request on the client edge; one call on the provider edge; a call a
  plugin makes with `cx.call`.
- `Filter<T>`, `Outcome`: edits output events as they stream; what an
  end hook learns (status, end, usage).
- `Format`, `Event`, `ClientView`, `Decoder`, `Encoder`: a wire protocol;
  the bound on a kind's types; the client's request as a format sees it;
  bytes to Parts and back.
- `Part`, `Item`, `Doc`, `Usage`: what a response decodes into (`Item`,
  `Frame`, `Usage`, `End`); one message's events and bytes; a whole body;
  an additive map of units to amounts.
- `Response`: a provider's, from a call, or a plugin's answer.
- `Fault`: the one error type.

## The response pipeline

1. **Head.** A relayed response keeps the provider's head, minus
   hop-by-hop headers, those its `Connection` names, and its origin's
   (`set-cookie`, `alt-svc`, `strict-transport-security`). Any other
   keeps only its status and `Retry-After`, and is refused as a 502 if
   it is a redirect with a `Location`. Trailers are not passed on.
2. **Hold.** The head waits for the first bytes. A success that fails
   before any, or a panic, is sent as a refusal. A 204, a 304 or an
   answer to `HEAD` is read to its end and sent with no body.
3. **Decode.** Every byte of content belongs to an item or a Frame.
4. **Edit.** Filters change, hold, drop and insert events: the provider
   edge's first, then the client edge's, each the last added first, as
   are head edits (`map_head`). An unedited item is written as its
   bytes.
5. **End.** Each body has one end. A stream cut short ends
   `Err(truncated)` in the client's framing.
6. **Close.** End hooks run once, detached from the response:
   `on_client_end` as the last bytes are handed out, or when the client
   leaves or a panic ends the response; a call's `on_provider_end` when
   it fails, or when its response is sent, collected or dropped. A
   call's response still open when its request closes ends abandoned.

## Routing

A router sets the target and passes, as `ModelRouter` does, so the
plugins after it still run. A fallback answers by calling providers:

```rust
let mut last = Err(Fault::upstream("no providers"));
for name in &self.0 {
    last = cx.call(*name).await;
    if last.as_ref().is_ok_and(|r| r.head().status.is_success()) {
        break;
    }
}
last.map(Some)
```

Answering skips the plugins after it, so such a plugin goes last. A
judge's call carries its own input and is read, not sent:
`cx.call(p).input(prompt).await?.collect::<Reply>().await?`. Every call
runs the provider edge, so billing and limits see each one.

## Crates

| Crate | What it is |
|---|---|
| `core` | The kernel: no HTTP client or server, no config, no kinds. |
| `formats` | The `conversation` kind with `OpenAiChat` and `AnthropicMessages`, and helpers for any format: `sse`, `whole`, `posts_to`, `pass_headers`, `json_model`, `write_model`, `frames_decoder` and `frames_encoder`. |
| `plugins` | First-party plugins: `ModelRouter`. |
| `server` | `router`, an axum router sending every path to the gateway, bodies bounded at 32 MiB; `HttpTransport`, a base URL and headers, with the `CallHeaders` extension. |
| `testkit` | `MockTransport`, `mock` replies, `post`, `collect`, `Outcomes`, and the kernel's tests. |
| `respan-gateway` | The binary: YAML config, formats by name, routing by model, and `check`. |
