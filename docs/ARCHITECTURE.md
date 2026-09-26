# Architecture

The kernel relays, translates between formats of one kind, runs a plugin
chain, and closes every request that reaches a route exactly once. The tests
in `crates/testkit/tests/` state what it promises: `relay.rs` (the kernel
and OpenAI relay), `translate.rs` (OpenAI and Anthropic both ways) and
`custom.rs` (a kind the kernel has never seen); `crates/server/tests/`
covers HTTP.

## Settled

| # | Rule | Record |
|---|---|---|
| 1 | The core is feature-neutral; every feature is a plugin on one public API | [0001](decisions/0001-neutral-core.md) |
| 2 | No dynamic plugin loading; plugins are compiled in | [0002](decisions/0002-no-dynamic-plugin-loading.md) |
| 3 | A minimal kernel, typed by kind, whose contracts grow without breaking; what is promised to come | [0003](decisions/0003-kernel-contracts.md) |

## The idea

A request runs through its route's plugin chain: the first plugin to answer
or refuse ends it, otherwise the target answers, and every plugin that ran
gets one `on_close`.

```rust
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    async fn on_request(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
        Ok(None)
    }
    async fn on_close(&self, _cx: &Context, _outcome: &Outcome) {}
}
```

## Concepts

| Concept | What it is |
|---|---|
| **Gateway** | Transport, providers and routes, fixed when built (`Gateway::new`); new config is a new `Gateway`, the old one finishing what it began. `handle(route, request)` returns a streaming response, whose `Closed` extension resolves to its `Outcome`. `check_target` checks a target a plugin may set; `idle` waits for close hooks. |
| **Context** | One request: route, head, body, typed request, model, target, typed extensions, `call(provider)`, and response edits (`map_head`, `filter`). |
| **Route** / **Provider** / **Target** | A route is a format, a plugin chain and a default target; a provider a named upstream (format, base URL, headers); a target names the provider that answers. |
| **Format** | One wire protocol: its name, which decides relay (the same name) or translation, where it keeps the model, `prepare`, a decoder and encoder per response, and its `Kind`. |
| **Kind** | The request and event types a family of formats shares, such as `Conversation` and `conversation::Event`, defined in any crate: `Kind::new::<Request, Event>(decode, encode)`. |
| **Response** | A provider's, from `cx.call`, sent or read with `collect::<Event>()`; or a plugin's, as bytes in the client's format or as events (`Response::stream`). Dropping it cancels it. |
| **Doc** / **Part** / **Item** | A `Doc` is a whole body, JSON or not. A response decodes into Parts: `Item`s (typed events with their bytes, if any), `Frame`s, `Usage` and one end, `Ok` or `Err(fault)`. |
| **Outcome** / **Fault** | What `on_close` learns (status, end, usage); the one error type, written to clients in their own format. |
| **Transport** / **Body** | How providers are reached, with the same `http` types the gateway takes from clients; `Body` is `http_body_util`'s unsync boxed body with `Fault` errors. |

## The response pipeline

1. **Head**: a relayed response keeps the provider's head, minus hop-by-hop
   headers and its origin's (cookies, `alt-svc`, HSTS). A translated one,
   or one to a plugin's own request, keeps its status and `Retry-After`,
   and refuses a redirect. Formats read a non-2xx whole and end it `Err`.
   Trailers are not passed on.
2. **Hold**: the head waits for the first bytes, so a success that fails
   before any is sent as a refusal with the fault's status.
3. **Decode**: every byte of content becomes an item's or a Frame's.
4. **Edit**: filters change, hold, drop and insert events; an unedited item
   is written as its bytes.
5. **End**: one end per body; a stream cut short ends `Err(truncated)` in
   the client's framing.
6. **Close**: close hooks run once, after the last bytes, a client drop or
   a panic.

## Routing

A plugin routes by **setting the target** and passing, as `ModelRouter` and
YAML `models:` do, so the plugins after it still run. Or it **answers by
calling** providers itself, as a fallback does:

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

Answering ends the chain, so such a plugin goes last. A call may carry its
own request and be read rather than sent, as a judge's is:
`cx.call(p).request(prompt).await?.collect::<Event>().await?`.

## Crates

| Crate | What it is |
|---|---|
| `core` | The kernel: no HTTP client or server, no config, no kinds. |
| `formats` | The `conversation` kind with OpenAI Chat Completions and Anthropic Messages, and helpers for any format: `sse` (`or_whole`, `at_eof`, `is_stream`, `event`, `event_as`), `whole` (`decoder`, `Relay`), `pass_headers`, `pass_credentials`, `json`, `json_model`, `write_model`, `default_content_type`, and `frames_decoder`/`frames_encoder` for a format of no kind. |
| `plugins` | First-party plugins: `ModelRouter`. |
| `server` | axum router and `HttpTransport`. |
| `testkit` | `MockTransport`, `mock` replies, `post`, `collect`, `Outcomes`, and the kernel's tests. |
| `respan-gateway` | The binary: YAML config, routing by model, `check`. |
