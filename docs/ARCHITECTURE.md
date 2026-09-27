# Architecture

The kernel relays, translates between formats of one kind, runs its
plugins in one order, and closes every request a served format accepts
exactly once. The tests
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
| 4 | Two edges, client and provider, on a kind's input and output; a typed `Format`; a provider is a name, a format and a transport | [0004](decisions/0004-two-edges-neutral-events.md) |
| 5 | No routes: formats recognise their requests; one global plugin registry, ordered by `after`; a default target | [0005](decisions/0005-no-routes-global-plugins.md) |

## The idea

A request goes to the first format served that accepts it, and has two
edges. On the client edge it runs through the gateway's plugins once, in
their order: the first plugin to answer or refuse ends it,
otherwise the target answers, and every plugin that ran gets one
`on_client_end`. On the provider edge every call the request makes, the
target's, a fallback's or a judge's, runs `on_provider` for each plugin
until one refuses, and `on_provider_end` once when the call fails or its
response ends, with that call's usage.

```
 client ⇄ client edge ⇄ gateway ⇄ provider edge ⇄ provider
          on_client               on_provider
          filters                 filters
          on_client_end           on_provider_end
          once per request        once per call, whoever makes it
```

```rust
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    async fn on_client(&self, _cx: &mut Context) -> Result<Option<Response>, Fault> {
        Ok(None)
    }
    async fn on_client_end(&self, _cx: &Context, _outcome: &Outcome) {}
    async fn on_provider(&self, _px: &mut ProviderContext) -> Result<(), Fault> {
        Ok(())
    }
    async fn on_provider_end(&self, _px: &ProviderContext, _outcome: &Outcome) {}
}
```

A plugin implements the edge it belongs to: guardrails, PII and routing
the client's; billing, limits and signing the provider's.

## Concepts

| Concept | What it is |
|---|---|
| **Gateway** | Providers, the formats served, plugins and a default target, fixed when built, by value, each setting checked as it is given (`Gateway::new(providers)?.serve(format)?.plugin(name, plugin)?.after(name, before)?.target(provider)?`); new config is a new `Gateway`, the old one finishing what it began. `handle(request)` returns a streaming response, whose `Closed` extension resolves to its `Outcome`. `check_target` checks a target a plugin may set; `idle` waits for requests in flight and end hooks. |
| **Registry** | Plugins by unique name, in one order on both edges: built one plugin at a time, the next being the earliest registered of those whose `after` plugins have all run; a plugin's filters wrap those of the plugins after it. Every plugin sees all traffic; one for a kind lets other traffic pass by matching `WRONG_KIND`, or refuses it with `?`. Per-endpoint differences are marks the adapter puts on the request. |
| **Context** | One request, on the client edge: its format's name, head with typed extensions (`head`, `head_mut`), body, input (`input`, `input_mut`; reading changes nothing), model, target, `call(provider)`, and response edits (`map_head`, `filter`). |
| **ProviderContext** | One call, on the provider edge, inside its request (`px.client()`): target, format name, model, and its own input, extensions and response filters, run before the client edge's. Its extensions reach the transport, such as a tenant's key. |
| **Provider** / **Target** | A provider is a name, a format and a transport (0004); a target names the provider that answers: the gateway's, unless a plugin set one. |
| **Format** | One wire protocol: its name, which decides relay (the same name) or translation, which client requests it accepts (`accepts`, from the head), where it keeps the model, `decode` and `encode` of its input, `prepare` (the call's HTTP request, relative to the provider's transport, given a `ClientView` of the client's request), and a decoder and encoder per response. Its kind is its pair of types, `Input` and `Output`, such as `Conversation` and `conversation::Reply`; `()` for both is no kind (0003). |
| **Response** | A provider's, from `cx.call`, sent or read with `collect::<Reply>()`; or a plugin's, as bytes in the client's format or as events (`Response::stream`). Dropping it cancels it. |
| **Doc** / **Part** / **Item** | A `Doc` is a whole body, JSON or not. A response decodes into Parts: `Item`s (typed events with their bytes, if any), `Frame`s, `Usage` and one end, `Ok` or `Err(fault)`. |
| **Outcome** / **Fault** | An `Outcome` is what an end hook learns (status, end, usage): on the client edge the committed answer's, with the status sent; on the provider edge the call's, with the provider's status. A `Fault` is the one error type, written to clients in their own format. |
| **Transport** / **Body** | How and where one provider is reached (a URL and credentials, a process, a socket), on the gateway's `http` types. It gets the path and query relative to itself, with the client's credentials if its format passed them on, and owns credential policy: the kernel knows none. `Body` is `http_body_util`'s unsync boxed body with `Fault` errors. |

## The response pipeline

1. **Head**: a relayed response keeps the provider's head, minus hop-by-hop
   headers, those its `Connection` names, and its origin's (cookies,
   `alt-svc`, HSTS). A translated one, or one to a call's own input,
   keeps its status and `Retry-After`, and refuses a redirect (301, 302,
   303, 307 or 308 with a `Location`), ending the call with that refusal;
   any other status passes. Formats read a non-2xx whole and end it
   `Err`. Trailers are not passed on.
2. **Hold**: the head waits for the first bytes, so a success that fails
   before any is sent as a refusal with the fault's status, and a panic
   before any is refused whatever the status. A 204, a 304 or an answer
   to `HEAD` is read to its end first and sent with no body.
3. **Decode**: every byte of content becomes an item's or a Frame's.
4. **Edit**: filters change, hold, drop and insert events, the provider
   edge's first, then the client edge's, each the last added first; an
   unedited item is written as its bytes.
5. **End**: one end per body; a stream cut short ends `Err(truncated)` in
   the client's framing.
6. **Close**: end hooks run once. `on_client_end` runs as the last bytes
   are handed out, or at a client drop or a panic, once every call's
   `on_provider_end` has run. A call's runs when it fails before a
   response, or its response is done: sent, as the client's ends;
   collected, as `collect` returns; dropped, abandoned. A response still
   open when its request closes is cut off and ends abandoned, its filters
   getting their `end`.

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

Answering skips the plugins after it, so such a plugin goes last. A call may carry its
own input and be read rather than sent, as a judge's is:
`cx.call(p).input(prompt).await?.collect::<Reply>().await?`. Every call,
the target's included, runs the provider edge, so billing and limits see
the judge's call and each attempt too.

## Crates

| Crate | What it is |
|---|---|
| `core` | The kernel: no HTTP client or server, no config, no kinds. |
| `formats` | The `conversation` kind with OpenAI Chat Completions and Anthropic Messages, and helpers for any format: `sse`, `whole`, header and model helpers, `posts_to` for `accepts`, frame codecs for a format of no kind. |
| `plugins` | First-party plugins: `ModelRouter`. |
| `server` | axum router sending every path to the gateway, and `HttpTransport` (a base URL and headers, checked when built; one shared connection pool unless given its own client). A transport with headers of its own, or a call's `CallHeaders` extension, removes the client's credentials first; one with none passes them through. |
| `testkit` | `MockTransport` (`at(base_url)` per provider, with headers as `HttpTransport`, `reply`, `sent`), `mock` replies, `post(path, body)`, `collect`, `Outcomes`, and the kernel's tests. |
| `respan-gateway` | The binary: YAML config (providers, formats served, a `plugins:` list with names and `after`, a default `target`), formats by name from a table of constructors, routing by model, `check`. |
