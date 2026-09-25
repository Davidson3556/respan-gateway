# Architecture

**Status: proposed.** Chosen from four design rounds (about 70 agents).
Every [use case](use-cases.md) is expressible with no architectural gap. What
remains is detail work, listed at the end.

## Settled

| # | Rule | Record |
|---|---|---|
| 1 | The core is feature-neutral; every feature is a plugin on one public API | [0001](decisions/0001-neutral-core.md) |
| 2 | No dynamic plugin loading; plugins are compiled into the binary | [0002](decisions/0002-no-dynamic-plugin-loading.md) |

## The idea

A client request runs through a chain of plugins. Each one passes, refuses,
or responds; any plugin can call providers to build its response. If every
plugin passes, the route's target answers. A response nobody edits is relayed
byte for byte.

## Concepts

| Concept | What it is |
|---|---|
| **Context** | Everything about one client request that is not the message: id, route, session, store, typed extensions (`ext::<T>()`, `insert_ext`), and `call(provider)`. Shared by every hook, client side and provider side. |
| **Request** | A message exchanged with a peer: the client's request, or one of the gateway's requests to a provider. Same shape on both sides: peer, format, envelope (model, stream), body, and two streams to edit, `request()` on its way to whoever answers and `response()` on its way back. |
| **Response** | What came back: a provider's response (`cx.call`) or a synthetic one. Live while it streams; responding with it commits it, dropping it cancels it. Close hooks see it finished: status, head, `end()`, `usage()`. |
| **Flow** | What a client-request hook decides: `Pass` or `Respond(Response)`. |
| **Part** | What flows through a stream: Head, Doc, Delta (Text, Args, Reasoning, Bytes), Frame, Usage, End. Edit with `head()`, `map()`, `filter()`; observe with `tap()`, which never forces a re-encode. |
| **Store** | Shared key-value store with atomic batches. Keys are typed by a `Class`. |
| **Format** | A wire protocol, used on both sides: it parses and renders client traffic, and forges and parses provider traffic. Stateless; a decoder and encoder per stream. |
| **Target** | A configured provider: the Format it speaks, base URL, auth, model override. Config, not code. |

## The plugin trait

```rust
pub enum Flow { Pass, Respond(Box<Response>) }

#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    async fn on_client_request(&self, cx: &Context, req: &Request) -> Result<Flow, Fault> { Ok(Flow::Pass) }
    async fn on_provider_request(&self, cx: &Context, req: &Request) -> Result<(), Fault> { Ok(()) }
    async fn on_provider_close(&self, cx: &Context, req: &Request, resp: Result<&Response, &Fault>) {}
    async fn on_client_close(&self, cx: &Context, req: &Request, resp: Result<&Response, &Fault>) {}
}
```

- `Err` from `on_client_request` refuses the request, in the client's format.
  A plugin that responds (router, fallback, cache) goes after the plugins
  that must see the request (auth, limits, PII). If every plugin passes, the
  route's target answers.
- Provider requests run `on_provider_request` for the chain entries that
  have run so far.
- Close hooks run in one async, tracked close phase when the request ends:
  every provider request that ended, in order, then the client request. A
  plugin is closed only for requests its request hook saw. `Ok(resp)` means a
  response existed (`resp.end()` says how it ended: Done, Failed, Cancelled,
  Abandoned); `Err(fault)` means there never was one.
- Plugins share context through typed extensions on the Context. The kernel
  defines only the shared vocabulary: `Principal`, `Credential`, `HttpHead`.

## Invariants the kernel guarantees

- Close hooks run exactly once, including on disconnect: each provider
  request's first, then the client request's. There is no built-in Record;
  the SDK `Recorder` assembles one from the close hooks.
- At most one committed response per client request. After commit no attempt can start, and
  later failures go out in the client's framing.
- Each provider request gets its own copy of the body. Crossing Formats means the
  target Format decodes and the client Format encodes.
- Subrequests are full client requests on another route, depth-limited, inheriting
  identity, forbids, trace and deadline.
- Required chain entries run first and their filters sit at the edge, so no
  other plugin's output bypasses them. A required entry that cannot cover a
  request makes the kernel refuse it.
- Unknown string leaves count as content, so new provider fields are guarded
  by default.
- A stream missing its terminal marker ends as `Failed{truncated}`, never as a
  clean stop.

## SDK (optional crate, public kernel API only)

Fallback, hedge, split/merge and translation sources; text rewriting with
held tails (PII, translation); memo cache; wallets, reserve/settle and job
holds; fixed-window limits; admission queue with priority and fairness; stall
watchdog; file inlining.

## Scores (review panel, 1-10)

| | Score |
|---|---|
| Use cases with no architectural gap | 14 of 14 |
| Simplicity | 7 (Pingora 7, tower 6, Envoy 6, Nginx 3 on the same rubric) |
| Elegance | 6-7 |
| Target | 9; accepted at 7 for now |

## Known weaknesses

- The six concepts are chapter headings. A newcomer meets about 14 things
  inside them.
- Store classes and forbids are untyped strings; a typo fails closed silently.
- Required entries follow a different ordering rule from the YAML order.
- A few feature-named kernel terms remain (`FileRef`, estimated usage).

## Path to 9

Reviewers agree on what keeps simplicity at 7, and what would lift it without
adding concepts:

- Done: stream directions are `request()` and `response()` on both the
  client and provider Request, so the nesting reads the same on both sides.
- Add a read-only `tap` verb next to `map` and `filter`, so observers never
  force a re-encode.
- Let subrequests and timer requests inherit their root request's admission, so a
  naive rate limiter is correct without knowing about `origin`.
- Replace the ZDR exemptions with one ordinary scope value (`Durable` vs
  `Process`).
- Declare required-entry placement in YAML (`at: head | tail`) instead of
  deriving it.
- Write the 10-minute examples with SDK helpers.

## Cuts that were tried and failed

A focused round cut the design to five concepts. Simplicity stayed at 7,
elegance fell to 6, and it caused 10-14 regressions. Do not repeat these:

- Dropping the `origins` default: rate limiters, admission and auth start
  running on subrequests, polls and timer requests.
- Forcing required entries to the end of the chain: auth can no longer be
  required, and hooks run before zero-retention forbids exist.
- Removing the kernel flush guarantee: a filter that returns early truncates
  output and reports a clean stop.
- Moving estimated usage to free-form labels: a cut stream can settle at $0.

## Detail backlog

- Per-attempt body edits for split sources.
- PII mappings must also be registered on cache hits.
- `forbid` should take a scope, so ZDR can allow in-process keys.
- Default `origins` skips webhook and timer requests, so jobs settled that way
  are not billed.
- Durable file ids need non-expiring spill handles.
- Admission waits need an exemption from per-entry time budgets.
- Session rules for binding response ids to concurrent open requests.
- Spliced passthrough still needs a cheap scan for usage and the terminal
  marker.
- Fault text must pass through required down filters.
- Embedded mode must say who drives timers and lease renewal.
