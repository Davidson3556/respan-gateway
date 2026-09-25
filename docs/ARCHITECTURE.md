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

A client request runs through a chain of plugins. The route's source decides
which provider requests to make; the response it returns is committed to the
client. Plugins are one trait whose methods say where they run. A response
nobody edits is relayed byte for byte.

## Concepts

| Concept | What it is |
|---|---|
| **ClientRequest** | Client to gateway. The plugin's main handle: head, body, principal, session, store, labels, `from_client()` and `to_client()` streams, `on_close`. Closes into exactly one Record. |
| **ProviderRequest** | Gateway to one provider. A client request can make several (fallback, retry, hedging, split). Has its own copy of the body and `to_provider()` / `from_provider()` streams. |
| **ClientResponse** / **ProviderResponse** | What goes back to the client, and what a provider sent. Turning a ProviderResponse into a ClientResponse commits it. |
| **Providers** | The capability to make provider requests. Only the route's source gets it. |
| **Session** | A group of client requests that share state and a provider: a socket, a job, an MCP session. A plain HTTP request is a session of one. |
| **Part** | What flows through a stream: Head, Doc (a whole message), Delta (Text, Args, Reasoning, Bytes), Frame, Usage, End. Edit with `head()`, `map()` and `filter()`. |
| **Store** | Shared key-value store with atomic batches. Keys are typed by a `Class`. |
| **Format** | A wire protocol. Stateless; hands out a decoder and encoder per stream. Compiled in, or declared in YAML. |
| **Config** | Routes, chains, providers. Anything it names that the binary does not contain is a startup error. |

## The plugin trait

```rust
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    async fn on_client_request(&self, req: &ClientRequest) -> Result<(), Fault> { Ok(()) }
    async fn on_provider_request(&self, req: &ProviderRequest<'_>) -> Result<(), Fault> { Ok(()) }
    async fn dispatch(&self, req: &ClientRequest, providers: &Providers<'_>)
        -> Result<ClientResponse, Fault> { Err(Fault::config("not a source")) }
}
```

Stream directions are named after who is on the other end:

```
client ──from_client()──► gateway ──to_provider()──► provider
client ◄──to_client()──── gateway ◄─from_provider()── provider
```

## Invariants the kernel guarantees

- Every client request closes into exactly one Record; `on_close` runs once,
  including on disconnect.
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

- Name stream directions absolutely (`to_client`, `from_provider`) so the
  request stage wrapping the attempt stage is visible in the code.
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
