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

Every shape (unary, stream, job, session) is a sequence of **Turns** grouped
into an **Exchange**. A plugin is one trait whose method signatures say where
it runs: once per request, once per attempt, or as the route's source. Only the
source can reach a provider, and the Call it returns is the commit.

## Six concepts

| Concept | What it is |
|---|---|
| **Turn** | One request/response unit and the plugin's only handle. `Turn<Req>` for the request, `Turn<Attempt>` for one provider call. Carries the up/down streams, identity, labels, store access, subrequests, offload and `on_close`. Closes into exactly one Record (a tree of attempts and subrequests). The Format decides its Exchange: itself for HTTP, the socket for Realtime, a sealed id for jobs, files and MCP. |
| **Part** | What flows each way: Head, Doc (whole message, lazily parsed, lossless), Delta (Text, Args, Reasoning, Bytes), Usage, End. Streams offer `head()`, stateless `map()` and stateful `filter()` with holds. A direction with no map or filter is spliced untouched. |
| **Plugin** | One trait, four optional methods: `covers`, `request`, `attempt`, `serve`. Configured instance per chain entry and tenant version. |
| **Store** | Shared key-value store with atomic batches (Get, Put, Add with bounds/TTL/lease, Settle, Once, Del), change waits and purge by tenant and class. Keys are tenant, exchange or global scoped. No billing or queue types. |
| **Format** | Codec between wire frames and Parts: decode, encode, faults in the client's framing, turn boundaries, usage normalization, a content map for every string leaf, and id fields the kernel seals. Compiled in, or declared in YAML (`GenericJson`, JSON pointers only). |
| **Policy** | Read-only versioned config: static snapshot plus per-tenant overlays (entry config, BYOK targets, routes). |

## The plugin trait

```rust
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    fn covers(&self) -> Covers { Covers::NONE }
    async fn request(&self, t: &Turn<Req>) -> Result<(), Fault> { Ok(()) }        // once per Turn
    async fn attempt(&self, a: &Turn<Attempt>) -> Result<(), Fault> { Ok(()) }    // once per provider call
    async fn serve(&self, t: &Turn<Req>, up: &Upstream) -> Result<Call, Fault> { // route source; returned Call = commit
        Err(Fault::config("not a source"))
    }
}
```

A per-key rate limit, for scale:

```rust
impl Plugin for PerKeyRate {
    async fn request(&self, t: &Turn<Req>) -> Result<(), Fault> {
        let minute = now_secs() / 60;
        let key = t.key("limit", &format!("rpm:{}:{minute}", t.principal()?.id()));
        let op = Op::add(key, 1.0).max(self.per_minute).ttl(Duration::from_secs(120));
        if t.store().apply([op]).await?.is_applied() { Ok(()) } else { Err(Fault::too_many(60)) }
    }
}
```

## Invariants the kernel guarantees

- Every Turn closes into exactly one Record; `on_close` runs once, including
  on disconnect, cancel or panic.
- At most one committed Call per Turn. After commit no attempt can start, and
  later failures go out in the client's framing.
- Each attempt gets its own copy-on-write request. Crossing Formats means the
  target Format decodes and the client Format encodes.
- Subrequests are full Turns on another route, depth-limited, inheriting
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
- Let subrequests and timer Turns inherit their root Turn's admission, so a
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
  running on subrequests, polls and timer Turns.
- Forcing required entries to the end of the chain: auth can no longer be
  required, and hooks run before zero-retention forbids exist.
- Removing the kernel flush guarantee: a filter that returns early truncates
  output and reports a clean stop.
- Moving estimated usage to free-form labels: a cut stream can settle at $0.

## Detail backlog

- Per-attempt body edits for split sources.
- PII mappings must also be registered on cache hits.
- `forbid` should take a scope, so ZDR can allow in-process keys.
- Default `origins` skips webhook and timer Turns, so jobs settled that way
  are not billed.
- Durable file ids need non-expiring spill handles.
- Admission waits need an exemption from per-entry time budgets.
- Session rules for binding response ids to concurrent open Turns.
- Spliced passthrough still needs a cheap scan for usage and the terminal
  marker.
- Fault text must pass through required down filters.
- Embedded mode must say who drives timers and lease renewal.
