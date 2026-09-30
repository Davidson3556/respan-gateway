# Use cases

Only cases that are weird, hard and practical. Each one states requirements
for a gateway built with this library. These are integration requirements,
not a list of features that belong in core or a claim that every example
implements every requirement. See [examples](../examples/README.md) for
implemented cases and remaining gaps.

Core supplies typed event streams, per-edge contexts, plugin hooks, calls,
response buffering, and lifecycle completion. Adapters own transport details;
SDKs and plugins supply routing, storage, accounting, and other policies.
Ordinary features (routing, rate limits, logging) are not listed separately;
the design must support them.

## 1. Convert attachments before the model sees them

A client sends a PDF as a file part through `/v1/chat/completions`. The agent
behind it only takes text.

- Per attempt, not per request: a fallback target that reads PDFs natively
  should get the original file.
- Agents resend the whole history every turn, so the same PDF comes back on
  turn 30. Extraction must be cached by content hash across requests.
- Parsing is CPU-heavy or a slow OCR call, on untrusted 20-50 MB input.
- The PDF may be a `file_id` uploaded earlier.

**Integration requires:** offload for blocking work, a memo cache on the shared
store, per-hook time budgets, per-route body limits, resolving stored file
references, a transform marker in completion facts.

## 2. Restore redacted PII in a streamed response

PII is replaced with placeholders before the model and restored on the way
out. The model streams `<PER` `SON_` `1>` across three SSE deltas, sometimes
inside tool-call arguments.
([LiteLLM #42476](https://github.com/BerriAI/litellm/issues/42476),
[#31950](https://github.com/BerriAI/litellm/issues/31950))

**Integration requires:** per-stream plugin state, holding events and releasing
them several frames later, tool-call argument deltas exposed as editable text.

## 3. A required safety plugin silently skipped

A guardrail runs on `/chat/completions` but not on the native passthrough or
on `/responses`. This is the source of LiteLLM's worst guardrail and PII bugs.
([#30728](https://github.com/BerriAI/litellm/issues/30728),
[#14152](https://github.com/BerriAI/litellm/issues/14152))

**Integration requires:** required plugins check which event types they can
handle and register filters that force decoding. Uncovered traffic must be
refused. Plugin ordering must cover synthetic responses and every provider
call, including calls made by other plugins.

## 4. One request, many upstream calls

- Hedging: start a second provider after 2 s, keep the first to answer, cancel
  the other. The provider still bills the loser.
- Split a 5,000-input embedding batch over a provider cap, merge by index.
- Merge `/v1/models` across every configured provider.

**Integration requires:** concurrent calls with exactly one committed result,
usage per attempt with its role (winner, loser, shadow), and one completion
per client request. Core owns call and completion lifetimes; routing policy
chooses the result and records roles in extensions.

## 5. A plugin that needs to call a model

Prompt-injection detection, semantic routing, a judge deciding whether to
escalate from a cheap model, translating output mid-stream.
([#19499](https://github.com/BerriAI/litellm/issues/19499))

**Integration requires:** a subrequest API that inherits tenant, egress rules,
residency, budget and trace, with a depth limit. Without it, these calls
bypass all of those.

## 6. The provider fails after bytes were sent

A stream dies after 200 tokens, or stalls. Retrying replays a tool call the
client already saw; the status code is already 200.
([#42955](https://github.com/BerriAI/litellm/issues/42955),
[#40404](https://github.com/BerriAI/litellm/issues/40404))

**Integration requires:** an explicit commit point (after it, no silent retry), a
stall watchdog, errors encoded in the client's own framing (SSE error event,
WebSocket close code), truncated streams reported as errors, not `stop`.

## 7. Realtime and other two-way sessions

OpenAI Realtime over WebSocket, Gemini Live, Responses over WebSocket, MCP
sessions. Many frames in both directions, usage per turn, sessions lasting
hours. ([#18456](https://github.com/BerriAI/litellm/issues/18456),
[new-api #3027](https://github.com/QuantumNous/new-api/issues/3027))

**Integration requires:** duplex event streams as well as unary, streaming, and
job workflows; completion and billing per turn; a drain policy for hour-long
sessions. Core connects receiving streams and sending sinks. Adapters and
plugins interpret turns, meter them, and enforce session limits.

## 8. State that outlives the connection

Background Responses polled later, video jobs settled hours later, a `file_id`
uploaded to one Azure deployment and used from another, provider webhooks for
finished jobs. ([#25759](https://github.com/BerriAI/litellm/issues/25759),
[#36275](https://github.com/BerriAI/litellm/issues/36275))

**Integration requires:** a logical operation id that completion and billing key
on, signed ids that encode deployment and credential and still fit provider
length limits (Azure: 64 chars), durable reservations with a TTL.

## 9. Money under concurrency

Fifty parallel requests against a $1 wallet. Budgets nested key within team
within customer. A stream cut before the usage frame billed as $0.
([one-api #2440](https://github.com/songquanpeng/one-api/issues/2440),
[new-api #5235](https://github.com/QuantumNous/new-api/issues/5235),
[#35524](https://github.com/BerriAI/litellm/issues/35524))

**Integration requires:** reserve and settle through an accounting plugin and
store, atomic across participating accounts or ledgers, idempotent on settle,
and settling from estimated usage when the provider reported none. A request
with no bound on cost is capped or refused. Core lifecycle hooks must let the
plugin reserve before sending and settle once when each call ends.

## 10. Waiting instead of failing

Every provider is rate-limited. Paid users should go first, and each end user
should get a fair share. ([#13405](https://github.com/BerriAI/litellm/issues/13405),
[#18128](https://github.com/BerriAI/litellm/issues/18128))

**Integration requires:** an admission stage before the attempt loop that can
park a request until a deadline.

## 11. Zero data retention for one tenant

A tenant requires that no prompt or completion is stored anywhere, while
billing and audit records still flow.
([#16336](https://github.com/BerriAI/litellm/issues/16336))

**Integration requires:** content as its own channel, separate from billing
records and traces, gated by tenant policy. Every store that holds content
(cache, stored responses, job outputs) is indexed by tenant so it can be
disabled or deleted.

## 12. Local proxy in front of Claude Code or Codex

A developer points their coding agent at the gateway on a laptop. The agent
uses beta headers, new fields and subscription auth the gateway has never
seen. ([#39528](https://github.com/BerriAI/litellm/issues/39528),
[portkey #1345](https://github.com/Portkey-AI/gateway/issues/1345))

**Integration requires:** unknown fields and protocol headers survive untouched,
a mode that runs with no config file.

## 13. Thousands of tenants changing config all day

Respan's case: each org has its own keys, routes and limits, edited at any
time.

**Integration requires:** tenant policy separate from the static config snapshot,
updated incrementally with versions, never written back from the request path.

## 14. Gateway as a library

Another Rust service, or Python through PyO3, wants the pipeline without a
second hop. ([TensorZero embedded](https://www.tensorzero.com/docs/gateway/clients))

**Integration requires:** a kernel that takes a request and returns a response
plus a completion, with no HTTP server inside it. The server is an adapter.

---

## Considered and rejected

- **WASM or scripted plugins as an escape hatch for non-Rust users.** Violates
  [0002](decisions/0002-no-dynamic-plugin-loading.md). The answer for those
  users is prebuilt images and a build tool, as with Caddy's `xcaddy`.
