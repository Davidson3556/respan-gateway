# Examples

Each example is one file that reads top to bottom as the code to copy. Most
`main` functions run offline against a mock provider; `gemini_live` and
`responses_ws` serve real providers using environment credentials. Their tests, in
`tests/<name>.rs`, check the promise it makes. `src/lib.rs` holds the helpers
the examples share.

```bash
cargo run -p respan-gateway-examples --example wallet
cargo test -p respan-gateway-examples
```

Use case numbers refer to [docs/use-cases.md](../docs/use-cases.md).

| Example | Use cases | What it shows |
| --- | --- | --- |
| [attachments](attachments.rs) | 1 | Killable, bounded PDF extraction; tenant/SHA-256 SQLite cache with deletion fencing; metadata-checked stored files fetched under the tenant's key; per-attempt conversion facts and native PDF fallback. The HTTP adapter supports per-route body limits. |
| [pii](pii.rs) | 2 | E-mail addresses hidden in every call's input and restored across stream deltas, tool-call arguments included. |
| [guardrail](guardrail.rs) | 3 | A term list no request skips: every response the client gets and every call's input. |
| [batch_split](batch_split.rs) | 4 | Bounded concurrent embedding chunks, individual transient retries, index-checked merge and summed usage. |
| [models_merge](models_merge.rs) | 4 | `GET /v1/models` answered by merging every provider's list, each asked under its own key. |
| [fallback](fallback.rs) | 4, 6 | Retry with `Retry-After`, fallback to another format, a hedged race, a stall watchdog. |
| [judge](judge.rs) | 5 | A plugin that asks a small model to pick the target, as a subrequest. |
| [mcp_proxy](mcp_proxy.rs) | 7 (MCP part) | MCP over streamable HTTP with a tool allow-list; optionally a stdio server. |
| [realtime](realtime.rs) | 7 | OpenAI Realtime text and explicitly permitted audio: per-turn usage, duration-based audio reservations, output caps, cancellation settlement, text PII filtering that refuses opaque audio, and session deadlines. Separately billed transcription is refused in capped mode. |
| [gemini_live](gemini_live.rs) | 7 | Gemini setup, text/audio turns with explicit activity boundaries, tool and resumption events, per-turn usage, reservations, interruptions, and graceful shutdown. |
| [responses_ws](responses_ws.rs) | 7 | Responses over WebSocket: independent named lanes, continuation IDs, warmup, output caps, per-turn usage and cancellation settlement, and graceful shutdown. |
| [jobs](jobs.rs) | 8 | Durable idempotency through SQLite, crash leases, stable provider idempotency keys, tenant-isolated replay and lease-loss cancellation. |
| [jobs_background](jobs_background.rs) | 8 | Durable background Responses jobs, signed deployment IDs, restart recovery, cancellation, budget holds and a retryable completion-webhook outbox. Streaming remains unsupported. |
| [wallet](wallet.rs) | 9 (and 4) | Nested prepaid wallets; fifty parallel requests never overspend $1; the loser of a hedge pays at least its hold. |
| [cost_cap](cost_cap.rs) | 9 | A per-call cost cap: bound `max_tokens`, hold it, cut a runaway stream, settle. |
| [admission](admission.rs) | 10 | Weighted fair turns within priority, shared across processes through SQLite; deadlines, cancellation and renewable crash leases; 429 with `Retry-After` when admission expires. |
| [logging](logging.rs) | 11 | Request logs to a sink you choose; a zero-retention tenant stores no content. |
| [local_proxy](local_proxy.rs) | 12 | A pass-through Anthropic proxy for Claude Code, byte for byte (the binary's `proxy` command is the no-config version). |
| [tenants](tenants.rs) | 13 | Versioned tenant config edited while requests run; per-tenant provider keys. |
| [embedded](embedded.rs) | 14 | The gateway as a library in another Rust service. |
| [billing](billing.rs) | - | Price each call's usage from your table into your store. |
| [custom_format](custom_format.rs) | - | Add a protocol (OpenAI embeddings) as a kind and direct `Format` implementations over owned HTTP messages. |
| [custom_transport](custom_transport.rs) | - | A model in this process as a dial-only provider `Format`, no bytes. |

## Billing policies

Billing is application code, not a supported SDK or first-party plugin API.
[billing](billing.rs) records priced usage without reservations;
[wallet](wallet.rs) and [cost cap](cost_cap.rs) share private source modules in
`billing/`, also used for reservations by the realtime examples. These modules
are not exported by `src/lib.rs`:

- [accounting.rs](billing/accounting.rs) holds each provider attempt's `Quote`
  before sending and invokes settlement once at its end. A filter prices generic
  message `Usage` into a shared `Tally`. Register `Accounting` last so its filter
  sees provider outputs before earlier filters can reject them. Unsent calls and
  uncut provider refusals cost nothing; sent calls cut off or lacking complete
  usage cost at least their reserve. An empty account quote is refused with 401.
- [ledger.rs](billing/ledger.rs) demonstrates atomic nested-account reservations,
  idempotent settlement and bounded hold retention in memory. It is not a durable
  billing store. Settlement errors do not change the response or trigger policy
  retries; applications must provide durable charging and recovery. Hold expiry
  releases reserved funds but cannot recover a lost charge.

The wallet target also runs the migrated ledger, accounting, non-HTTP wire and
atomic tally tests under `tests/`, preserving concurrency, cancellation, refusal,
TTL, late settlement and failure-recovery coverage. Run them with
`cargo test -p respan-gateway-examples --example wallet`.

## Remaining gaps

- Streaming background Responses, chaining before a native ID exists, and mapping
  arbitrary body IDs or list endpoints.
- Tenant-wide job-content deletion and retention expiration.
- Periodic job recovery/webhook supervision must be scheduled by the caller;
  crash-safe external execution also depends on provider idempotency support.
- Background multimodal or external-reference inputs need a provider-specific
  cost quote; the example's byte/token estimate is insufficient for those inputs.
- Signed file-upload ownership across deployments and video-specific job adapters.
- Provider-hosted tools with no equivalent in the other chat format are refused.

## Live provider examples

`OPENAI_API_KEY=... MODEL=... MODEL_INPUT_LIMIT=... cargo run -p respan-gateway-examples --example responses_ws`
serves `/v1/responses` on localhost:8081. `GEMINI_API_KEY=... MODEL=... MODEL_INPUT_LIMIT=... cargo run -p
respan-gateway-examples --example gemini_live` serves the Gemini Live path on
localhost:8082. Ctrl-C closes existing sessions and waits for billing hooks.
Each policy binds its configured model to `MODEL_INPUT_LIMIT`, the model's full
input-context bound; client requests for other models are refused. Sessions end
after 30 minutes, before reservation expiry. Their shared budget quote includes
that full input bound plus the enforced output cap;
amounts are token units, not a hard-coded currency price. Choose per-model prices
and bounds when embedding these policies. Gemini disables automatic/proactive
generation and requires activity boundaries for audio so the hold precedes generation.

The realtime audio policy preserves waveforms; it does not redact speech. Its
text-PII mode refuses opaque input audio. A deployment requiring speech redaction
must transcribe, redact, and synthesize before permitting that audio path.
Protocol tests use real local sockets and scripted providers; they do not claim
credentialed interoperability with a live external model.

## Private text support

Guardrail, PII and realtime share `support/text.rs` as a private source module.
It owns provider-aware readable-text checks, streaming holdback and placeholders;
these are application examples, not part of the plugin SDK. The four `text_*`
regression targets preserve the reader, tool-JSON and split-escape coverage.
