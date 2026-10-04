# Examples

Runnable examples are grouped by topic below. Each is an explicit Cargo target
with its test modules in [`tests/cases/`](tests/cases), grouped by the same topics.
Cargo discovers the four `tests/text_*.rs` integration suites automatically.
Private source modules live in
[`support/`](support) and shared library helpers remain in [`src/`](src).
Use case numbers refer to [the use-case guide](../docs/use-cases.md).

Run from the repository root (replace `wallet` with any target name below):

```bash
cargo run -p respan-gateway-examples --example wallet
cargo test -p respan-gateway-examples --all-targets
cargo test -p respan-gateway-examples --example wallet
```

Most examples run offline with scripted/local providers. **`gemini_live` and
`responses_ws` are live-provider servers:** running them needs credentials and
can incur provider charges; their automated tests use local scripted sockets
and do not need credentials or make paid requests.

| Topic | Example | Demonstrates |
| --- | --- | --- |
| Policies | [admission](policies/admission.rs) | Weighted fair admission, deadlines and crash leases (use case 10). |
| | [guardrail](policies/guardrail.rs) | Check every request and response against a term list (3). |
| | [logging](policies/logging.rs) | Request logs and zero-retention tenants (11). |
| | [pii](policies/pii.rs) | Redaction and restoration across streaming text and tool calls (2). |
| | [tenants](policies/tenants.rs) | Hot-reloaded tenant config and per-tenant keys (13). |
| Routing | [batch_split](routing/batch_split.rs) | Bounded concurrent embedding chunks, retries and usage merge (4). |
| | [fallback](routing/fallback.rs) | Retry, fallback, hedging and stall watchdogs (4, 6). |
| | [judge](routing/judge.rs) | A model-selecting subrequest (5). |
| | [models_merge](routing/models_merge.rs) | Merge provider model lists using their respective keys (4). |
| Billing | [billing](billing/billing.rs) | Price each call's usage into an application-owned store. |
| | [cost_cap](billing/cost_cap.rs) | Reserve and settle a per-call cost cap (9). |
| | [wallet](billing/wallet.rs) | Nested prepaid wallets and concurrent holds (9). |
| Integrations | [attachments](integrations/attachments.rs) | Bounded PDF extraction, tenant-scoped cache and native fallback (1). |
| | [custom_format](integrations/custom_format.rs) | Implement an embeddings protocol with `Format`. |
| | [custom_transport](integrations/custom_transport.rs) | Bind an in-process model without HTTP bytes. |
| | [mcp_proxy](integrations/mcp_proxy.rs) | MCP over streamable HTTP with a tool allow-list (7). |
| Hosting | [embedded](hosting/embedded.rs) | Embed the gateway in another Rust service (14). |
| | [local_proxy](hosting/local_proxy.rs) | Byte-exact pass-through proxy for Claude Code (12). |
| Jobs | [jobs](jobs/jobs.rs) | SQLite-backed durable idempotency and crash leases (8). |
| | [jobs_background](jobs/jobs_background.rs) | Durable background Responses, recovery, cancellation and webhook outbox (8). |
| Realtime | [realtime](realtime/realtime.rs) | Metered Realtime text/audio, cancellation and output caps (7). |
| | [gemini_live](realtime/gemini_live.rs) | Gemini Live text/audio turns, tools and shutdown (7). **Live credentials to run.** |
| | [responses_ws](realtime/responses_ws.rs) | Responses WebSocket lanes, continuation and shutdown (7). **Live credentials to run.** |

## Billing policies

Billing is application code, not a supported SDK or first-party plugin API.
The private [accounting](support/billing/accounting.rs) module quotes each
attempt before sending, prices generic `Usage`, and settles once; the
[ledger](support/billing/ledger.rs) demonstrates atomic nested-account holds
and idempotent settlement in memory, **not** durable charging or recovery.
Register accounting last so it observes provider outputs before earlier filters
can reject them. Unsent calls and uncut provider refusals cost nothing; sent
calls cut off or lacking complete usage cost at least their reserve. The wallet
target also runs ledger, accounting, wire and atomic tally tests.

## Live provider runs

```bash
OPENAI_API_KEY=... MODEL=... MODEL_INPUT_LIMIT=... cargo run -p respan-gateway-examples --example responses_ws
GEMINI_API_KEY=... MODEL=... MODEL_INPUT_LIMIT=... cargo run -p respan-gateway-examples --example gemini_live
```

These bind localhost ports 8081 and 8082 respectively. Choose the actual
model's full input-context bound for `MODEL_INPUT_LIMIT`; other models are
refused. Gemini audio clients must send `realtimeInput.activityStart` before
audio input; automatic activity detection is disabled. Ctrl-C closes active
sessions and awaits billing hooks. The realtime audio policy does not redact
speech; its text-PII mode refuses opaque audio.
Live-provider interoperability is not established by the local protocol tests.

Private [text support](support/text.rs) is shared by guardrail, PII and
realtime; the four `text_*` integration test targets cover its reader, tool JSON
and split-escape behavior.

## Remaining gaps

Streaming background Responses, arbitrary body-ID mapping, tenant-wide job
content deletion/expiry and signed file-upload ownership need application
work. Background multimodal inputs need provider-specific quotes; caller-owned
scheduling and provider idempotency are required for crash-safe job recovery.
Provider-hosted tools without an equivalent in the other chat format are
refused.
