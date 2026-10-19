# Examples

Runnable examples are grouped by topic below. Each is an explicit Cargo target.
Private source modules live in [`support/`](support) and shared library
helpers remain in [`src/`](src).

Run from the repository root (replace `wallet` with any target name below):

```bash
cargo run -p respan-gateway-examples --example wallet
cargo build -p respan-gateway-examples --examples
```

Most examples run offline with scripted/local providers. **`gemini_live`,
`responses_ws` and `tool_guard` are live-provider servers:** running them needs
credentials and can incur provider charges.

| Topic | Example | Demonstrates |
| --- | --- | --- |
| Policies | [admission](policies/admission.rs) | Weighted fair admission, deadlines and crash leases. |
| | [guardrail](policies/guardrail.rs) | Check every request and response against a term list. |
| | [logging](policies/logging.rs) | Request logs and zero-retention tenants. |
| | [media_caps](policies/media_caps.rs) | Image count and size caps, an audio upload cap and budget, and embedding tokens metered per key, over the media kinds. |
| | [pii](policies/pii.rs) | Redaction and restoration across streaming text and tool calls. |
| | [tenants](policies/tenants.rs) | Hot-reloaded tenant config and per-tenant keys. |
| Routing | [batch_split](routing/batch_split.rs) | Bounded concurrent embedding chunks, retries and usage merge. |
| | [fallback](routing/fallback.rs) | Retry, fallback, hedging and stall watchdogs. |
| | [judge](routing/judge.rs) | A model-selecting subrequest. |
| | [models_merge](routing/models_merge.rs) | Merge provider model lists using their respective keys. |
| Billing | [billing](billing/billing.rs) | Price each call's usage into an application-owned store. |
| | [cost_cap](billing/cost_cap.rs) | Reserve and settle a per-call cost cap. |
| | [wallet](billing/wallet.rs) | Nested prepaid wallets and concurrent holds. |
| Integrations | [attachments](integrations/attachments.rs) | Bounded PDF extraction, tenant-scoped cache and native fallback. |
| | [custom_format](integrations/custom_format.rs) | Implement an embeddings protocol with `Format`. |
| | [custom_transport](integrations/custom_transport.rs) | Bind an in-process model without HTTP bytes. |
| | [mcp_proxy](integrations/mcp_proxy.rs) | MCP over streamable HTTP with a tool allow-list. |
| Hosting | [embedded](hosting/embedded.rs) | Embed the gateway in another Rust service. |
| | [local_proxy](hosting/local_proxy.rs) | Byte-exact pass-through proxy for Claude Code. |
| Jobs | [jobs](jobs/jobs.rs) | SQLite-backed durable idempotency and crash leases. |
| | [jobs_background](jobs/jobs_background.rs) | Durable background Responses, recovery, cancellation and webhook outbox. |
| Realtime | [realtime](realtime/realtime.rs) | Metered Realtime text/audio, cancellation and output caps. |
| | [gemini_live](realtime/gemini_live.rs) | Gemini Live text/audio turns, tools and shutdown. **Live credentials to run.** |
| | [responses_ws](realtime/responses_ws.rs) | Responses WebSocket lanes, continuation and shutdown. **Live credentials to run.** |
| | [tool_guard](realtime/tool_guard.rs) | A tool allow-list for any realtime session, here OpenAI Realtime clients on Gemini Live. **Live credentials to run.** |

## Billing policies

Billing is application code, not a supported public or first-party plugin API.
The private [accounting](support/billing/accounting.rs) module quotes each
attempt before sending, prices the typed `Usage`, and settles once; the
[ledger](support/billing/ledger.rs) demonstrates atomic nested-account holds
and idempotent settlement in memory, **not** durable charging or recovery.
Register accounting last so it observes provider outputs before earlier filters
can reject them. Unsent calls and uncut provider refusals cost nothing; sent
calls cut off or lacking complete usage cost at least their reserve.

## Live provider runs

```bash
OPENAI_API_KEY=... MODEL=... MODEL_INPUT_LIMIT=... cargo run -p respan-gateway-examples --example responses_ws
GEMINI_API_KEY=... MODEL=... MODEL_INPUT_LIMIT=... cargo run -p respan-gateway-examples --example gemini_live
GEMINI_API_KEY=... ALLOWED_TOOLS=... cargo run -p respan-gateway-examples --example tool_guard
```

These bind localhost ports 8081, 8082 and 8083 respectively. Choose the actual
model's full input-context bound for `MODEL_INPUT_LIMIT`; other models are
refused. Gemini audio clients must send `realtimeInput.activityStart` before
audio input; automatic activity detection is disabled. Ctrl-C closes active
sessions and awaits billing hooks. The realtime audio policy does not redact
speech; its text-PII mode refuses opaque audio.

Private [text support](support/text.rs) is shared by guardrail, PII and
realtime.

## Remaining gaps

Streaming background Responses, arbitrary body-ID mapping, tenant-wide job
content deletion/expiry and signed file-upload ownership need application
work. Background multimodal inputs need provider-specific quotes; caller-owned
scheduling and provider idempotency are required for crash-safe job recovery.
Provider-hosted tools without an equivalent in the other chat format are
refused.
