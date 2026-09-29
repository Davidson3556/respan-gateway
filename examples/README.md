# Examples

Each example is one file that reads top to bottom as the code to copy. Its
`main` runs offline against a mock provider, and its tests, in
`tests/<name>.rs`, check the promise it makes. `src/lib.rs` holds the helpers
the examples share.

```bash
cargo run -p respan-gateway-examples --example wallet
cargo test -p respan-gateway-examples
```

Use case numbers refer to [docs/use-cases.md](../docs/use-cases.md).

| Example | Use cases | What it shows |
| --- | --- | --- |
| [attachments](attachments.rs) | 1 | A PDF becomes text only in calls to text-only targets, per attempt, under a time budget. |
| [pii](pii.rs) | 2 | E-mail addresses hidden in every call's input and restored across stream deltas. |
| [guardrail](guardrail.rs) | 3 | A term list no request skips: every response the client gets and every call's input. |
| [fallback](fallback.rs) | 4, 6 | Retry with `Retry-After`, fallback to another format, a hedged race, a stall watchdog. |
| [judge](judge.rs) | 5 | A plugin that asks a small model to pick the target, as a subrequest. |
| [mcp_proxy](mcp_proxy.rs) | 7 (MCP part) | MCP over streamable HTTP with a tool allow-list; optionally a stdio server. |
| [realtime](realtime.rs) | 7 (WebSocket part) | OpenAI Realtime over WebSocket: e-mail addresses hidden from the provider and restored across deltas, each turn metered as it completes. |
| [jobs](jobs.rs) | 8 | An idempotent request: a retry or duplicate gets the stored result; one provider call. |
| [wallet](wallet.rs) | 9 | Nested prepaid wallets; fifty parallel requests never overspend $1. |
| [cost_cap](cost_cap.rs) | 9 | A per-call cost cap: bound `max_tokens`, hold it, cut a runaway stream, settle. |
| [admission](admission.rs) | 10 | Requests wait in a priority queue for a slot, else 429 with `Retry-After`. |
| [logging](logging.rs) | 11 | Request logs to a sink you choose; a zero-retention tenant stores no content. |
| [local_proxy](local_proxy.rs) | 12 | A pass-through Anthropic proxy for Claude Code, byte for byte. |
| [tenants](tenants.rs) | 13 | Versioned tenant config edited while requests run; per-tenant provider keys. |
| [embedded](embedded.rs) | 14 | The gateway as a library in another Rust service. |
| [billing](billing.rs) | - | Price each call's usage from your table into your store. |
| [custom_format](custom_format.rs) | - | Add a protocol (OpenAI embeddings) as a kind and a `Codec` (`impl_format!` makes it a `Format`). |
| [custom_transport](custom_transport.rs) | - | A model in this process as a dial-only provider `Format`, no bytes. |

## Not built

- Use case 4: splitting an embedding batch and merging `/v1/models`.
- Use case 7: audio turns, Gemini Live and Responses over WebSocket, a limit on
  a session's length, draining long sessions at shutdown, and a turn cut before
  its `response.done` (`realtime` does not meter it).
- Use case 10: fair share per end user.
- Use case 8: signed job ids.
- Use case 2: restoring a placeholder split across streamed tool-argument
  fragments.
- Use case 2: streamed calls with several choices (`n > 1`) are refused by
  `pii`, not restored.
- Use case 1: `file_id` references, and a converted attachment reaching a
  fallback in another format.
- Use case 8: durable holds and jobs that survive a restart (`jobs` keeps
  results in memory; its header shows a durable store).
- In a request, the text SDK reads Anthropic tool blocks (`tool_use`,
  `tool_result`) as content with no text form, so it refuses them. In a
  reply they are read and edited.
