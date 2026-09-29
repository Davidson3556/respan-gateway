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
| [attachments](attachments.rs) | 1 | A PDF becomes text only in calls to text-only targets, and a `document` part in calls to an Anthropic fallback, per attempt, under a time budget. |
| [pii](pii.rs) | 2 | E-mail addresses hidden in every call's input and restored across stream deltas, tool-call arguments included. |
| [guardrail](guardrail.rs) | 3 | A term list no request skips: every response the client gets and every call's input. |
| [batch_split](batch_split.rs) | 4 | An embeddings batch over a provider's cap split into concurrent calls (`with_input`), merged in order, usage summed. |
| [models_merge](models_merge.rs) | 4 | `GET /v1/models` answered by merging every provider's list, each asked under its own key. |
| [fallback](fallback.rs) | 4, 6 | Retry with `Retry-After`, fallback to another format, a hedged race, a stall watchdog. |
| [judge](judge.rs) | 5 | A plugin that asks a small model to pick the target, as a subrequest. |
| [mcp_proxy](mcp_proxy.rs) | 7 (MCP part) | MCP over streamable HTTP with a tool allow-list; optionally a stdio server. |
| [realtime](realtime.rs) | 7 (WebSocket part) | OpenAI Realtime over WebSocket: e-mail addresses hidden from the provider and restored across deltas, each turn metered as it completes, a turn cut before `response.done` from an estimate. |
| [jobs](jobs.rs) | 8 | An idempotent request: a retry or duplicate gets the stored result; one provider call. |
| [wallet](wallet.rs) | 9 | Nested prepaid wallets; fifty parallel requests never overspend $1. |
| [cost_cap](cost_cap.rs) | 9 | A per-call cost cap: bound `max_tokens`, hold it, cut a runaway stream, settle. |
| [admission](admission.rs) | 10 | Requests wait in a priority queue, round-robin across end users, for a slot, else 429 with `Retry-After`. |
| [logging](logging.rs) | 11 | Request logs to a sink you choose; a zero-retention tenant stores no content. |
| [local_proxy](local_proxy.rs) | 12 | A pass-through Anthropic proxy for Claude Code, byte for byte (the binary's `proxy` command is the no-config version). |
| [tenants](tenants.rs) | 13 | Versioned tenant config edited while requests run; per-tenant provider keys. |
| [embedded](embedded.rs) | 14 | The gateway as a library in another Rust service. |
| [billing](billing.rs) | - | Price each call's usage from your table into your store. |
| [custom_format](custom_format.rs) | - | Add a protocol (OpenAI embeddings) as a kind and a `Codec` (`impl_format!` makes it a `Format`). |
| [custom_transport](custom_transport.rs) | - | A model in this process as a dial-only provider `Format`, no bytes. |

## Not built

- Use case 7: audio turns, and Gemini Live and Responses over WebSocket.
  `realtime` estimates a cut turn's output text only, not its input. Draining
  sessions at shutdown and a maximum session length are in the SDK
  (`serve_ws_until`, tested there), not wired into `realtime`.
- Use case 4: retrying one chunk of a split batch (`batch_split` fails the
  request), and a list of providers read from the gateway (`models_merge` is
  given their names).
- Use case 10: weighted shares per end user, and a queue shared by several
  gateway processes.
- Use case 8: signed job ids.
- Use case 2: streamed calls with several choices (`n > 1`) are refused by
  `pii`, not restored; Anthropic's streamed `partial_json` fragments are not
  held, only OpenAI's tool-call arguments.
- Use case 1: `file_id` references (refused, not resolved).
- Use case 8: durable holds and jobs that survive a restart (`jobs` keeps
  results in memory; its header shows a durable store).
- In a request, the text SDK reads Anthropic tool blocks (`tool_use`,
  `tool_result`) as content with no text form, so it refuses them. In a
  reply they are read and edited.
- Translating a request with tools between chat formats: a field or part is
  sent only in the format it is in (a plugin rewrites it for the target, as
  `attachments` does); nothing converts one format's tools to another's.
