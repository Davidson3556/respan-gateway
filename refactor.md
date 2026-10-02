# Refactor: remove kernel relay; make raw relay an explicit plugin choice

Status: implemented and verified. This is an
approved public-contract change. The owner confirmed retaining SDK per-value
provenance, carrying HTTP heads in opaque events, and using explicit wire/head
slots for WebSocket handshakes. Credentials remain a separate extension.

## Why

Today the kernel decides on its own to relay a client's original wire data
whenever a call carries the client's own input to a provider whose format has
the same name (`core/src/call.rs`, `dial`: `relay = !own && same name`). On a
relay it silently:

1. copies the client's request metadata (original HTTP head + whole body
   bytes) onto the provider's outgoing stream,
2. keeps the provider's response metadata (response head) and hands it to the
   client's format,
3. lets a provider's raw error body (`Fault::with_answer`) reach the client.

HTTP then derives credential passthrough from that provenance
(`http/src/transport.rs`: private `Relayed` marker, `is_relayed`,
`credentials`).

Whether unknown fields, protocol headers and the client's key survive
untouched is an application decision, not a universal one. The core must have
no hidden behavior: **by default nothing is relayed; every call is encoded from
its typed input.** A plugin that wants a raw relay sees the raw data and
supplies it to the call explicitly, through the two channels the kernel
already has: typed values (`Call::with_input`) and `Extensions`
(`Call::with_extension`).

## Baseline

Build on the current working tree, which has uncommitted, tested changes from
the same audit (all workspace tests pass):

- `Erased` lost its `const PROVIDER: bool`; `kind` is set at registration
  (`Erased::client` / `Erased::provider`); a provider's `with_input` snapshot
  is `Provider::snapshot`.
- `core/src/values.rs` has two erased types: `Value` (one event, used at the
  edges, for whole inputs and live-input readers) and `Values` (a batch, used
  by filters, buffering and `ToClient`).
- Plugins are unnamed: `Gateway::plugin(plugin) -> Gateway` is infallible;
  YAML name uniqueness is checked in `gateway/src/config.rs` (`ordered`).

`docs/ARCHITECTURE.md` is deleted in the working tree by the owner. If it is
restored before you start, update it with this contract; otherwise do not
recreate it.

## Target contract

### Core: remove

| Remove | Where |
|---|---|
| `EventStream::with_metadata`, `EventStream::metadata` and the private `metadata`, `metadata_copy`, `set_metadata`, `clear_metadata` | `core/src/events.rs` |
| `type Metadata`, `FormatData` | `core/src/events.rs`, `core/src/extensions.rs` |
| `Fault::with_answer`, `Fault::answer`, private `with_answer_of`, `unrelayed`, the `answer` field | `core/src/fault.rs`; `plugin::failed` keeps only the filter's fault |
| The relay rule in `dial` (both uses), `Input.metadata`, `Input::events(relay)` parameter, `Input.own`'s relay role | `core/src/call.rs`, `core/src/input.rs` |
| `Response.metadata`, the `relayed` flag of `ToClient`, the unrelayed refusal path | `core/src/response.rs`, `core/src/leg.rs`, `core/src/outflow.rs`, `core/src/context.rs` |
| `EventStream::is_lazy` and the `lazy` provenance flag in `Single`; `known_error`'s lazy special case | `core/src/events.rs` |

Keep `EventStream::lazy` as plain deferred decoding (decode at most once, when
read). Keep `EventStream::stop` (bodyless/already-encoded delivery still needs
it). `Call::with_input`'s `own` stays only if something other than relay
needs it; otherwise remove it.

After this, the kernel carries typed values and extensions only. Formats get
no private channel between edges.

### HTTP: add (names are proposals; keep the glossary short)

1. **`Wire`** — the client's raw request: `http::Request<Bytes>` for a whole
   body (head + collected bytes), head only for a live/streaming input.
   Published as an exchange extension by the HTTP adapter path
   (`respan_gateway_http::handle`, next to the existing `Asked`). Consider
   folding `Asked` into it rather than having both. Credential headers are
   *included* only in a form the plugin must copy deliberately (decide: keep
   the `Credential` extension separate, as today).
2. **`SendRaw`** — a call extension: "send this request as given instead of
   encoding the call's input". `Outgoing` uses it verbatim after the usual
   hop-by-hop stripping (`proxy_call`). No `SendRaw` → always encode from the
   typed input. This replaces `RequestMetadata`, `request_head`, `raw_body`.
3. **Provider response head** — a slot a plugin puts on a call
   (`with_extension`), filled by `Outgoing` when the provider's head arrives,
   readable once `call.await` resolves (the call resolves after the head).
   Replaces `ResponseMetadata` on the provider side.
4. **Client response head** — a slot in the exchange's extensions, created by
   the HTTP adapter and shared with the `Incoming` binding. A plugin sets it
   explicitly; the client format writes it if set, else its own default
   (today's `response_head` fallback: a fresh success head). Replaces
   `ResponseMetadata` on the client side.
5. **Provider error answers** — provider formats attach a typed HTTP hint
   (`Fault::with(..)`, e.g. `{ format, response: http::Response<Bytes> }`)
   instead of `with_answer`. Client formats **never** write another provider's
   raw body by default; they write it only when a plugin explicitly asks
   (e.g. a `RawReply` hint the plugin adds to the fault it returns). Replaces
   `whole.rs:53`, `common.rs:360/377/1161/1582`, `opaque.rs:229`,
   `anthropic.rs:1278`, `openai.rs:1140`, `ws/src/events.rs:268`,
   `ws/src/transport.rs:651`, `http/src/lib.rs:480`.
6. **Credentials** — delete the private `Relayed` marker and `is_relayed`.
   `credentials` becomes: the call's `CallHeaders`, else the transport's own
   headers; the client's `Credential` is sent only if the plugin put it in the
   call (via `SendRaw`'s head or `CallHeaders`). Never implicitly.

A relay plugin then reads, in full:

```rust
let wire = cx.extensions().get::<Wire>().cloned().ok_or(..)?;
let head = ProviderHead::default();
let resp = cx.call("openai")
    .with_extension(SendRaw(wire))
    .with_extension(head.clone())
    .await?;
cx.extensions().get::<ClientHead>().map(|c| c.set(head.get()));
Ok(Some(resp))
```

### Files that read metadata today (all must change)

- http: `src/codec.rs` (`request_head`, `raw_body`, `response_head`),
  `src/driver/accept.rs:130`, `src/driver/dial.rs:59,125`,
  `src/formats/opaque.rs:78,115,229`, `src/formats/whole.rs:53`,
  `src/transport.rs` (`Relayed`, `is_relayed`, `credentials`), `src/lib.rs:480`
- providers: `src/openai.rs:127,1097,1140`, `src/anthropic.rs:142,1278`,
  `src/common.rs:360,377,1161,1582`
- ws: `src/events.rs:152,191,268`, `src/transport.rs:651`
- mcp: `src/streamable.rs:95,122,125`
- examples: `integrations/custom_format.rs`, `routing/models_merge.rs`,
  `src/rest.rs`, `src/store.rs`
- binary: `gateway/src/main.rs` `proxy` mode must register an explicit relay
  plugin for the chat provider and for `passthrough` (see use case 12 below).

## Decisions to confirm with the owner before coding

1. **SDK per-value provenance** (`sdk/src/relayed.rs` `Relayed`/`Came`,
   `Conversation::came_in`, `Message::source`, `Reply::came_in`): encoders
   reuse a value's original bytes when it is unchanged (e.g. MCP
   `streamable.rs:144,252,264`, `ws/src/events.rs:53`, `openai.rs:1097`). This
   is format-level, not kernel-level, and the bytes are already visible to
   plugins. Recommendation: keep it in this refactor, but no encoder may
   depend on removed kernel provenance (`openai.rs:1097` uses
   `so_far.relayed`, which came from metadata — rework it to `came_in` only or
   to an explicit signal). Revisit separately.
2. **`Passthrough` (opaque) events** are bare bytes (`Sent<F>(Bytes)`,
   `Got<F>(Bytes)`); the method, path and headers live only in metadata.
   Either the events carry the head (`http::Request<Bytes>` /
   `http::Response<Bytes>`), or passthrough always needs `Wire`/`SendRaw` and
   the head slots. Recommendation: put the head in the events; then
   passthrough needs no special relay plugin.
3. **WebSocket/live sessions**: the handshake head travels as metadata today
   (`ws/src/events.rs:152,191`). Apply the same `Wire`/head-slot design to the
   handshake; confirm it works for a relayed WS session and for a WS→SSE
   bridge.

## Use-case check (`docs/use-cases.md`)

| Use case | After the refactor |
|---|---|
| 1 attachments, 20–50 MB bodies | A plugin that relays sends `Wire` bytes without decoding; otherwise the body is decoded and encoded (accepted cost). |
| 3 required safety plugin | Simpler: no path skips decoding implicitly. |
| 6 failure after bytes sent | Unchanged; errors in the client's framing are encoded by the client format. |
| 7 realtime / two-way | Depends on decision 3. |
| 12 local proxy for Claude Code/Codex | Met by an explicit relay plugin registered by `proxy` mode: headers, unknown fields and subscription auth survive because the plugin chose to send `Wire` and the client's credential. |
| 14 gateway as a library | Unchanged. |

## Tests

Suites that assert the old implicit guarantee must be rewritten to assert the
new contract (default: encoded, no client credential, no provider head, no
raw error body; with an explicit relay plugin: byte-exact request, client
credential only when the plugin sends it, provider head and raw error only
when the plugin asks):

- `providers/tests/relay.rs`, `providers/tests/translate.rs`
- `http/tests/rules.rs`, `codec.rs`, `driver_formats.rs`, `driver_toy.rs`,
  `formats_custom.rs`, `line.rs`, `new_api.rs`, `standard_body.rs`,
  `standard_polling.rs`, `streams.rs`
- `adapters/axum/tests/credentials.rs`, `http.rs`
- `core/tests/transports.rs`, `edge/mod.rs`, `lifecycle.rs`; unit tests in
  `core/src/events.rs`, `format.rs`, `fault.rs`
- `gateway/tests/e2e.rs`, `examples/tests/batch_split.rs`
- MCP JSON/SSE reframing tests (`mcp/tests/mcp.rs`) per decision 1

Add a core test that a same-name call without any opt-in carries no
format-private data (there is no API for it any more, so this is a compile-
level guarantee; assert instead that the provider's sink sees a plain typed
input) and an HTTP test that the client's credential never reaches a provider
without an explicit plugin.

## Order of work

1. HTTP: add `Wire`, `SendRaw`, the two head slots and the answer hint
   alongside the old paths; switch `credentials` to explicit.
2. Move providers, ws, mcp, examples and the binary to the new types; add the
   relay plugin for `proxy` mode.
3. Remove metadata, answers, relay and lazy provenance from core.
4. Rewrite the tests listed above; delete tests of removed behavior.
5. Verify:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --all-features
```

Also build each crate with `--no-default-features` (feature unification can
hide missing feature gates).

## Execution result

- Removed kernel metadata, automatic relay selection, raw fault answers and lazy provenance.
- Added `Wire`, `SendRaw`, `ProviderHead`, `ClientHead`, `ProviderError` and `RawReply`.
- Kept SDK per-value provenance; opaque events now own HTTP messages.
- Proxy mode, examples and fixtures share the optional `plugins::relay` policy.
- Verified default typed encoding, separate credential opt-in, raw error opt-in,
  WebSocket relay, WS-to-SSE, MCP reframing and bodyless response lifecycles.
- Formatting, Clippy (all targets/features, warnings denied), documentation
  (warnings denied), and all ten individual no-default-features builds passed.
- A fresh `cargo test --workspace` passed end to end, including runtime tests
  and doctests. The realtime example's 43 tests passed.
- Network stress tests ran with `ulimit -n 8192`; the environment's default
  1024 descriptors is insufficient for their concurrent sockets.
