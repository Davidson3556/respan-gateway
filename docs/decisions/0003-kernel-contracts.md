# 0003. Kernel contracts: minimal, typed by kind, grown without breaking

- Status: accepted
- Date: 2026-09-25

## Context

The [use cases](../use-cases.md) need far more than a first release:
sessions, more of the provider edge, budgets, retention, work that
outlives its request. Adding it must not break plugins or formats, or
change what they see. A chat model in the kernel fits neither MCP,
images, video nor users' own protocols. Translators per pair of formats
grow with the square of the formats and give plugins no typed view.

## Decision

### A minimal kernel

- Four hooks ([0004](0004-two-edges-neutral-events.md)) and one response
  pipeline. A new hook needs a use case and a test.
- `on_client` changes the head, input, model and target, never the
  body's bytes.
- Edits are for message streams: responses, and a session's frames.
- An embedder gets the request's one `Outcome` from `Closed`.
- Identity and HTTP details belong to plugins and adapters.

### Kinds

- A kind is the pair of types its formats share, `Format::Input` and
  `Format::Output`: defined outside the kernel by anyone, as few as the
  traffic needs, never shared types like `serde_json::Value`. A format
  decodes and encodes its input; one of no kind names `()` for both.
- Formats of one name are one protocol, so of one kind.
- Plugins ask by type (`cx.input_mut::<Conversation>()`,
  `cx.filter(|r: &mut Reply| ..)`); other traffic is `WRONG_KIND`
  ([0005](0005-no-routes-global-plugins.md)).
- An unchanged input, read or not, reaches a provider of the client's
  format as the client's body; a changed one is patched into it; another
  format of the kind gets it encoded anew.
- A provider sharing neither the client's format nor its kind is refused
  when called.
- Decoding refuses only a body that is not the format. What the input
  type cannot hold is relayed within one format, and refused on
  translation unless it cannot change the answer.
- Within one format, body fields, the query and end-to-end headers pass
  on, except headers that change how the provider reads the request or
  who sent it. Across formats only the input passes.
- The gateway's own credentials are removed before `handle`, or by a
  plugin through `Context::head_mut`; the transport decides the rest.
- Inputs and outputs are copied and compared to detect changes, so large
  payloads sit in an `Arc` of an `Eq` type.
- The kernel holds the model; the rest is in the input.

### Growing without breaking

- Growing data types are `#[non_exhaustive]` or have private fields;
  types plugins build have constructors.
- New behaviour comes as defaulted trait methods, context methods, or
  entry points beside `Gateway::handle`. The `Arc` impls of `Format`,
  `Plugin` and `Transport`, like any wrapper, forward every method.
- A plugin lacking a later hook covers nothing that hook sees; required
  plugins declare what they cover (below).
- `Gateway::new(providers)` keeps its signature. New gateway-wide
  settings (required plugins, the drain deadline, the store) are by-value
  methods as in 0005, given before the gateway is cloned or used.
- A later Part comes with an `Encoder` method whose default refuses it.
- How a response ends (`Result<(), Fault>`) and whether its bytes are
  the client format's own (`bool`) never grow.
- Content outside the hooked request and response, such as a session's
  turns, reaches only plugins that opt in; for others the accessors are
  an `Err`, so a guardrail refuses what it cannot see.
- Safety never relies on plugin order: plugins after one that answers
  never run, so a router sets the target rather than calling it.
- Response edits apply the last added first, wrapping later ones.
- Providers are reached only through either context, so whatever later
  governs calls covers every call.

### Promised

Each lands additively; names are chosen when it comes.

**Calls**

- A call with its own `http::Request`, of any kind, format and provider.
- More of `on_provider`: waiting for capacity, adding to the call's
  facts, and calling providers in the request's scope, running every
  other plugin's `on_provider` within the depth limit.
- Changing a call's final `http::Request<Body>` on the provider edge,
  such as to sign it, with any client credentials the format passed on.
  A streamed body stays streamed.
- Targets given by value, such as a per-tenant provider, checked for
  kind when called.
- Reading a response's Parts, usage included, before committing (peeked,
  not taken) or to the end.
- An owned call handle in the request's scope, so an async stream
  filter, a trait beside `Filter` whose runs are hook runs, can call
  models mid-stream.
- Usage a call or turn never reported is estimated by each plugin's
  `on_provider_end` from the call's state, or by the tap on traffic of
  no kind; a truncated call's partial report is topped up.
- A dropped call, such as an unread hedge loser, is settled by its
  plugin from its reservation.
- On the provider edge: each call's id, its role set on the `Call` (a
  hedge loser, a shadow), and whether it was committed, which only the
  kernel marks.
- Usage told to the client that calls already count, such as a merged
  batch's, comes by a method beside `with_usage`, not billed again;
  `Outcome.usage` keeps its meaning.
- A call runs only the gateway's own plugins. A guarded subrequest is
  its own entry point, within the scope and the depth limit.

**Scope**

- An opaque value on the `Context`, such as a tenant, carrying the trace
  context; calls inherit it and state keys on it.
- The caller of `handle`, or an async resolver beside `Gateway::new`,
  fixes the scope and its storage key before the plugins run; the kernel
  takes it out of the extensions, so no plugin can change it.
- The resolver sees the head, or a record's key, and may refuse in the
  client's format, under the admission deadline, which a scope may lower,
  never raise.
- Tenant policy (keys, targets, limits) comes through the scope from a
  versioned source outside the `Gateway`'s snapshot.
- Retention per scope can make every accessor yielding body bytes or
  events an `Err` for plugins not opted in, provider edge and tap too:
  - a fault's message, details and source are content, leaving status
    and code; signing a body counts as reading it;
  - a policy may count the URI as content, then shown redacted;
  - every content store keys on the scope; what an opted-in plugin
    copies out is its own;
  - each hook has its own plugin's rights; one running while another
    holds the `Context`, such as `on_provider`, gets its own view.

**Operation records**

- Work outliving its request, such as a video job or a shadow call that
  must finish, closes as a record with an id billing keys on, never
  delaying `on_client_end`.
- A later request, such as a poll or a webhook, settles it by that id
  through the store; that needs a resolver taking a stored key.
- A defaulted settle hook beside `on_client_end` gives the record's
  `Outcome` to its opener, every plugin whose `on_provider` ran on its
  calls, and the required plugins covering it, matched by configuration
  keys the record stores, on whichever gateway settles it.
- It runs after the settling response, under the after-end budget rule
  and the record's scope re-resolved from its key. A resolver told it
  resolves to settle returns a scope refusing new work, not a refusal,
  so a revoked tenant still settles.
- Its calls are requests of their own under that scope, with a fresh
  deadline, within the depth limit, running the provider hooks of the
  gateway's and the required plugins.
- Embedders get settlements through a method beside `Gateway::new`.
  `idle` and the drain never wait for a record awaiting settlement.
- Delivery is at least once, with the same facts and ids, so plugins
  settle idempotently:
  - the store claims each run; a claim lapses if its gateway does not
    finish it;
  - a run is pending for a plugin until its hook returns, or is
    cancelled past its budget, on a gateway that has the plugin;
  - refused calls are facts of the run, not a reason to rerun it;
  - a run's calls' facts reach the plugins in a further run, the series
    stopping at the depth limit;
  - a later settle, or any gateway sharing the store, delivers pending
    runs; the record is removed once none is pending.
- At the TTL, pending runs are delivered with no request, their calls
  refused. A record never settled is delivered with an `Outcome` ending
  `Err` and its calls' usage, the budget plugin charging its
  reservations rather than releasing them, and is then removed.

**Budgets and time**

- Reservations are made in `on_provider`, a turn's too, atomic across
  nested ledgers through the store, settled idempotently on a call's,
  operation's or turn's id.
- A reservation on a record's operation or calls lasts until its
  settlement charges it; any other expires, released, after a TTL its
  plugin sets.
- A required budget plugin refuses a request with no cost bound, such as
  one of no kind.
- An admission deadline is set with the scope, a session's anew each
  turn. A hook parks through a method of either context, bounded by it;
  only parked time counts.
- Calls are bounded by stall bounds within a turn, set per format served
  and per provider, never by the deadline or a hook's budget. A format
  may have none, as an MCP stream idle for hours needs. An idle bound
  covers a silent session.
- Each hook run has a time budget, enforced at await points on its own
  view, merged back only if it returns in time. The clock pauses while
  its calls are pending and its offloaded work waits for a thread.
- A hook past its budget fails as its `Err` would, unless its plugin may
  be skipped: then its changes are dropped and its calls cancelled, their
  facts and reservations still reaching the `Outcome`. A plugin required
  for the scope is never skipped.
- After-end hooks have no `Err`: past budget a skippable one is
  cancelled and logged; a required one runs on until the drain deadline,
  which records it as lost. Blocking inline is a bug.

**Bodies**

- Streamed request bodies, passed on untouched, the model read from the
  head or from a prefix like a body.
- A request streams only when every plugin, required ones too, says
  through a default method that it does not need the body. It goes once:
  a later call is an `Err`, so a plugin that may call twice needs it.
- A format without the new methods gets the body buffered up to its
  limit.
- Trailers, through default decoder and encoder methods: a relayed
  response's pass on untouched; a translated one refuses them rather
  than drop them. A decoder may consume one that cannot change the
  answer, such as `grpc-status: 0`, or turn it into Parts, usage or an
  `Err` end.

**Sessions, on the same types**

- A session is one `http::Request<Body>` whose body, and its response's,
  carry the protocol's frames after the handshake.
- The kernel keeps its `Upgrade` headers. The transport returns at the
  `101` head with both bodies streaming; a session provider's transport
  dials its `https` URL.
- An entry point beside `Gateway::handle` is fed the upgraded
  connection's bytes. A helper crate outside the kernel frames them,
  ending compression and subprotocols on each edge so frames can be
  edited.
- One codec per direction. A session format's default method splits the
  client's bytes into one body per frame; the kernel runs `decode` and
  `encode` on each. Every frame, the first included, is the kind's input.
  Each body chunk is exactly one frame, both ways.
- `filter` over the input type, on either context, edits input frames
  like responses, the last added first (today an `Err` with
  `WRONG_KIND`); over the output type it is unchanged. A required
  plugin's input filter runs after the others', seeing what the provider
  gets.
- A turn Part marks which turn the Parts after it belong to and carries
  how it ended; the session splitter's decoder pushes one before the
  client frame starting a turn. Turns may overlap.
- A refused turn is an error event and the session stays open; only a
  failed end closes it.
- A drain ends a session with a code of its own, written in the format's
  close framing.
- Filters added at the handshake last the session. Those added in a
  turn's `on_provider` see only its events and the frames held while the
  hook ran, and get their `end` with its usage.
- A turn starts at the first mark from either side; its frames wait
  while its `on_provider` runs, outside stall bounds.
- A provider-started turn a hook refuses is cancelled by a frame the
  provider's format encodes, and billed from the call's facts.
- Each turn is a call of its own: `on_provider` at its start, to
  reserve, park or refuse, one at a time in turn order; `on_provider_end`
  with its usage for every turn that started; the gateway's plugins
  first, then the required ones.
- An embedder gets each turn's `Outcome` beside `Closed`;
  `Outcome.usage` is the sum of the turns.
- A session's scope is re-resolved as each turn starts; a refusal ends
  the session as a drain does.
- A client-first frame, such as a setup frame, is read like a body
  through `model`, `decode` and `prepare`, before `on_client`, the
  gateway accepting the upgrade before dialing the provider. The format
  writes a model a plugin sets into it, or refuses the request, so
  `set_model` stays infallible. Without one, `on_client` sees no input
  and inputs are read per turn.
- After the handshake `input_mut` is an `Err`, model and target apply to
  later calls, and head edits are refused.
- A per-turn start hook on the client edge runs after the client's
  format reads the turn's first input, so `cx.input()` and `cx.model()`
  work there; it may set the turn's model and target.
- The turn's call's `on_provider` may change the model again and reads
  the turn's first input through `px.input`; its provider's format
  encodes each of the call's inputs.
- MCP over streamable HTTP is ordinary requests tied by scope and the
  store, not a session.

**Drain**

- A drain with a deadline, beside `Gateway::idle`, ends each session
  between turns, and any response still open at the deadline, in its
  own close framing, so a swapped config keeps no revoked key alive.
- A drain can be aimed at one scope key, a tenant or a credential.
- A drain, or a refusal on re-resolving a scope, ends new work only.
  Settling usage already incurred still runs within the drain's
  deadline, its calls refused: the end hooks, the settle hook, and the
  end of a provider-started turn. A call it cancels is billed as
  dropped.

**Other**

- Plugins required on the gateway, for every request or per scope, each
  declaring the kinds, formats or input types and the hooks it covers;
  the kernel refuses any request or call they cannot cover.
- A required plugin checks each call in `on_provider` after the others,
  and a plugin's answer before it is committed. Its filter is added
  first, so it sees what the client gets. It gets both edges' end hooks
  after the others'. A guardrail covering every call does it on the
  provider edge.
- Formats writing path or header fields from the input.
- Translation between kinds, under a decision of its own.
- Scrubbing the error text of a provider called with the operator's key,
  which may name the operator's account, before a client sees it,
  through a defaulted `Format` method.

Also new API beside these: the store; a read-only tap that sees usage as
it streams and can fail the response; body limits per format served,
which a scope may lower, never raise; offloading blocking work; and
resolving stored file references.

## Consequences

- The kernel has no request or response model and no chat-shaped types.
  A plugin written for a kind works on all its formats.
- Code outside the core builds these types with constructors and matches
  them with a wildcard arm, so each promise lands without a break.
