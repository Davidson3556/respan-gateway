# 0003. Kernel contracts: minimal, typed by kind, grown without breaking

- Status: accepted
- Date: 2026-09-25
- Amended by: [0004](0004-two-edges-neutral-events.md),
  [0005](0005-no-routes-global-plugins.md)

## Context

The [use cases](../use-cases.md) need far more than a first release:
sessions, more of `on_provider`, budgets, retention, work that outlives
its request. Adding it must not break existing plugins or formats, or
change what they see. A chat model in the kernel fits neither MCP, images,
video nor users' own protocols; translators per pair of formats grow with
the square of the formats and give plugins no typed view.

## Decision

### A minimal kernel

- Four hooks, two per edge, and one response pipeline. A new hook needs a
  use case and a test.
- `on_client` changes the head, the input, the model and the target, never
  the body's bytes. Edits are for message streams: responses, and a
  session's frames (Sessions, below).
- `on_client_end` runs once per request with one `Outcome`; an embedder
  gets the same `Outcome` as `Closed` in the response's extensions.
- Identity and HTTP details belong to plugins and adapters.

### Kinds

- A kind is the pair of types its formats share: its input and its
  output, which responses decode into. Anyone defines kinds, outside the
  kernel, as few as the traffic needs.
- A format names them as `Format::Input` and `Format::Output` and decodes
  and encodes its input (`Format::decode`, `Format::encode`); a format of
  no kind names `()` for both. Formats are of one kind when both types
  match; a kind's types are its own, never shared ones like
  `serde_json::Value`. Formats of one name are one protocol, so one kind.
- Plugins ask by type (`cx.input_mut::<Conversation>()`,
  `cx.filter(|r: &mut Reply| ..)`); on traffic of another kind that is an
  `Err`.
- An unchanged input, read or not, reaches a provider of its own format as
  its bytes; a changed one patches the client's body; another format of
  the kind gets it encoded anew. An unknown target is refused at build; a
  provider of another kind, the gateway's target included, when called.
- Decoding refuses only a body that is not the format. What the type cannot
  hold is relayed within one format, and refused on translation unless it
  cannot change the answer.
- Within one format, body fields, the query and the client's end-to-end
  headers survive, less headers that change how the provider reads the
  request or who sent it; across formats only the input does. Whose
  credentials reach a provider is its transport's to decide (0004). The
  gateway's own credentials are removed by the adapter or embedder before
  `handle`, or by a plugin through `Context::head_mut`.
- Types are copied and compared to detect changes, so large payloads sit in
  an `Arc` of an `Eq` type. The kernel holds the model; the rest is in the
  input.

### Growing without breaking

- Growing data types are `#[non_exhaustive]` or have private fields; types
  plugins build have constructors.
- New behaviour comes as trait methods with defaults, context methods, or
  entry points beside `Gateway::handle`. The `Arc` impls of `Format`,
  `Plugin` and `Transport` forward every method, as any wrapper must.
- A plugin without a hook added later covers nothing that hook sees. A
  required plugin declares the hooks it covers, and the kernel refuses what
  it would miss.
- `Gateway::new(providers)` keeps its signature. Gateway-wide settings
  (the formats served, plugins, the target, and later required plugins,
  the drain deadline, the store) come as by-value methods beside it,
  given before the gateway is cloned or used and failing the build as it
  does.
- A Part added later comes with an `Encoder` method whose default refuses
  it. How a response ends (`Result<(), Fault>`) and whether its bytes are
  the client format's own (`bool`) never grow.
- Content outside the hooked request and response, such as a session's
  turns, reaches only plugins that opt in; for the others the content
  accessors are an `Err`, so a guardrail refuses what it cannot see.
- Safety never relies on plugin order: plugins after one that responds
  never run, so a router sets the target rather than calling it.
- Response edits apply the last added first, so a plugin's edit wraps those
  added after it.
- Providers are reached only through either context, so whatever later
  governs calls covers every call.

### Promised

Each lands additively; names are chosen when it comes.

**Calls**

- A call with its own `http::Request`, of any kind, format and provider.
- More of `on_provider`: waiting for capacity, adding to the call's
  facts, and calling providers within the request's scope, which runs
  every other plugin's `on_provider` within the depth limit.
- Changing a call's final `http::Request<Body>` on the provider edge, such
  as to sign it; a streamed body stays streamed. It sees any client
  credentials the format passed on.
- Targets given by value, such as a provider built for one tenant,
  checked for kind when called.
- Reading a response's Parts, usage included, before committing or to the
  end; Parts read before committing are peeked, not taken.
- An owned call handle, inheriting the request's scope, so an async stream
  filter, a trait beside `Filter` whose runs are hook runs, can call models
  mid-stream.
- A call, or a turn, that ends without reported usage, truncated ones
  too, gets its usage estimated by each plugin's `on_provider_end`, from
  that call's own state, or by the read-only tap on traffic of no kind. A
  truncated call that reported some usage is topped up by the estimate. A
  dropped call, such as an unread hedge loser, is settled by its plugin
  from its reservation.
- Each call has an id, readable on the provider edge.
- A call's role, set on the `Call`, such as a hedge's loser or a shadow,
  and whether it was committed, which only the kernel marks, are readable
  on the provider edge. Billing is each call's usage in its
  `on_provider_end`, plus usage no call produced, such as a cache hit's
  (`with_usage`), given apart in the client edge's `Outcome`. Usage told
  to the client that calls already count, such as a merged batch's, is
  given by a method beside `with_usage` and not billed again.
  `Outcome.usage` keeps its meaning.
- A call runs only the gateway's own plugins; a guarded subrequest is an
  entry point of its own, within the scope and the depth limit.

**Scope**

- An opaque value on the `Context`, such as a tenant, that its calls
  inherit and state keys on, carrying the trace context.
  - Fixed before the plugins run, with the key it is stored under, by the
    caller of `handle` or an async resolver beside `Gateway::new`. The
    resolver sees the head, or a record's key, may refuse in the client's
    format, and runs under the admission deadline, which a scope may lower,
    never raise.
  - No plugin can change it: the kernel takes it out of the extensions.
  - Tenant policy, such as keys, targets and limits, comes through it from a
    versioned source outside the `Gateway`'s snapshot.
- Retention per scope can make every accessor that yields body bytes or
  events an `Err` for plugins not opted in, the provider edge's and the
  tap's included.
  - A fault's message, details and source are content: such a plugin gets
    status and code only. Signing a body counts as reading it.
  - A policy may count the URI as content; a plugin not opted in then sees
    it redacted.
  - Every content store keys on the scope; what an opted-in plugin copies
    out is its own.
  - Each hook runs with its own plugin's rights; one that runs while
    another holds the `Context`, such as `on_provider`, gets a view of its
    own.

**Operation records**

- Work that outlives its request, such as a video job or a shadow call that
  must finish, closes as an operation record with an id billing keys on,
  never by delaying `on_client_end`. A later request, such as a poll or a
  webhook, settles it by that id through the store. Opening one needs a
  resolver that takes a stored key.
- A settle hook, with a default beside `on_client_end`, gives its
  `Outcome` to the opener, every plugin whose `on_provider` ran on its
  calls, and the gateway-required plugins covering it, matched by keys
  from their configuration that the record stores, on whichever gateway
  settles it.
- The settle hook runs after the settling response, like `on_client_end`,
  under the after-end budget rule, and under the record's scope
  re-resolved from its key. Told it resolves to settle, the resolver
  returns a scope that refuses new work rather than refusing, so a revoked
  tenant still settles.
- Its calls are requests of their own under that scope, with a fresh
  deadline, within the depth limit, running the provider hooks of the
  gateway's plugins and the gateway-required plugins'.
- Embedders get settlements through a method beside `Gateway::new`. `idle`
  and the drain wait for in-process work, never for a record awaiting
  settlement.
- Delivery is at least once, with the same facts and ids, so plugins settle
  idempotently:
  - the store claims each run; the claim lapses if its gateway does not
    finish it;
  - a run is pending for a plugin until its hook returns, or is cancelled
    past its budget, on a gateway that has the plugin;
  - refused calls are facts of the run, not a reason to rerun it; the facts
    of the calls it makes reach the plugins in a further run, a series that
    stops at the depth limit;
  - a later settle, or any gateway sharing the store, delivers pending
    runs; the record is removed once none is.
- At the TTL, pending runs are delivered with no request and their calls
  refused; a record never settled is delivered with an `Outcome` ending
  `Err` and its calls' usage, and the budget plugin charges its
  reservations rather than releasing them. The record is then removed.

**Budgets and time**

- Reservations, made in `on_provider`, a turn's too, atomic across nested
  ledgers through the store, settled idempotently on a call's, an
  operation's or a turn's id. One on a record's operation or calls lasts
  until the record's settlement charges it; any other expires, released,
  after a TTL its plugin sets.
- A required budget plugin refuses a request with no cost bound, such as one
  of no kind.
- An admission deadline is set with the scope, a session's anew each turn.
  A hook parks through a method of either context bounded by it; only
  parked time counts against it.
- Calls are bounded by stall bounds within a turn, set per format served
  and per provider, never by the deadline or a hook's budget. A format may
  have none, such as for an MCP stream that idles for hours; an idle bound
  covers a session that sends nothing.
- Time budgets per hook run, enforced at await points on a view of the
  hook's own, merged back only if it returns in time. The clock pauses while
  the hook's calls are pending and its offloaded work waits for a thread.
- A hook past its budget fails as its `Err` would, unless its plugin may be
  skipped: then its changes are dropped and its calls cancelled, their facts
  and reservations still reaching the `Outcome`. A plugin required for the
  scope is never skipped.
- After-end hooks, such as `on_client_end`, have no `Err`: past its budget a
  skippable plugin's is cancelled and logged, a required one's runs on until
  the drain deadline, which records it as lost. Blocking inline is a bug.

**Bodies**

- Streamed request bodies, passed on untouched, the model read from the
  head or a prefix like a body. A request streams only when every plugin,
  required ones too, says through a default method that it does not need
  the body. A streamed body goes once: a later call is an
  `Err`, so a plugin that may call twice says it needs the body.
- A format without the new methods gets the body buffered up to its
  body limit.
- Trailers, through default decoder and encoder methods: a relayed
  response's pass on untouched; a translated one refuses them rather than
  drop them. A decoder may consume a trailer that cannot change the answer,
  such as `grpc-status: 0`, or turn it into Parts, usage or an `Err` end.

**Sessions, on the same types**

- One `http::Request<Body>` whose body, and its response's, carry the
  protocol's frames after the handshake. The kernel keeps its `Upgrade`
  headers; the transport returns at the `101` head with both bodies
  streaming. A session provider's transport dials its `https` URL.
- An entry point beside `Gateway::handle`, fed the upgraded connection's
  bytes, which a helper crate outside the kernel frames, ending compression
  and subprotocols on each edge so frames can be edited.
- One codec per direction: a session format's default method splits the
  client's bytes into one body per frame, and the kernel runs the format's
  `decode` and `encode` on each; a frame is the kind's input, the first
  one included. On a session each body chunk is exactly one frame, in
  both directions. Plugins pick a direction by type: either context's
  `filter::<Input>` edits input frames like responses, the last added
  first; `filter` over the output type is unchanged. Until sessions land,
  `cx.filter::<Input>` is an `Err` with `WRONG_KIND`. A required plugin's
  input filter runs after the others', so it sees what the provider gets.
- A turn Part marks which turn the Parts after it belong to, and carries
  how the turn ended. On the client side, the session splitter's decoder
  pushes a turn Part before the frame that starts a turn. Turns may
  overlap. A refused turn is an error event and
  the session stays open; only a failed end closes it. A drain ends it with
  a code of its own, which formats write in their close framing; old
  encoders refuse turn Parts by default.
- Filters added at the handshake last the session; filters added in a
  turn's `on_provider` last that turn, see only its events and the frames
  held while the hook ran, get their `end` with its usage, and hold
  per-turn state.
- A turn starts at the first mark from either side; its frames wait while
  its `on_provider` runs, outside stall bounds. A turn the provider
  started that a hook refuses is cancelled by a frame the provider's
  format encodes, and billed from the call's facts.
- Each turn runs the provider edge as a call of its own: `on_provider` at
  its start, to reserve, park or refuse, one at a time in turn order, and
  `on_provider_end` with its usage, for every turn that started. Both run
  for the gateway's plugins, then the required ones. An embedder
  gets each turn's `Outcome` beside `Closed`.
- A session's scope is re-resolved as each turn starts; a refusal ends the
  session as a drain does.
- A format whose client speaks first, such as with a setup frame, has
  that first frame read like a body through `model`, `decode` and
  `prepare`, before `on_client`: the gateway accepts the client's upgrade
  before dialing the provider. A model a plugin sets is written into it
  by the format, and a format that cannot refuses the request, so
  `set_model` stays infallible. Otherwise `on_client` sees no first
  input, and inputs are read per turn.
- After the handshake `input_mut` is an `Err`, model and target apply to
  later calls, and head edits are refused. The client's format reads a
  turn's first input before a per-turn start hook on the client edge, so
  `cx.input()` and `cx.model()` work there, and the hook may set that
  turn's model and target. The turn's call's `on_provider` may change the
  model again, and reads the turn's first input through `px.input`. Each
  such call's inputs are encoded by its provider's format.
- `Outcome.usage` is the sum of the turns.
- MCP over streamable HTTP is ordinary requests tied by scope and the store,
  not a session.

**Drain**

- A drain with a deadline beside `Gateway::idle` ends each session between
  turns and any response still open at the deadline, each in its own close
  framing, so a swapped config keeps no revoked key alive for hours. It can
  be aimed at one scope key, a tenant or a credential, ending only that
  scope's work.
- A drain, or a refusal when a scope is re-resolved, ends new work only:
  what settles usage already incurred, such as the end hooks, the settle
  hook and the end of a turn the provider started, still runs within the
  drain's deadline, its calls refused; a call it cancels is billed as
  dropped.

**Other**

- Plugins required on the gateway, declaring the kinds or formats and the
  hooks they cover, for every request or per scope; the kernel refuses any
  request or call they cannot cover. One checks each call in `on_provider`
  after the other plugins, and a plugin's answer before it is
  committed; its filter is added first, so it sees what the client gets.
  It gets both edges' end hooks, after the others'. A guardrail covering
  every call does it on the provider edge.
- Formats writing path or header fields from the input.
- Translation between kinds, under a decision of its own.
- The error text of a provider with the operator's key, which may name
  the operator's account, scrubbed before a client sees it, through a
  `Format` method with a default.

Everything else the use cases ask of core is new API beside these: the
store; a read-only tap that sees usage as it streams and can fail the
response; body limits per format served, which a scope may lower, never
raise; offloading blocking work; and resolving stored file references.

## Consequences

- The kernel has no request or response model and no chat-shaped types; a
  plugin written for a kind works on all its formats.
- Code outside core builds these types with constructors and matches them
  with a wildcard arm, so each promise lands without a break.
