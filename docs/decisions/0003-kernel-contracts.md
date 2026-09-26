# 0003. Kernel contracts: minimal, typed by kind, grown without breaking

- Status: accepted
- Date: 2026-09-25

## Context

The [use cases](../use-cases.md) need far more than a first release:
sessions, per-call hooks, budgets, retention, work that outlives its
request. Adding it must not break plugins or formats written today, or
change what they see. A chat model in the kernel fits neither MCP, images,
video nor users' own protocols; translators per pair of formats grow with
the square of the formats and give plugins no typed view.

## Decision

### A minimal kernel

- Two hooks, `on_request(&mut Context)` and `on_close(&Context, &Outcome)`,
  and one pipeline for the client's response. A new hook needs a use case
  and a test.
- `on_request` changes the request through its kind's type, its model, its
  target and typed extensions, never its bytes. Edits are for message
  streams: responses, and a session's frames when sessions come.
- `on_close` runs once per request with one `Outcome`; an embedder gets the
  same `Outcome` as `Closed` in the response's extensions.
- Identity and HTTP details belong to plugins and adapters.

### Kinds

- A kind is the pair of types its formats share: a request type and the
  event type responses decode into. Anyone defines kinds, outside the
  kernel, as few as the traffic needs.
- `Format::kind` returns `Kind::new::<Request, Event>(decode, encode)`.
  Formats are of one kind when both types match; a kind's types are its
  own, never shared ones like `serde_json::Value`. Formats of one name are
  one protocol, so one kind.
- Plugins ask by type (`cx.request_mut::<Conversation>()`,
  `cx.filter(|e: &mut Event| ..)`); on a route of another kind that is an
  `Err`.
- An unchanged request reaches a provider of its own format as its bytes; a
  changed one patches the client's body; another format of the kind gets it
  encoded anew. A target of another kind is refused at build, any provider
  of another kind at call time.
- Decoding refuses only a body that is not the format. What the type cannot
  hold is relayed within one format, and refused on translation unless it
  cannot change the answer.
- Types are copied and compared to detect changes, so large payloads sit in
  an `Arc` of an `Eq` type. The kernel holds the model; the rest is in the
  kind's type.

### Growing without breaking

- Growing data types are `#[non_exhaustive]` or have private fields; types
  plugins build have constructors.
- New behaviour comes as trait methods with defaults, `Context` methods, or
  entry points beside `Gateway::handle`. The `Arc` impls of `Format`,
  `Plugin` and `Transport` forward every method, as any wrapper must.
- A plugin without a hook added later covers nothing that hook sees; the
  kernel refuses what a required plugin's missing hook would miss. With the
  first such hook comes a method, taking `self: Arc<Self>`, that lists a
  plugin's hooks as objects; its default gives today's two.
- `Kind::new` and `Gateway::new` keep their signatures. More kind types
  come as methods beside `Kind::new`; gateway-wide settings (required
  plugins, the drain, the store) as by-value methods beside `Gateway::new`,
  given before the gateway is cloned or used and failing the build as it
  does.
- A Part added later comes with an `Encoder` method whose default refuses
  it. How a response ends (`Result<(), Fault>`) and whether its bytes are
  the client format's own (`bool`) never grow.
- Content outside the hooked request and response, such as a session's
  turns, will reach only plugins that opt in; for the others the content
  accessors are an `Err`, so a guardrail refuses what it cannot see.
- Safety never relies on chain order: plugins after one that responds never
  run, so a router sets the target rather than calling it.
- Response edits apply the last added first, so a plugin's edit wraps those
  added after it.
- Providers are reached only through the `Context`, so whatever later
  governs calls covers every call.

### Promised

Each lands additively; names are chosen when it comes.

**Calls**

- A call with its own `http::Request`, of any kind, format and provider.
- A per-call hook, known when the route is built. It runs for every call
  the request makes, whoever makes it, even before `on_request`, and can:
  - wait, such as for capacity, refuse before anything is sent, and add to
    the call's facts;
  - change its own copies of the typed request, before encoding, and of the
    final `http::Request<Body>`, before sending, such as to sign it; a
    streamed body stays streamed;
  - add edits to that call's response, run before the route's;
  - call providers within the request's scope; those calls run every other
    plugin's per-call hook and carry the depth limit.

  A plugin whose per-call hook ran gets `on_close`, as does every
  gateway-required plugin, after the chain's.
- Targets given by value, such as a provider built for one tenant, checked
  when called as `Gateway::new` checks a provider, and for kind.
- Reading a response's Parts, usage included, before committing or to the
  end; Parts read before committing are peeked, not taken.
- An owned call handle, inheriting the request's scope, so an async stream
  filter, a trait beside `Filter` whose runs are hook runs, can call models
  mid-stream.
- A call still running at close is cancelled and recorded as dropped,
  unless it becomes an operation record; its filters get their `end`.
- A call, or a turn, that ends without reported usage, truncated ones too,
  gets usage estimated by its filters through a default `Filter` method,
  each plugin's kept apart, or by the read-only tap on a route of no kind.
  A budget plugin adds its filter in the per-call hook, so an unread hedge
  loser is estimated too.
- Every call's facts in the `Outcome`, dropped ones too, with a role the
  caller sets on the `Call`, such as a hedge's loser or a shadow; only the
  kernel marks the committed one. Billing is the calls' facts plus usage no
  call produced (`with_usage`). Usage told to the client that calls already
  count, such as a merged batch's, is given by a method beside `with_usage`
  and not billed again. `Outcome.usage` keeps its meaning.
- A call never runs another route's chain; a guarded subrequest is an entry
  point of its own, within the scope and the depth limit.

**Scope**

- An opaque value on the `Context`, such as a tenant, that its calls
  inherit and state keys on, carrying the trace context.
  - Fixed before the chain runs, with the key it is stored under, by the
    caller of `handle` or an async resolver beside `Gateway::new`. The
    resolver sees the head, or a record's key, may refuse in the client's
    format, and runs under the admission deadline, which a scope may lower,
    never raise.
  - No plugin can change it: the kernel takes it out of the extensions.
  - Tenant policy, such as keys, routes and limits, comes through it from a
    versioned source outside the `Gateway`'s snapshot.
- Retention per scope can make every accessor that yields body bytes or
  events an `Err` for plugins not opted in, the per-call hook's and the
  tap's included.
  - A fault's message, details and source are content: such a plugin gets
    status and code only. Signing a body counts as reading it.
  - A policy may count the URI as content; a plugin not opted in then sees
    it redacted.
  - Every content store keys on the scope; what an opted-in plugin copies
    out is its own.
  - Each hook runs with its own plugin's rights; one that runs while
    another holds the `Context`, such as the per-call hook, gets a view of
    its own.

**Operation records**

- Work that outlives its request, such as a video job or a shadow call that
  must finish, closes as an operation record with an id billing keys on,
  never by delaying `on_close`. A later request, such as a poll or a
  webhook, settles it by that id through the store. Opening one needs a
  resolver that takes a stored key.
- A settle hook, with a default beside `on_close`, gives its `Outcome` to
  the opener, every plugin whose per-call hook ran on its calls, and the
  gateway-required plugins covering it, matched by keys from their
  configuration that the record stores, on whichever gateway settles it.
- The settle hook runs after the settling response, like `on_close`, under
  the after-end budget rule, and under the record's scope re-resolved from
  its key. Told it resolves to settle, the resolver returns a scope that
  refuses new work rather than refusing, so a revoked tenant still settles.
- Its calls are requests of their own under that scope, with a fresh
  deadline, within the depth limit, running the record's route's per-call
  hooks and the gateway-required plugins'.
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
    of the calls it makes reach the plugins in a further run, a chain that
    stops at the depth limit;
  - a later settle, or any gateway sharing the store, delivers pending
    runs; the record is removed once none is.
- At the TTL, pending runs are delivered with no request and their calls
  refused; a record never settled is delivered with an `Outcome` ending
  `Err` and its calls' usage, and the budget plugin charges its
  reservations rather than releasing them. The record is then removed.

**Budgets and time**

- Reservations, made in the per-call hook and as a turn starts, atomic
  across nested ledgers through the store, settled idempotently on a call's,
  an operation's or a turn's id. One on a record's operation or calls lasts
  until the record's settlement charges it; any other expires, released,
  after a TTL its plugin sets.
- A required budget plugin refuses a request with no cost bound, such as one
  on a route of no kind.
- An admission deadline is set with the scope, a session's anew each turn.
  A hook parks through a `Context` method bounded by it; only parked time
  counts against it.
- Calls are bounded by stall bounds within a turn, set per route and per
  provider, never by the deadline or a hook's budget. A route may have
  none, such as for an MCP stream that idles for hours; an idle bound covers
  a session that sends nothing.
- Time budgets per hook run, enforced at await points on a view of the
  hook's own, merged back only if it returns in time. The clock pauses while
  the hook's calls are pending and its offloaded work waits for a thread.
- A hook past its budget fails as its `Err` would, unless its plugin may be
  skipped: then its changes are dropped and its calls cancelled, their facts
  and reservations still reaching the `Outcome`. A plugin required for the
  scope is never skipped.
- After-end hooks, such as `on_close`, have no `Err`: past its budget a
  skippable plugin's is cancelled and logged, a required one's runs on until
  the drain deadline, which records it as lost. Blocking inline is a bug.

**Bodies**

- Streamed request bodies, passed on untouched, the model read from the
  head or a prefix through a default method. A route streams only when
  every plugin in its chain, required ones too, says through a default
  method that it does not need the body. A streamed body goes once: a later
  call is an `Err`, so a plugin that may call twice says it needs the body.
- A format without the new methods gets the body buffered up to the route's
  body limit, as today.
- Trailers, through default decoder and encoder methods: a relayed
  response's pass on untouched; a translated one refuses them rather than
  drop them. A decoder may consume a trailer that cannot change the answer,
  such as `grpc-status: 0`, or turn it into Parts, usage or an `Err` end.

**Sessions, on the same types**

- One `http::Request<Body>` whose body, and its response's, carry the
  protocol's frames after the handshake. The kernel keeps its `Upgrade`
  headers; the transport returns at the `101` head with both bodies
  streaming. A session provider's base URL is its `https` one.
- An entry point beside `Gateway::handle`, fed the upgraded connection's
  bytes, which a helper crate outside the kernel frames, ending compression
  and subprotocols on each edge so frames can be edited.
- A session format adds a client-direction decoder and a provider-direction
  encoder as default methods, and its kind a client-direction event type of
  its own, declared beside `Kind::new`. Plugins pick a direction by type
  (`filter::<ClientEvent>`); client frames are edited like responses. A
  required plugin's client-direction filter runs after the route's, so it
  sees what the provider gets.
- A turn Part marks which turn the Parts after it belong to, and carries how
  the turn ended. Turns may overlap. A refused turn is an error event and
  the session stays open; only a failed end closes it. A drain ends it with
  a code of its own, which formats write in their close framing; old
  encoders refuse turn Parts by default.
- Filters added at the handshake last the session; filters added in a
  turn's hook last that turn, see only its events and the frames held while
  the hook ran, get their `end` with its usage, and hold per-turn state.
- A turn starts at the first mark from either side; its frames wait while
  its hook runs, outside stall bounds. A turn the provider started that a
  hook refuses is cancelled by the provider-direction encoder and billed
  from the call's facts.
- Two per-turn hooks beside `on_close`: at a turn's start, on
  `&mut Context`, to reserve, park or refuse, run one at a time in turn
  order; at its end, with the turn's `Outcome`, on a view that cannot call
  providers, for every turn that started. Both run for the route's plugins,
  then the gateway-required ones. An embedder gets each turn's `Outcome`
  beside `Closed`.
- A session's scope is re-resolved as each turn starts; a refusal ends the
  session as a drain does.
- The model may come from a prefix of the client's frames, such as a setup
  message, read before `on_request`; a model a plugin sets is written into
  the prefix by the format, and a format that cannot refuses the request, so
  `set_model` stays infallible.
- After the handshake `request_mut` is an `Err`, model and target apply to
  later calls, and head edits are refused. A format may give a turn a call
  of its own, reading the turn's model from its first frame before the
  turn's hook, which may change it; each such call gets its own
  provider-direction encoder.
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
  what settles usage already incurred, such as `on_close`, the settle hook
  and the end of a turn the provider started, still runs within the drain's
  deadline, its calls refused; a call it cancels is billed as dropped.

**Other**

- Plugins required on the gateway, declaring the kinds or formats they
  cover, for every request or per scope; the kernel refuses any request or
  call they cannot cover. One checks each call in the per-call hook after
  the route's plugins, and a plugin's answer before it is committed; its
  filter is added first, so it sees what the client gets.
- Formats writing path or header fields from the typed request.
- Translation between kinds, under a decision of its own.
- A keyed provider's error text, which may name the operator's account,
  scrubbed before a client sees it, through a `Format` method with a
  default.

Everything else the use cases ask of core is new API beside these: the
store; a read-only tap that sees usage as it streams and can fail the
response; per-route body limits, which a scope may lower, never raise;
offloading blocking work; and resolving stored file references.

## Consequences

- The kernel has no request or response model and no chat-shaped types; a
  plugin written for a kind works on all its formats.
- Code outside core builds these types with constructors and matches them
  with a wildcard arm, so each promise lands without a break.
