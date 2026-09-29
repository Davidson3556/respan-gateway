# 0005. No routes: formats recognise requests, plugins are global

- Status: accepted, amended by [0008](0008-events-carry-their-bytes.md), [0009](0009-core-cleanups.md), [0012](0012-http-out-of-the-kernel.md), [0013](0013-no-http-in-core.md)
- Date: 2026-09-26

## Context

A route would bundle a path, a format, a plugin chain and a target. Paths
are the adapter's business, and a format already knows its protocol. One
chain per route lets a guardrail run on one endpoint and not the next
(use case 3), and a shared plugin would be registered once per route.

## Decision

### Formats recognise their requests

- `Format::accepts(&self, head)` decides from the head alone.
- `Gateway::serve(format)` adds a format clients speak; a request goes to
  the first on its wire that accepts it. One none accepts is an `Err` from
  `handle`, which the adapter writes as a plain 404.
- First-party formats match method and path suffix (`formats::posts_to`):
  OpenAI Chat a `POST` to `.../chat/completions`, Anthropic Messages to
  `.../v1/messages`.
- `Gateway::handle(req, streaming)`: the adapter sends every request to
  it, bounds a whole body, and says whether inputs stream.
  `ClientContext::format()` names the client's format.

### One plugin registry

- `Gateway::plugin(name, plugin)` registers a plugin under a unique name;
  `Gateway::after(name, before)` makes it run after each of `before`.
- One order for both edges: next is the earliest registered plugin whose
  `after` plugins have all run. Registered `a`, `b`, `c` with `a` after
  `c`, they run `b`, `c`, `a`. Hooks run one at a time, end hooks too.
- A plugin's filters wrap those of the plugins after it. A plugin that
  answers skips those after it, so it goes after every plugin that must
  see each request.
- Every plugin sees all traffic. On another kind the typed accessors are
  an `INTERNAL` `Err`, so a plugin fails closed. To pass such traffic:

  ```rust
  if !cx.is_kind::<Conversation>() {
      return Ok(None);
  }
  ```

### The default target

- `Gateway::target(provider)` names the provider called when no plugin
  sets one; given again, it replaces it. Routers override it with
  `cx.set_target`.
- With no target, a request is refused 404 `NOT_FOUND`; with one that
  cannot take it, 500 `CONFIG`, as the call fails.

### Building

- `Gateway::new()` starts empty; each setting is a by-value method
  returning `Result<Gateway, Fault>`, failing `CONFIG` as it is given: a
  duplicate name, a format whose kind or wire breaks a rule, an unknown
  plugin or provider, or an `after` that closes a cycle.

  ```rust
  let gw = Gateway::new()
      .provider("openai", OpenAiChat, HttpTransport::new(url)?)?
      .serve(OpenAiChat)?
      .plugin("guard", NoSecrets)?
      .plugin("pii", Pii)?
      .after("pii", ["guard"])?
      .target("openai")?;
  ```

- Per-endpoint differences belong to the adapter or embedder, which marks
  the request (an extension) for plugins to read.

### Promised

- Parallel execution of independent branches of the order, for plugins
  that only read.

## Consequences

- A binary's config lists the formats served, the plugins with `after`,
  and a default `target`.
- A plugin for one kind says how it treats the others.

## History

- 0008: `()` as a format of no kind went; opaque traffic is a kind of one.
- 0009: several default targets became one, as choosing by kind is routing.
- 0012: `handle` takes whether the request streams, and returns `Err` for
  what no format can write.
- 0013: formats are served per wire.
