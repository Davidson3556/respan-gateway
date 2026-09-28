# 0005. No routes: formats recognise requests, plugins are global

- Status: accepted, amended by [0008](0008-events-carry-their-bytes.md), [0009](0009-core-cleanups.md), [0012](0012-http-out-of-the-kernel.md)
- Date: 2026-09-26

## Context

A route would bundle a path, a format, a plugin chain and a target. Paths
are the adapter's business, and a format already knows its own protocol.
One chain per route lets a guardrail run on one endpoint and not the
next, the bug of use case 3, and a plugin shared by routes would be
registered once per route, in orders that could differ.

## Decision

### Formats recognise their requests

- `Format::accepts(&self, head)` decides from the head alone. A format
  given only to providers is never asked.
- `Gateway::serve(format)` adds a format clients speak; a request goes to
  the first that accepts it, and one none accepts gets a plain-text 404.
  *Amended by 0012:* `handle` returns it `Err`, which the adapter writes
  as plain text.
- First-party formats match method and path suffix (`formats::posts_to`):
  OpenAI Chat a `POST` to `.../chat/completions`, Anthropic Messages to
  `.../v1/messages`.
- `Gateway::handle(req)` takes the request alone; the adapter sends every
  path to it and bounds the body. *Superseded by 0012:*
  `handle(req, streaming)`, the adapter saying whether inputs stream. `ClientContext::format()` names the
  client's format.

### One plugin registry

- `Gateway::plugin(name, plugin)` registers a plugin under a unique name;
  `Gateway::after(name, before)` makes it run after each of `before`.
- One order for both edges: next is the earliest registered plugin whose
  `after` plugins have all run. Registered `a`, `b`, `c` with `a` after
  `c`, they run `b`, `c`, `a`. Hooks run one at a time in it, end hooks
  too.
- A plugin's filters wrap those of the plugins after it. A plugin that
  answers skips those after it, so it goes after every plugin that must
  see each request.
- Every plugin sees all traffic. On another kind the typed accessors
  (`input`, `take_inputs`, `filter`) are an `INTERNAL` `Err`, which `?`
  turns into a refusal, so a plugin fails closed. To pass such traffic, a
  plugin asks first; the registry has no kind filter, as this is one line:

  ```rust
  if !cx.is_kind::<Conversation>() {
      return Ok(None);
  }
  ```

### The default target

- `Gateway::target(provider)` names the provider called when no plugin
  sets one; given again, it replaces it. Routers override it with
  `cx.set_target`, routing by format or kind included.
- With no target, a request is refused 404 `NOT_FOUND`; with one that
  cannot take it, 500 `CONFIG`, as the call fails.
- Whether a request can use a provider is decided when it is called;
  `target` checks only that the provider exists.

### Building

- `Gateway::new()` starts empty; each setting is a by-value method
  returning `Result<Gateway, Fault>`, failing `CONFIG` as it is given: a
  duplicate provider or plugin name, a format whose kind breaks a rule,
  an unknown plugin or provider, or an `after` that closes a cycle.

  ```rust
  let gw = Gateway::new()
      .provider("openai", OpenAiChat, HttpTransport::new(url)?)?
      .serve(OpenAiChat)?
      .plugin("guard", NoSecrets)?
      .plugin("pii", Pii)?
      .after("pii", ["guard"])?
      .target("openai")?;
  ```

- Each `?` names the setting at fault, there is no final step to forget,
  and a `Gateway` value is always complete.
- Per-endpoint differences belong to the adapter or embedder, which marks
  the request (an extension or a header) for plugins to read.

### Promised

- Parallel execution of independent branches of the order, for plugins
  that only read.

## Consequences

- A binary's config lists the formats served, a `plugins:` list with names
  and `after`, and a default `target`.
- A plugin for one kind, on a gateway serving several, says how it treats
  the others.

## History

- 0008: `()` as the kind of a format of no kind, `is_kind::<()>`, the
  check that such a format has a provider, and usage filters working for
  every format went; opaque traffic is a kind of one format.
- 0009: several default targets, the first a request can use, became
  one, as choosing by kind is routing.
- 0012: `handle` takes whether the request streams, and hands back what no
  format can write.
