# 0005. No routes: formats recognise requests, plugins are global

- Status: accepted
- Date: 2026-09-26

## Context

A route would bundle a path, a format, a plugin chain and a target. Paths
are the adapter's business, and a format already knows its own protocol.
One chain per route lets a guardrail run on one endpoint and not the
next, the bug of use case 3. A plugin shared by routes would be
registered once per route, in orders that could differ.

## Decision

### Formats recognise their requests

- `Format::accepts(&self, head: &http::request::Parts) -> bool` decides
  from the head alone, never the body.
- `accepts` is required. A format given only to providers is never asked
  and may answer `false`.
- `Gateway::serve(format)` adds a format clients speak. A request goes to
  the first format served that accepts it. A request none accepts gets
  the kernel's plain-text 404.
- First-party formats match method and path suffix: OpenAI Chat a `POST`
  to `.../chat/completions`, Anthropic Messages a `POST` to
  `.../v1/messages`. `formats::posts_to` does this for any format.
- `Gateway::handle(req)` takes the request alone. The HTTP adapter sends
  every path to it and bounds the body.
- `Context::format()` names the client's format.

### One plugin registry

- `Gateway::plugin(name, plugin)` registers a plugin under a unique name.
  `Gateway::after(name, before)` makes it run after each of `before`.
- The kernel keeps one order for both edges, built one plugin at a time:
  the next is the earliest registered of those whose `after` plugins
  have all run. Registered `a`, `b`, `c` with `a` after `c`, they run
  `b`, `c`, `a`.
- Hooks run one at a time in that order, end hooks too.
- A plugin's filters wrap those of the plugins after it.
- Every plugin sees all traffic. On another kind the typed accessors
  (`input`, `input_mut`, `filter`, `Call::input`) are an `Err` with
  `WRONG_KIND`, which `?` turns into a refusal, so a plugin fails closed.
  A plugin that lets other traffic pass matches that code and returns
  `Ok(None)`.
- The registry has no kind filter.

### The default target

- `Gateway::target(provider)` sets the provider that answers when no
  plugin sets one. Routers override it per request with `cx.set_target`.
  With no target, the request is refused 404.
- Whether a request can use a target is decided when it is called
  ([0003](0003-kernel-contracts.md)).
  `Gateway::check_target` checks that the gateway has the provider.

### Building

- `Gateway::new(providers)` and the settings above are by-value methods
  returning `Result<Gateway, Fault>`.
- Each fails with `CONFIG` as it is given: a duplicate provider or plugin
  name, a format whose kind breaks a rule, an unknown plugin or provider,
  or an `after` that closes a cycle.

  ```rust
  let gw = Gateway::new(providers)?
      .serve(OpenAiChat)?
      .serve(AnthropicMessages)?
      .plugin("guard", NoSecrets)?
      .plugin("pii", Pii)?
      .after("pii", ["guard"])?
      .target("openai")?;
  ```

- Each `?` names the setting at fault, there is no final step to forget,
  and a `Gateway` value is always whole.
- `after` is given once both plugins are registered, so a cycle is
  caught when it closes.

### Per-endpoint differences

- These belong to the adapter or embedder. It marks the request, such as
  with an extension or a header, and plugins read the mark. The kernel
  has no feature for it.

### Promised

Each lands additively. Names are chosen when it comes.

- Plugins attached to a provider, whose provider-edge hooks run only for
  that provider's calls.
- Parallel execution of independent branches of the order, for plugins
  that only read.

Required plugins are promised in 0003.

## Consequences

- The core exports 22 public types.
- A binary's config lists the formats served, a `plugins:` list with
  names and `after`, and a default `target`.
- A plugin for one kind, on a gateway serving several, says how it treats
  the others.
