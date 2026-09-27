# 0005. No routes: formats recognise requests, plugins are global

- Status: accepted
- Date: 2026-09-26
- Amends: [0003](0003-kernel-contracts.md),
  [0004](0004-two-edges-neutral-events.md)

## Context

A route bundled a path, a format, a plugin chain and a target. Paths are
the adapter's business, a format already knows its own protocol, and one
chain per route let a guardrail run on one endpoint and not the next, the
bug of use case 3. Plugins shared between routes were registered once per
route, in orders that could differ.

## Decision

### Formats recognise their requests

- `Format::accepts(&self, head: &http::request::Parts) -> bool`, from the
  head alone, never the body. It is required: a format given only to
  providers is never asked and may answer `false`.
- `Gateway::serve(format)` adds a format clients speak. A request goes to
  the first format served that accepts it; none gives the kernel's plain
  404. First-party formats match method and path suffix: OpenAI Chat a
  `POST` to `.../chat/completions`, Anthropic Messages to
  `.../v1/messages`; `formats::posts_to` does this for any format, of a
  kind or of none.
- `Gateway::handle(req)` takes no name. The HTTP adapter sends every path
  to it, bounding the body as before.
- `Context::format()` names the client's format.

### One plugin registry

- `Gateway::plugin(name, plugin)` registers a plugin under a unique name;
  `Gateway::after(name, before)` makes it run after each of `before`.
- The kernel keeps one order, used on both edges, built one plugin at a
  time: the next is the earliest registered of those whose `after`
  plugins have all run. Registered `a`, `b`, `c` with `a` after `c`, they
  run `b`, `c`, `a`. A plugin's filters wrap those of the plugins after
  it.
- Execution is sequential in that order, as before; `on_provider` and the
  end hooks use it too.
- Every plugin sees all traffic. For another kind the typed accessors
  (`input`, `input_mut`, `filter`, `Call::input`) are an `Err` with
  `WRONG_KIND`, which `?` turns into a refusal, so a plugin fails closed;
  one that lets other traffic pass matches that code and returns
  `Ok(None)`. The registry has no kind filter.

### The default target

- `Gateway::target(provider)` sets the provider that answers when no
  plugin set one. Routers (`ModelRouter`, `cx.set_target`) override it
  per request. With none, the request is refused 404.
- Whether a request's format can use a target, the default one included,
  is decided per request: another kind is a `CONFIG` refusal when called.
  `check_target` checks that the gateway has the provider.

### Building

- `Gateway::new(providers)` and the settings above are by-value methods
  returning `Result<Gateway, Fault>`, as 0003 asks. Each fails with
  `CONFIG` as it is given: a duplicate provider or plugin name, a format
  whose kind breaks a rule, an unknown plugin or provider, or an `after`
  that closes a cycle.

  ```rust
  let gw = Gateway::new(providers)?
      .serve(OpenAiChat)?
      .serve(AnthropicMessages)?
      .plugin("guard", NoSecrets)?
      .plugin("pii", Pii)?
      .after("pii", ["guard"])?
      .target("openai")?;
  ```

- Errors surface per call, not at a final step: each `?` names the
  setting at fault, there is no step to forget, and a `Gateway` value is
  always whole. `after` names both plugins, given once both are
  registered, so a cycle is caught when it closes and needs no final
  check.

### Per-endpoint differences

- These belong to the adapter or embedder: it marks the request, such as
  with an extension or a header, and global plugins read the mark. The
  kernel has no feature for it.

### Promised

Each lands additively; names are chosen when it comes.

- Plugins attached to a provider: provider-edge hooks run only for that
  provider's calls.
- Required plugins declared on the registry with the input types they
  cover; the kernel refuses what they cannot cover (0003).
- Parallel execution of independent branches of the order, for plugins
  that only read.

## Consequences

- `Route` is gone; the core exports 22 public types.
- A binary's config lists the formats served, a global `plugins:` list
  with names and `after`, and a default `target`.
- A plugin for one kind on a gateway serving several says how it treats
  the others.
