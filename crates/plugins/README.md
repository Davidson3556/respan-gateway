# respan-gateway-plugins

`Routing` targets each request at a provider by `Routes`: providers, each
one core provider per format it `Speaks`, `models` tables, claimed model
prefixes and a default target. Of the providers serving the request's
kind, it picks the one its model names as `provider/model` (sent as
`model`), else a `models` table's (the last added first), else the longest
claim's, else the target if it serves the kind, else the first that does;
then that provider's core provider of the client's format, else, for a
`Kind` that translates, its first of the kind. Each format is given its
`Kind` and a `Model`, such as `Model::of(Input::model, Input::set_model)`,
or `Model::new(read, write)` for a model named elsewhere, such as a
session's handshake; a request of another format keeps its target.

```rust
use std::sync::Arc;
use respan_gateway_conversation::Input;
use respan_gateway_plugins::{Kind, Model, Routes, Routing, Speaks};

const CHAT: Kind = Kind { label: "chat", translates: true };
let speaks = |format: &str, internal: &str| Speaks {
    format: format.into(),
    kind: CHAT,
    internal: internal.into(),
};
let routes = Routes::new()
    .provider("openai", vec![speaks("openai-chat", "openai")])
    .provider("anthropic", vec![speaks("anthropic-messages", "anthropic")])
    .claim("claude", "anthropic")
    .target("openai");
let routing = Routing::new(Arc::new(routes))
    .format("openai-chat", CHAT, Model::of(Input::model, Input::set_model));
```

Plugins compose in registration order: `Routing` keeps a target a plugin
before it set. A gateway's own default target is not a plugin's choice;
tell `Routing` of it with `.over(provider)`. A request whose input is not
the type its `Model::of` reads is refused, not sent to the default.

The public `routing` module contains transport-neutral attempt orchestration: sequential retry and
fallback, a staggered race, and each attempt's role. Each answer is read
`Until` a stopping point before it is judged: its `Head`, a plugin-chosen
event (`Until::content(predicate)`), or its `End` (`Response::until_end`,
held whole). A success counts only once read that far; nothing is retried
after commitment.

- `first_success(calls, &Backoff, Until, should_retry)`: each call in turn,
  retried only when the caller's classifier permits and
  `Backoff::new(tries, base, max)` allows. The classifier sees every failure
  and a `response_opened` bool: true only if `call.await` returned a response
  before applying `Until`, not when a request was sent, refused, or bytes
  arrived. A false result skips retries of that target, not fallback targets.
  A response's `Retry-After` delay is waited for as asked, but a delay over
  `max` means no retry; a failure before opening supplies no delay.
  `Until::Head` counts a provider's refusal (such as a 429 or 503 answer) as
  a failure. With live (streaming) inputs, `Until::content` and `Until::End`
  cannot read the response and commit as `Head` does.
- `race(calls, stagger, Until)`: start each call `stagger` after the last;
  the first success wins.
- `Attempt` (extension on each call): its `index()` and `Role`.

For HTTP, explicitly pass `respan_gateway_http::retryable` as the classifier.
HTTP's `watchdog(transport, head, idle)` lives in the HTTP protocol module; it bounds a
transport's head wait and polled per-frame body gaps. Other transports can
choose a different classifier without importing HTTP.

## Agent loop

`agent::Agent` runs tools inside chat requests, for every chat format and
provider, as a plugin over the conversation kind. A `Tools` runner opens a
`Toolbox` per request (`list` the tools to offer, `call` one by name with
JSON arguments, answering an `Outcome` whose failure the model reads). The
agent adds the tools to the request, calls the target, and while the reply
calls only those tools runs them (at most `parallel` at once, 4 by default,
and their answers within `max_results` bytes in all) and calls again; the client gets
one reply with the usage summed. It stops at a reply with no such call, at a
call of a client's own tool (returned to the client; the gateway's calls in it
are left out), at `max_steps` (the last call forbids tools) and at `timeout`.
Register it after the plugins that route and edit requests, since it answers
the request itself:

```rust,ignore
let agent = Agent::new(MyTools)       // impl Tools
    .max_steps(8)
    .tool_timeout(Duration::from_secs(60))
    .when(|cx, input| input.model().is_some_and(|m| m.starts_with("gpt")))
    .observe(|cx| Some(sink_for(cx))); // each step and tool call, reported
let gateway = gateway.plugin(Routing::new(routes)).plugin(agent);
```

Each model call carries `agent::Step(n)` in its extensions; a runner that
calls tools through the gateway marks those calls with `agent::ToolHop` so
other plugins can tell them apart. `Agent::flag_errors_for` says which
providers can be sent a result flagged as a failure (others get `Error: `
text). What the model said goes back in the next step as the kind holds it,
reasoning and native fields included (a signed Anthropic `thinking` block, a
Gemini `thoughtSignature`); native events of later steps are renumbered with
their blocks. The timeout fails the request with `agent_timeout`, a 504.
Used by the binary's `agent:` section, with MCP servers as the tools
([tests](tests/agent.rs)).

Used by: [fallback](../../examples/routing/fallback.rs),
[attachments](../../examples/integrations/attachments.rs).

Billing is application policy, not a supported plugin export. See
[the billing examples](../../examples/README.md#billing-policies) for pricing,
conservative per-attempt accounting, nested wallets and per-turn reservations.
Their support modules are private to the examples, not a replacement billing
crate. Provider usage is the conversation kind's typed `Usage`.
