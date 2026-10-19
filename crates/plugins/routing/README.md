# respan-gateway-plugin-routing

`Routing` targets each request at a provider by `Routes`: providers, each
one core provider per format it `Speaks`, `Rule`s and a default target. Of the providers serving the request's kind, it picks
the candidates of a rule named exactly its model, else the provider its
model names as `provider/model` (sent as `model`), else the candidates of
the first glob rule (`llama-*`) that matches, else the provider whose listing
names it (in the `Catalog`; of several, the first added), else the target
if it serves the kind, else the first that does; then that provider's core
provider of the format it sends the model in (`Routes::format_for`, by the
request's `Ask` and the model's card), else of the client's format, else
its first of the kind: the formats of a kind read and write the same
neutral types, so any of them answers a client of another. A rule that
does not apply to the request's kind or client, or none of whose
candidates serve the kind, is skipped. Each format is given its kind, such
as `"chat"`, and a `Model`, such as
`Model::of(Input::model, Input::set_model)`, or `Model::new(read, write)`
for a model named elsewhere, such as a session's handshake; a request of
another format keeps its target.

A `Rule` routes a model name, or a glob of them, to `Candidate`s
(`provider`, `provider/model` or `provider/{model}`, `{model}` the name
asked for), ordered by its `Pick`: `First`, `Cheapest` (by each model's
`Price` in the routes' `Catalog`, input plus three times output), `Fastest` (by the router's own
`Stats` of each candidate's time to a response; unknown or stale ones are
tried), `Weighted` or `Random`. Its `needs` passes over candidates whose
model is known to lack one (the SDK's `Model::lacks`); one the catalog says nothing of is
kept. The `Catalog` is the SDK's, shared with the host
(`Routes::with_catalog`, `Routes::catalog`). A
candidate that failed its last 3 calls within 30 s goes last. The router
records `Stats` itself, from each call's `Attempt::head` and outcome.
`Routes::plan(&Ask)` says where a request goes (`Planned`: the `Chosen`
candidates, why as `By`, and those passed over).

```rust
use std::sync::Arc;
use respan_gateway_conversation::Input;
use respan_gateway_plugin_routing::{
    Candidate, Dispatch, Model, Pick, Routes, Routing, Rule, Speaks,
};

const CHAT: &str = "chat";
let speaks = |format: &str, internal: &str| Speaks {
    format: format.into(),
    kind: CHAT,
    internal: internal.into(),
};
let mut smart = Rule::new(
    "smart",
    vec![Candidate::parse("anthropic/claude-opus-4-1"), Candidate::parse("openai/gpt-5")],
);
smart.pick = Pick::Cheapest;
let routes = Routes::new()
    .provider("openai", vec![speaks("openai-chat", "openai")])
    .provider("anthropic", vec![speaks("anthropic-messages", "anthropic")])
    .rule(smart)
    .target("openai");
let routing = Routing::new(Arc::new(routes))
    .format("openai-chat", CHAT, Model::of(Input::model, Input::set_model));
let dispatch = Dispatch::new(|_, _| true);
```

`Dispatch` sends a rule-routed request to its candidates as the rule says:
each in turn until one succeeds, retried as its `backoff` allows
(`first_success`), or raced when it has a `hedge` (`race`); a request no rule
routed, or whose target a later plugin changed, is left alone. It answers
the request itself, so register it last, after every plugin that targets,
edits or answers the request. A request a plugin before it answers, as the
tool loop does a chat request it runs tools for, goes to the rule's first
candidate alone, with no fallback, retry or hedge.

Plugins compose in registration order: `Routing` keeps a target a plugin
before it set. A gateway's own default target is not a plugin's choice;
tell `Routing` of it with `.over(provider)`. `.client(read)` names a
request's client key, for rules of some clients alone. A request whose
input is not the type its `Model::of` reads is refused, not sent to the
default.

See also [`fallback`](../fallback/README.md) for the retry and race helpers
`Dispatch` is built on.
