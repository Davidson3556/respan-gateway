# respan-gateway-plugin-routing

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
use respan_gateway_plugin_routing::{Kind, Model, Routes, Routing, Speaks};

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

See also [`fallback`](../fallback/README.md) for retrying and racing the attempts a routed request makes.
