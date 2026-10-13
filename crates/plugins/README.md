# First-party plugins

Each plugin is a crate of its own, built on the public core APIs. A gateway
registers the ones it wants; they compose in registration order.

| Crate | Package | What it does |
|---|---|---|
| [`routing`](routing/README.md) | `respan-gateway-plugin-routing` | `Routing` targets each request at a provider by its model |
| [`fallback`](fallback/README.md) | `respan-gateway-plugin-fallback` | `Fallback` sends a request to its target, then down the target's chain until one succeeds; also the retry and race helpers it is built on |
| [`tool-loop`](tool-loop/README.md) | `respan-gateway-plugin-tool-loop` | `ToolLoop` runs tools inside chat requests, for every chat format; `ToolFilter` and `ToolScope` say which tools are offered |
| [`credentials`](credentials/README.md) | `respan-gateway-plugin-credentials` | `ClientCredentials` sends a client's own key to the named providers that have none, of the client's format |
| [`mcp-aggregate`](mcp-aggregate/README.md) | `respan-gateway-plugin-mcp-aggregate` | `Aggregate` fronts many MCP servers as one at `/mcp` |

Billing is application policy, not a supported plugin export. See
[the billing examples](../../examples/README.md#billing-policies) for pricing,
conservative per-attempt accounting, nested wallets and per-turn reservations.
Their support modules are private to the examples, not a replacement billing
crate. Provider usage is the conversation kind's typed `Usage`.
