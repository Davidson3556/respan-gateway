# respan-gateway-sdk

What plugins, vendors and hosts share beyond the core. A leaf: it depends on
no other crate of the gateway's, only `chrono` and `serde`.

- `Model`: a model's card, the one shape of what is known of a model: its
  id and provider, kind, name and description, limits, what it reads
  (`Modality`) and can do (`Capability`), its `Price`, and when it came and
  goes. A vendor fills it from its list; it serializes as a `/v1/models`
  entry (`created` in seconds, `retires_at` in RFC 3339, `pricing` with who
  says it), and deserializes as a host declares one. `overlay` lays a
  declaration over a list's card; `lacks(need)` reads a route's need by any
  vendor's name of it.
- `Price`: US dollars per million input and output tokens, cached input at
  its own rate, and who says it (`PriceFrom`: the config or the provider);
  `charge(&Tokens)` and `weight()` (input + 3 × output, to rank by).
  Deserialized, as a host declares it, it is the host's (`from = config`).
- `Catalog`: the models a host knows, by provider and id, shared by whoever
  reads them (the router, the pricing plugin, `/v1/models`): `set` a
  provider's models, `get` one. The gateway sets each provider's list with
  the config's declared models laid over it (`Model::overlay`), and those
  it declares that the list does not name.
- `Caller`: who a request is for, put in its extensions by the host: an
  opaque, stable `id` that scopes what is kept for it (MCP sessions, stored
  responses), and a `name` to show (a request log, Respan's customer).
