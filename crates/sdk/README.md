# respan-gateway-sdk

What plugins, vendors and hosts share beyond the core. A leaf: it depends on
no other crate of the gateway's, only `chrono`, `parking_lot` and `serde`.

- `Model`: a model's card, the one shape of what is known of a model: its
  id and provider, kind, name and description, limits, what it reads
  (`Modality`) and can do (`Capability`), its `Price`, and when it came and
  goes. A vendor fills it from its list; it serializes as a `/v1/models`
  entry (`created` in seconds, `retires_at` in RFC 3339, its `Price` as
  `pricing`) and deserializes as a host declares one. `overlay` lays a
  declaration over a list's card; `lacks(need)` reads a route's need by any
  vendor's name of it.
- `Price`: US dollars per million input and output tokens, cache reads and
  writes at their own rates, and per second of audio for a model billed by
  its length; `weight()` (input + 3 × output, to rank by). What a call's
  usage cost is the pricing plugin's `charge`.
- `Catalog`: the models a host knows, by provider and id, shared by whoever
  reads them (the router, the pricing plugin, `/v1/models`): `set` a
  provider's models, `get` one. The gateway sets each provider's list with
  the config's declared models laid over it (`Model::overlay`), and those
  it declares that the list does not name.
- `Ask` / `FormatFor`: what a router knows of a request, and how a vendor
  whose formats each serve only some of its models picks the one a call is
  sent in.
