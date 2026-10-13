# Vendor kit

`respan-gateway-vendor-kit` is a vendor as a template, and a provider of it. The
vendors themselves are [`respan-gateway-vendors`](../vendors/README.md)'.

- `Vendor`: where a vendor is (`base_url`), where its key comes from
  (`key_env`), the formats it speaks as it speaks them (`Speaks`: the
  header its key goes in, where it sits beside the base URL), what its
  model list puts before an id, and its quirks: each described (`Quirk`),
  and the plugin that handles them.
- `VendorProvider`: a provider of a vendor (`Vendor::provider`), or of
  formats of no vendor (`VendorProvider::new`), at a base URL with a key.
  `register` adds a core provider of each of its formats, each at its base
  URL, taking the key as the vendor does, over HTTP or, for a session,
  WebSocket, and carrying the vendor's quirk plugin, which runs on its calls
  alone, after the gateway's plugins. `models` lists its models.

A format is a spec and names no vendor; a vendor's departures from a spec
are its own, here.
