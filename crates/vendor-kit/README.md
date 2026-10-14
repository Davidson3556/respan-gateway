# Vendor kit

`respan-gateway-vendor-kit` is a vendor as a template, and a provider of it. The
vendors themselves are [`respan-gateway-vendors`](../vendors/README.md)'.

- `Vendor`: where a vendor is (`base_url`, where a `{NAME}` is that
  environment variable), where its key comes from (`key_env`), the formats
  it speaks as it speaks them, how it lists its models if not as OpenAI
  does (`models`), and its quirks: each described (`Quirk`), and the plugin
  that handles them. What it does not say is `Vendor::DEFAULT`'s.
- `Speaks`: any core `Format` as a vendor speaks it, over HTTP
  (`Speaks::http`, `.follow` for redirects it may follow) or as a WebSocket
  session (`Speaks::ws`), the header its key goes in (`.key_in`) and where
  it sits beside the base URL (`.at`). A vendor lists each as a `Wire`,
  `&Speaks::http(OpenAiChat)`, its type erased there; the kit depends on no
  format.
- `Either`: one format, or another of the same kind for the calls whose
  input the first refuses, chosen per call from the input it sends, whoever
  sent it: a client, a tool loop that added tools, a route's fallback. It
  speaks as the first, by its name, so nothing else knows; OpenAI's chat is
  `Either::new(OpenAiChat, OpenAiResponses, chat_refuses)`.
- `VendorProvider`: a provider of a vendor (`Vendor::provider`), or of
  formats of no vendor (`VendorProvider::new`), at a base URL with a key.
  `register` adds a core provider of each of its formats, each at its base
  URL, taking the key as the vendor does, over HTTP or, for a session,
  WebSocket, and carrying the vendor's quirk plugin, which runs on its calls
  alone, after the gateway's plugins. `models` lists its models; `client`
  is a client of it, at its base URL with its key, for any call of its own.

A format is a spec and names no vendor; a vendor's departures from a spec
are its own, here.
