# respan-gateway-codec

What the [conversation](../conversation/README.md) codecs share (`Input` in,
`Output` out): the reader and encoder machinery around one wire format.

- `Codec`: what a format supplies (its names, how it reads and writes a
  request, a part, a whole response, its stream encoder and its error body);
  its provided methods are the HTTP codec on both edges, which a format's
  `Format` implementations call (`bind_client`, `bind_provider`).
- `Reader`: a format's decoder of one response into events, which
  `OutputParser` drives over SSE, holding a response's start until its
  first content.
- `StreamResponse(bool)`: a call extension choosing whether the provider is
  asked to stream; without it the call follows the client's request.
- Helpers both codecs use: messages and parts, the `native` fields a value
  keeps for its own format (`Keep`, `kept_fields`), usage totals, the faults
  of what cannot be written, and how a provider's error body reads
  (`provider_error`), leniently, as compatible servers send it.

What a format has no form for stays native to it (`NativeFields`,
`Content::Native`, `Output::Native`), is written only by that format, and is
refused in translation where it could change the answer. Only an explicit
raw relay ([`Relay::RAW`](../http/src/relay.rs)) sends a client's own bytes.
