# respan-gateway-anthropic

The Anthropic Messages [codec](../codec/README.md), named
`anthropic-messages`, usable on either edge: as a client format over
`Incoming` and as a provider format over `Outgoing`. A `POST` to a path
ending in `/v1/messages`; its provider base URL leaves the version out, such
as `https://api.anthropic.com`, and `anthropic-version: 2023-06-01` is sent
if the request names none.

Thinking is the kind's reasoning; its signature, and what else the kind has
no form for, stays native to this format. The crate's suites are its own;
those of translation with OpenAI Chat live in
[`respan-gateway-openai`](../openai/README.md).
