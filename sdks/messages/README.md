# Messages SDK

`respan-gateway-messages` provides transport-neutral message vocabulary:
`conversation::{Conversation, Reply, Message, …}`, `Usage`, `Details`, and
`relayed::{Relayed, Came}`. Decoded values retain their original bytes and
snapshot for byte-exact reuse and input-fault attribution. The SDK uses
`bytes` directly; it does not depend on HTTP.

HTTP codecs, JSON/SSE wire parsing, error encoding, opaque passthrough, and
body framing remain in `respan-gateway-formats`. In particular,
`respan_gateway_formats::json()` accepts JSON bodies **and** SSE `data:`
events; it is a wire helper, not part of the message vocabulary.
