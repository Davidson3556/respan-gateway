# Tool vendors

`respan-gateway-tool-vendors` is the HTTP APIs the gateway offers as MCP
tools, though none of them speaks MCP: where each is, how it takes a key,
and its tools. `ALL` lists them (`GREEN` and `YELLOW` by tier); `find(id)`
is one, and the binary's `[tools.<name>] vendor = "<id>"` serves it as an
MCP server in process, its tools named `<name>__<tool>` like any other
server's.

[`kit/`](kit) (`respan-gateway-tool-vendor-kit`) is what they are built of:
a vendor is a `ToolVendor` (its id, key, base URL and `Tools`), and
`ToolServer` serves one. `KeyIn` says how the key goes: a bearer token, a
header of its own, an `Authorization` scheme, Basic, or sent by the tools
themselves. `Api` sends any request with it and reads JSON, text or bytes,
`args` reads a call's arguments, and a call returns a `ToolOutput` of text,
image and audio blocks, and the usage it measured, by the vendor's own unit
(`with_usage("credits", 2.0)`): read from the vendor's answer, or worked out
from the arguments. `ToolServer` puts it in the result's `_meta`, under
`USAGE` (`respan.ai/usage`), for whoever prices it.

Each vendor is one file in a tier's crate, as the model vendors are:

- **green** ([`green/`](green), `respan-gateway-tool-vendors-green`):
  maintained, and verified end to end against the vendor's live API.
- **yellow** ([`yellow/`](yellow), `respan-gateway-tool-vendors-yellow`):
  maintained, but not verified end to end. A vendor moves to green once it is.

The end-to-end check is [`examples/live.rs`](examples/live.rs): it calls each
of a vendor's tools against its live API, for each vendor whose key is set,
and deletes what it creates. A vendor passing it, every tool, is green:

```sh
cargo run -p respan-gateway-tool-vendors --example live -- tavily e2b
```

Every tool vendor, its tier, key and tools, and what each tool calls, are in
[`docs/tool-vendors.md`](../../docs/tool-vendors.md); a vendor added here, or
a tool added to a vendor, is a row or an entry there too.

A tool's result is text for the model (results as compact JSON or
markdown, URLs kept), with images or audio where the tool makes them. Bad arguments, a failed request and a failed run are
tool errors, told to the model as such.
