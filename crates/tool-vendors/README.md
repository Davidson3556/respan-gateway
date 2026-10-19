# Tool vendors

`respan-gateway-tool-vendors` is the HTTP APIs the gateway offers as MCP
tools, though none of them speaks MCP: where each is, how it takes a key,
and its tools. `ALL` lists them; `find(id)` is one, and the binary's
`[mcp.<name>] vendor = "<id>"` serves it as an MCP server in process, its
tools named `<name>__<tool>` like any other server's.

[`kit/`](kit) (`respan-gateway-tool-vendor-kit`) is what they are built of:
a vendor is a `ToolVendor` (its id, key, base URL and `Tools`), and
`ToolServer` serves one. Each vendor is one file in `src/`.

| Vendor | Id | Key | Tools |
| --- | --- | --- | --- |
| [Linkup](https://linkup.so) | `linkup` | `LINKUP_API_KEY` (bearer) | `search`, `answer`, `fetch` |
| [TinyFish](https://tinyfish.ai) | `tinyfish` | `TINYFISH_API_KEY` (`X-API-Key`) | `search`, `fetch`, `run_agent` |

- `linkup`, at `https://api.linkup.so/v1`: `search` is `POST /search` for
  ranked sources, `answer` the same for a written answer with its sources,
  and `fetch` is `POST /fetch`, a page as markdown. Each call is billed;
  `depth: "deep"` costs more.
- `tinyfish`: `search` is `GET https://api.search.tinyfish.ai/`, `fetch` is
  `POST https://api.fetch.tinyfish.ai/` (up to 10 pages as markdown), and
  `run_agent` is `POST https://agent.tinyfish.ai/v1/automation/run`, a
  browser agent carrying out a goal on a site and waiting for it: slow,
  and billed in credits. `base_url` moves only the agent's host.

A tool's result is text for the model: results as compact JSON or
markdown, URLs kept. Bad arguments, a failed request and a failed run are
tool errors, told to the model as such.
