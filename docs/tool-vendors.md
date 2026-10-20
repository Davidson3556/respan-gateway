# Tool vendors

A tool vendor is an HTTP API the gateway serves as an MCP server of its
own, in process, though the API itself speaks no MCP. Set its key in the
environment and it is a server named after it, or name it under `[tools]`
in `gateway.toml`:

```toml
[tools.search]
vendor = "linkup"
key = "env:LINKUP_API_KEY"
```

Its tools are reached like any server's: at `/mcp/{name}`, at `/mcp` as
`{name}__{tool}` (`search__search`), and in the tool loop (see
[MCP](mcp.md#tool-vendors)).

Each tool vendor is in one of two tiers, as the [model vendors](vendors.md)
are:

- 🟢 **Green**: maintained, and verified end to end against the vendor's
  live API.
- 🟡 **Yellow**: maintained, but not yet verified end to end against the
  vendor's live API. A yellow vendor becomes green once it is.

## 🟢 Green

None yet.

## 🟡 Yellow

| | Vendor | `vendor` | Key | Tools |
|:-:|---|---|---|---|
| <picture><source media="(prefers-color-scheme: dark)" srcset="assets/tool-vendors/linkup-dark.svg"><img src="assets/tool-vendors/linkup.svg" width="20" height="20" alt=""></picture> | [Linkup](https://linkup.so) | `linkup` | `LINKUP_API_KEY` | `search`, `answer`, `fetch` |
| <img src="assets/tool-vendors/tinyfish.png" width="20" height="20" alt=""> | [TinyFish](https://tinyfish.ai) | `tinyfish` | `TINYFISH_API_KEY` | `search`, `fetch`, `run_agent` |

These tables are kept by hand: a vendor added to `ALL` in
[`crates/tool-vendors`](../crates/tool-vendors/README.md), or a tool added
to a vendor, is a row or an entry here too.

## Their tools

**Linkup**, at `https://api.linkup.so/v1`, its key a bearer token:

- `search` is `POST /search`, for ranked sources.
- `answer` is the same, for a written answer with its sources.
- `fetch` is `POST /fetch`, a page as markdown.

Each call is billed; `depth: "deep"` costs more.

**TinyFish**, its key in `X-API-Key`:

- `search` is `GET https://api.search.tinyfish.ai/`.
- `fetch` is `POST https://api.fetch.tinyfish.ai/`, up to 10 pages as
  markdown.
- `run_agent` is `POST https://agent.tinyfish.ai/v1/automation/run`, a
  browser agent carrying out a goal on a site and waiting for it: slow,
  and billed in credits.

`base_url` moves only the agent's host.

A tool's result is text for the model: results as compact JSON or
markdown, URLs kept. Bad arguments, a failed request and a failed run are
tool errors, told to the model as such.

How a tool vendor is built, and how to add one, is in
[`crates/tool-vendors`](../crates/tool-vendors/README.md).

---

Logos are each vendor's trademark, shown only to identify it, and are from
the vendors' own sites.
