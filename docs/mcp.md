# MCP

The gateway fronts [MCP](https://modelcontextprotocol.io) servers behind the
same client keys as everything else, serving each at `/mcp/{name}`:

```toml
[mcp.github]                            # HTTP upstream
url = "https://api.githubcopilot.com/mcp/"
auth = { bearer = "env:GITHUB_TOKEN" }

[mcp.files]                             # stdio upstream, a child per client session
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "/data"]

[mcp.linear]                            # A server a person must approve once
url = "https://mcp.linear.app/mcp"
auth = "login"                          # Run: respan-gateway mcp login linear
```

The fields match an MCP client's `mcp.json`, so entries copy across. `auth`
can be:

- `{ bearer = "env:KEY" }`, a static key;
- `{ oauth = { token_url, client_id, client_secret, ... } }`, the gateway's
  own OAuth client;
- `"login"`, a login saved by `respan-gateway mcp login <name>`. That
  command opens your browser on Enter. On a headless machine, it takes the
  callback address pasted back instead;
- `"forward"`, which sends each client's own credentials on.

`respan-gateway mcp` lists the servers and whether each is authenticated.

## One endpoint for all servers

`[plugins.mcp_aggregate]` serves `/mcp`, which merges every server into one,
with tools named `{server}__{tool}` and optional `allow`/`deny` globs. A
client key limited to certain `tools` reaches MCP servers only through it
(see [client keys](configuration.md#client-keys)).

## Tool loop

With `[plugins.tool_loop]`, the gateway gives the model the MCP servers'
tools and runs the calls itself, for any chat format and any provider, and
in realtime sessions too. The client sees one ordinary reply.

```toml
[plugins.tool_loop]
servers = ["github"]
allow = ["github__search_*"]            # Offer only what you would let any client do
max_steps = 8
```

A tool's description and output are untrusted input to the model. Offer
only tools from servers you trust, and use `deny` to keep destructive ones
out. A client can opt one request out with `x-respan-tool-loop: off`.
