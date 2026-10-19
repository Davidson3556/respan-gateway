# Configuration

The gateway runs without a config file: every vendor whose key is in the
environment becomes a provider. A config file adds client keys, routes,
custom providers, prices, plugins and tool servers.
[`gateway.example.toml`](../gateway.example.toml) is a short example to start from.

## Command line

```
respan-gateway [serve]           Serve ./gateway.toml, or the providers in the environment
respan-gateway init              Write a commented gateway.toml (-p groq enables a vendor;
                                 --client-key generates a client key and prints it once)
respan-gateway check [--online]  Show what a config serves; --online also calls each provider
respan-gateway models            List every provider's models
respan-gateway mcp               List the MCP servers and whether each is authenticated
respan-gateway mcp login <name>  Log in once to an MCP server that needs a person to approve
respan-gateway proxy --anthropic|--openai
                                 Sit in front of Claude Code or Codex, relaying the vendor's
                                 whole API with each client's own credentials
```

## Environment

Settings come from flags, the environment, or the nearest `.env` file. The
environment wins over `.env`.

| Variable | Meaning |
|---|---|
| `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY`, `GROQ_API_KEY`, … | Provider keys. With no config file, each key makes its vendor a provider |
| `RESPAN_CONFIG` | The config file (`--config`) |
| `RESPAN_LISTEN` | Where to listen (`--listen`): `127.0.0.1:8080`, `localhost:8080` or `8080` |
| `PORT` | The port to listen on, on every address, for hosts that assign one |
| `RESPAN_CLIENT_KEYS` | Client keys, comma separated |
| `RESPAN_LOG` | `text`, `json` or `off` (`--log`) |
| `RESPAN_COLOR` | `auto`, `always` or `never` (`--color`) |
| `RESPAN_<VENDOR>_BASE_URL` | Move a vendor's base URL when there is no config file |

## The config file

The config is one TOML file, `gateway.toml`, reloaded without a restart on
`SIGHUP`, `POST /ui/api/reload`, or a file change with `--watch`. A typical
file:

```toml
listen = "127.0.0.1:8080"
client_keys = [
  { name = "ops", key = "env:OPS_KEY", admin = true },
  { name = "app", key = "env:APP_KEY" },
]

[providers.openai]
vendor = "openai"
key = "env:OPENAI_API_KEY"

[providers.anthropic]
vendor = "anthropic"
key = "env:ANTHROPIC_API_KEY"

[providers.local]                       # Any OpenAI-compatible endpoint
formats = ["openai-chat"]
base_url = "http://localhost:8000/v1"

[routes.smart]                          # Clients ask for "model": "smart"
to = ["anthropic/claude-haiku-5-5", "openai/gpt-6-luna"]
tries = 2                               # Then fall back to the next candidate

[routes."llama-*"]
to = "local"

[plugins.respan]                        # Each request a trace in Respan
key = "env:RESPAN_API_KEY"
```

Write secrets as `env:NAME` to keep them out of the file.

## Providers

A provider is a `vendor` with its key, or any compatible endpoint given
`formats` and a `base_url`. The vendors are `openai`, `anthropic`, `gemini`,
`respan`, `typesafe`, `groq`, `deepseek`, `openrouter`, `xai`, `mistral`,
`together`, `fireworks`, `ollama`, `azure`, `vllm` and `cloudflare`. Each
vendor's tier, key, formats and base URL are in [Vendors](vendors.md).

## Client keys

With `client_keys` set, every request must carry one, as
`Authorization: Bearer`, `x-api-key` or `x-goog-api-key`. The gateway strips
it, so no provider ever sees it. Without keys, the gateway answers only
loopback and same-origin browsers, and **refuses to start on a non-loopback
address**, so nobody who can reach it spends your provider keys.

A key may be named, may be `admin` (needed for the admin API under
`/ui/api/`), and may be limited to certain `tools` (`server__tool` globs;
`[]` for none). A limited key reaches MCP servers only through `/mcp`,
never `/mcp/{name}`, which offers all of a server's tools. Browser apps on other
origins need a `cors = { origins = [...] }` entry.

## Routing

A request's model goes, in order, to:

1. a route of exactly that name;
2. `provider/model`, to that provider;
3. the first glob route that matches (`"llama-*"`);
4. the provider whose model list has it (the list is fetched at start and
   every 10 minutes);
5. otherwise, the default provider for that kind of request.

A route's `to` is a candidate or a list: `provider`, `provider/model`, or
`provider/{model}`. `pick` orders the candidates: `first`, `cheapest`,
`fastest`, `weighted` or `random`. `needs = ["tools"]` drops models that
lack a capability, and `when` limits a route to certain client keys or
kinds. A failed candidate falls back to the next (`tries`, `backoff`), and
`hedge = "200ms"` races them instead. To see where a model would go and
why, use `GET /ui/api/routes/explain?model=M`.

## Pricing

Each call is priced at its model's price, from the provider's model list or
from a price you declare:

```toml
[models."openai/my-finetune"]
pricing = { input = 1.0, output = 2.0, cached_input = 0.5 }   # $ per million tokens
```

A request's cost is the sum over its calls, retries and tool-loop steps
included. The request log and the UI show it.

## Conversation state

A Responses client's `previous_response_id` works with every provider, not
only OpenAI. When a translated provider keeps no state, the gateway keeps
it in memory for `keep` (default 1 hour). A Chat client's reasoning between
tool calls is kept the same way when OpenAI's Responses API serves it.
`[plugins.responses] enabled = false` turns this off.

## Tools

Tool servers (`[tools.<name>]`: MCP servers and tool vendors such as
Linkup), the `/mcp` endpoint that merges them, and the tool loop are in
[mcp.md](mcp.md).
