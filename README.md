# respan-gateway

An LLM gateway: one endpoint for OpenAI, Anthropic, Gemini and a dozen more
vendors, translating between their APIs. Point any official SDK at it and
name any model; the gateway routes by model, translates chat (text, tools,
images, reasoning), realtime voice, images, speech, transcription,
embeddings and video jobs, and relays everything else untouched. It is a
single binary with a web UI, a Python package, and a Rust library with a
small neutral core: routing, fallback, billing, guardrails and logging are
plugins built on one public API, compiled in (no dynamic loading).

> **Status: pre-alpha, release-candidate core.** The binary, the Python
> package and the four chat formats, the media kinds and both realtime
> formats work and are tested against strict fakes of each vendor; APIs
> around the core may still change. See the [roadmap](#roadmap).

## Install

- **Release binary**: each `v*` tag attaches `respan-gateway` (web UI built
  in) for Linux x86_64 and aarch64, macOS aarch64 and x86_64 (`.tar.gz`)
  and Windows x86_64 (`.zip`, with `respan-gateway.exe`) to its
  [GitHub release](https://github.com/respanai/respan-gateway/releases).
- **Docker**: `docker run -p 8080:8080 -e OPENAI_API_KEY -e RESPAN_CLIENT_KEYS=sk-gw-change-me ghcr.io/respanai/respan-gateway`
  (or `docker build -t respan-gateway .`). The image listens on every
  address, so it refuses to start without a client key.
- **Python**: `pip install respan-gateway` ([`python/`](python/README.md)).
- **Cargo**: `cargo install --git https://github.com/respanai/respan-gateway respan-gateway`,
  or from a checkout `cd apps/ui && pnpm install && pnpm build` then
  `cargo build --release -p respan-gateway`. Without the UI build the
  binary serves a page saying so; everything else works.

Release artifacts exist once a tag has been pushed and the registries are
set up ([CONTRIBUTING](CONTRIBUTING.md#releasing)); until then build from source.

## Quickstart

No config file is needed: every vendor whose key is in the environment
becomes a provider.

```bash
export OPENAI_API_KEY=sk-... ANTHROPIC_API_KEY=sk-ant-... GEMINI_API_KEY=...
respan-gateway        # prints where it listens, the providers, and a curl to try
```

Any client speaks any vendor; the model name picks the provider:

```bash
# OpenAI Chat to Claude (translated)
curl localhost:8080/v1/chat/completions \
  -d '{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hi"}]}'
# Anthropic Messages to GPT
curl localhost:8080/v1/messages \
  -d '{"model":"gpt-4o-mini","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}'
# OpenAI Responses; and `provider/model` picks a provider explicitly
curl localhost:8080/v1/responses -d '{"model":"groq/llama-3.3-70b-versatile","input":"hi"}'
```

SDKs only need a base URL:

```python
from openai import OpenAI
OpenAI(base_url="http://localhost:8080/v1", api_key="unused").chat.completions.create(
    model="gemini-2.5-flash", messages=[{"role": "user", "content": "hi"}])

from anthropic import Anthropic
Anthropic(base_url="http://localhost:8080", api_key="unused").messages.create(
    model="gpt-4o-mini", max_tokens=64, messages=[{"role": "user", "content": "hi"}])

from google import genai   # Gemini's native API, served at /v1beta/models/{model}:generateContent
genai.Client(api_key="unused", http_options={"base_url": "http://localhost:8080"}).models.generate_content(
    model="claude-sonnet-4-5", contents="hi")
```

(`api_key` is a client key when the gateway has `client_keys`.) Media work
the same, at OpenAI's endpoints, to whichever provider serves the model:

```bash
curl localhost:8080/v1/images/generations -d '{"model":"gpt-image-1","prompt":"a lighthouse"}'
curl localhost:8080/v1/audio/transcriptions -F file=@talk.mp3 -F model=whisper-1
curl localhost:8080/v1/embeddings -d '{"model":"text-embedding-3-small","input":"hello"}'
curl localhost:8080/v1/videos -d '{"model":"veo-3.0-generate-001","prompt":"a paper boat"}'
```

Other endpoints: `GET /v1/models` (every provider's models with their
`kind`, and each exact route name, `owned_by: "routes"`; `/v1beta/models` in
Gemini's shape), `GET /healthz`, and the web UI
at [localhost:8080/ui/](http://localhost:8080/ui/).

## The binary

```
respan-gateway [serve]          serve ./gateway.toml, or the providers in the environment
respan-gateway init             write a commented gateway.toml (-p groq enables a vendor;
                                --client-key generates a client key and prints it once)
respan-gateway check            what a config serves; exit 1 on a problem
respan-gateway check --online   ... and call each provider and MCP server with its key
respan-gateway models           every provider's models, with their kind
respan-gateway proxy --anthropic|--openai
                                sit in front of Claude Code or Codex, relaying a vendor's
                                whole API with each client's own credentials
```

Settings come from flags, the environment, or the nearest `.env` file in the current
directory or its parents (the environment wins). `respan-gateway --help` lists them:

| Variable | Meaning |
|---|---|
| `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY` (or `GOOGLE_API_KEY`), `GROQ_API_KEY`, ... | Provider keys; with no config file each is a provider |
| `RESPAN_CONFIG` | The config file, as `--config` |
| `RESPAN_LISTEN` | Where to listen, as `--listen` (`127.0.0.1:8080`, `localhost:8080`, `8080`) |
| `PORT` | The port, on every address (`0.0.0.0`), if neither `--listen`, `RESPAN_LISTEN` nor `listen` says; for hosts that hand it out |
| `RESPAN_CLIENT_KEYS` | Client keys, comma separated, added to the config's `client_keys` (named `key-N`) |
| `RESPAN_LOG` | `text`, `json` or `off` (one stderr line per finished request), as `--log` |
| `RESPAN_<VENDOR>_BASE_URL` | Moves a vendor's base URL when there is no config file |

**Client keys.** With `client_keys = ['env:GATEWAY_KEY']` (or
`RESPAN_CLIENT_KEYS`) every request must carry one (`Authorization: Bearer`,
`x-api-key` or `x-goog-api-key`); the gateway takes it off and no provider
sees it. Without keys the gateway answers only to loopback and to
same-origin browsers, and **refuses to start on a non-loopback address**
(`--listen 0.0.0.0:8080`, `PORT`, Docker) so nobody who can reach it spends
your provider keys. `init --client-key` writes a strong key into the file.
A provider with its own key always uses it; one with none receives the
client's own credential headers, only when the client speaks the provider's
format, never across a translation.

**Browser apps.** A web page on another origin (MCP Inspector, a chat front
end) is refused unless `cors` lists it: `cors: { origins: ["https://app.example.com"] }`
answers that origin's preflight (`OPTIONS`, which carries no key) and lets its
requests read the answers, including the `mcp-session-id` header; the preflight is answered with the headers it asks for (a browser SDK's `x-stainless-*`, say). Its requests
still need a client key if keys are set. `origins: ["*"]` allows any page and
needs `client_keys`. Without `cors`, nothing changes.

**Configuration** is one TOML file, every setting shown in
[`gateway.example.toml`](gateway.example.toml): providers (vendors or
spelled out), `formats` served, `client_keys`, `mcp` servers, `cors`,
`listen`, `log`, `ui`, `body_limit`, `ws_message_limit`,
`models` (declared by `provider/model`, each laid over what that provider's
list says of it: its `pricing` in US dollars per million tokens, and
anything else a model card has), `shutdown_grace`, `routes`, and
`plugins`. Each request's cost is the sum of its calls' to providers, each
priced at the `pricing` of the model it sent, as declared or as its
provider's list says (a route's pick, each step of a tool loop, a retry, a
race's loser). Each compiled-in plugin is configured under its own
name in `plugins`, and the gateway runs them in a fixed order whatever the
file's:

```toml
[plugins.routing]
default = "openai"                      # Each kind's default provider

[routes.smart]                          # Clients send "model": "smart"
to = ["anthropic/claude-opus-4-1", "openai/gpt-5"]
tries = 2                               # Then the next candidate

[routes."llama-*"]
to = "groq"                             # Sent as asked

[plugins.request_log]
history = 1000
file = "./requests.jsonl"               # Keep history across restarts

[plugins.responses]                     # Conversation state: on unless enabled = false
keep = "1h"

[plugins.respan]                        # Each request a trace in Respan
key = "env:RESPAN_API_KEY"
# content = false                       # Metadata only: no messages or replies
```

OpenAI keeps its own Responses, so a Responses client's `previous_response_id`
works when OpenAI answers. Translated to another provider, which keeps
nothing, the gateway keeps each translated response in memory, for the client
key that made it and for `keep`, and sends a follow-up the conversation it
names. A request that says `store: false` is not kept. The other way, a Chat
Completions client's call to OpenAI's Responses stores nothing at OpenAI
(`store: false`), and the reasoning its tool calls came of is kept the same
way, under each call's id, and sent back before those calls in the next turn,
as a Chat client cannot carry it. Both live in one gateway's memory: a client
whose requests reach several gateways must reach the one that kept them.
`[plugins.responses] enabled = false` turns it off.

Configuration is TOML only. The gateway reads `gateway.toml` from the working
directory, or uses provider keys from the environment when no file exists.
`-c FILE` selects an explicit TOML file; `init -o FILE` generates one.
Keep root settings above the first TOML table.

Text logs use colored levels and dim timestamps on terminals; redirected
output stays plain. Use `--color auto|always|never` (`RESPAN_COLOR`) to
control this; automatic colors respect `NO_COLOR`. `--log json` emits
unstyled structured logs and `--log off` disables request logs.

**Routes** are keyed by the model clients ask for, exact (`smart`, listed in
`/v1/models`) or a glob (`"llama-*"`). `to` is a candidate or a list:
`provider`, `provider/model`, or `provider/{model}` (`{model}` the name asked),
each alone or `{ target, weight }`. `pick` orders them: `first` (default),
`cheapest` (by each model's `pricing`, input + 3 × output),
`fastest` (the gateway's own time to first response; unknown ones get
tried), `weighted` or `random`. `needs = ["tools"]` drops candidates whose
listing lacks it (a capability, `tools`, `reasoning` or `structured_outputs`,
or what it reads, `image`, `audio`, `video` or `document`; `vision` and `pdf`
name the same); `when = { clients, kinds }` limits a route to client key
names and kinds. A failed candidate falls back to the next (`fallback =
false` stops it), each tried `tries` times (1 to 10) with `backoff` doubled up
to `max_backoff` (as `Retry-After` asks); `hedge = "200ms"` races them
instead, each started that long after the last. `until = "end"` holds an
answer whole before it counts. A candidate that failed its last 3 calls
within 30 s goes last, and a response carries the provider's real model.

Routing order: a route of that exact name; else `name/model` goes to provider
`name`; else the first glob route (in config order); else the provider whose
model list has it (of several, the vendor that claims the name); else the
vendors' prefixes (longest wins), for aliases and unlisted models; else
`routing.default`, or the kind's first provider
([routing](docs/ARCHITECTURE.md#routing)). A route whose candidates don't serve
the request's kind, or whose `when` doesn't match, is skipped. `GET
/ui/api/routes/explain?model=M` says where a model goes and why. The request
log's file is rotated at `file_max`, and `/ui/api/requests/export` downloads
the history.

**Reloading**: `kill -HUP <pid>`, `POST /ui/api/reload` (with a client key if
keys are set) or `--watch` (on a change of the file) re-read the config and
swap it in for new requests; open requests and sessions finish on the old
one, the request log carries over, and an invalid config is refused while the
old one keeps serving. A changed `listen` needs a restart. `SIGHUP` is unix-only; on Windows use
`POST /ui/api/reload` or `--watch`. In Docker, a `listen` in a mounted config
beats `PORT`, so `listen = "127.0.0.1:8080"` there is unreachable from outside.

**Client keys and admin.** A `client_keys` entry is a key, or `{ name, key,
admin?, tools? }`. The admin API (`/ui/api/*`: every client's request records,
reload, the config) is for admin keys: while no entry sets `admin: true`,
every key is one (single-user setups just work); once any entry does, only
those keys reach it and the others get `403` there (`admin: false` always
excludes a key). `tools` limits the tool-loop tools (`server__tool` globs, as
`plugins.tool_loop.allow`) a key's requests are offered, within the global `allow` and
`deny`, on the `/mcp` aggregate as for the tool loop.

**Admin config** (needs an admin key, like the rest of `/ui/api/`):
`GET /ui/api/config` shows the config file as on disk with every secret
(`client_keys`, provider `key`/`auth.key`, MCP `headers`, `auth.bearer`,
OAuth client secret and refresh token, `env` values and `args`, and URLs that
carry credentials) as `"…"`, while an
`env:NAME` reference stays as written. The Config page is read-only.
Edit the TOML file on disk, then use Reload to apply changes.

### MCP servers

The gateway fronts [MCP](https://modelcontextprotocol.io) servers too, behind
the same client keys and `Host`/`Origin` guard. Each one is served to MCP
clients over streamable HTTP at `/mcp/{name}`:

```toml
[mcp.github]                       # HTTP upstream
url = "https://api.githubcopilot.com/mcp/"
auth = { bearer = "env:GITHUB_TOKEN" }

[mcp.files]                        # stdio upstream
command = "npx"
args = ["-y", "@modelcontextprotocol/server-filesystem", "/data"]
```

The fields are those of an MCP client's `mcp.json`, so an entry copies
across: a `url` with `headers` and `auth`, or a `command` with `args`, `env`
and `cwd`. A client points its MCP config at `http://localhost:8080/mcp/github`
with a gateway key; the upstream credentials stay on the gateway. `headers`
(values literal or `env:NAME`, all treated as secrets) carries a key in a
header of its own, such as `{ "x-api-key" = "env:KEY" }`. `auth` is one of:

- `{ bearer = "env:KEY" }`: a key, sent as `Authorization: Bearer`.
- `{ oauth = { .. } }`: the gateway's own OAuth client (below).
- `"login"`: the login `respan-gateway mcp login <name>` saved in
  `.respan/mcp/<name>.json` beside the config.
- `"forward"`: each client's own credentials are sent on; without it they
  never are.

A stdio server runs as a child process, one per
client session, started when the session initializes (the `command` must be
on `PATH` at startup; on Windows `npx` finds `npx.cmd` through `PATHEXT`, and
the child and what it started are all ended with the session), given only `PATH` and its `env` (values literal or
`env:NAME`; `cwd` must be an existing directory), and stopped when the session
ends: the client's `DELETE`, ten idle
minutes, a crash (that session's calls fail and the client starts a new
session, so a fresh child), or the gateway stopping. Each MCP request is in
the request log as kind `mcp`, with its method and tool (`tools/call echo`)
in place of a model and the server as its provider; the MCP page and
`GET /ui/api/mcp` list the servers (how each signs in, and whether the tool
loop and `/mcp` use it), `GET /ui/api/mcp/{name}/tools` asks one for its
tools as a client would (kept out of the log and traces), and `check` prints
them (`check --online`
also asks each to `initialize`, ends the session, and says reachable,
unauthorized or unreachable; a stdio server is started and stopped for it).
With `client_keys`, a session belongs to the key that opened it: another key
naming its `Mcp-Session-Id` gets 404. Name `mcp` in `formats` if you list
formats.

**OAuth for an HTTP server.** For a server that wants an OAuth access token
rather than a fixed key, give it `auth = { oauth = { .. } }` and the gateway gets its own token
from the token endpoint (machine to machine) and sends it as the Bearer
token:

```toml
[mcp.crm]
url = "https://mcp.example.com/mcp"

[mcp.crm.auth.oauth]
token_url = "https://auth.example.com/oauth/token"
client_id = "env:CRM_CLIENT_ID"
client_secret = "env:CRM_CLIENT_SECRET"
scopes = ["read", "write"]          # Optional
# refresh_token = "env:CRM_REFRESH_TOKEN"
```

Without `refresh_token` the gateway uses the **client credentials** grant;
with it, the **refresh token** grant (a token a person authorized once, which
the gateway keeps alive; if the endpoint rotates it, the new one is kept in
memory until the gateway restarts, so restart with a fresh one in the
environment if the old one was revoked). The access token is cached until it
expires, and a `401` from the server drops it, gets a new one and sends the
call once more. `client_id`, `client_secret` and `refresh_token` are literal
or `env:NAME`; none is logged or shown by `check`, the admin API or the UI.
For a server that wants a person to approve in a browser (authorization code
with PKCE), run `respan-gateway mcp login <name>` once and set
`auth = "login"`.

**One endpoint for all of them.** `[plugins.mcp_aggregate]` turns on `/mcp`, which
fronts the `mcp` servers as a single MCP server, so a client configures one
URL:

```toml
[plugins.mcp_aggregate]             # All servers but `auth = "forward"` ones
# servers = ["github", "files"]     # Or choose servers, in list order
# allow = ["github__search_*"]      # Default: all tools
# deny = ["files__write_*"]         # Wins over allow
```

A client has one session, owned by its key like any other; the gateway holds
one session with each server behind it, opened with the client's and ended by
its `DELETE`. What the client sees:

- `initialize` answers with the servers' capabilities merged.
- Tools and prompts are named `{server}__{name}` (`github__search`); a call
  goes to that server under the name it knows. `allow` and `deny` apply to
  tools; a withheld or unknown name is an error (-32602).
- Resources are `mcp://{server}/{their URI}`, the same for templates and for
  the URIs a tool's result links to or embeds; `resources/read` and
  `subscribe` go to the server the URI names. Nothing is looked up.
- Lists (`tools/list`, `prompts/list`, `resources/list`,
  `resources/templates/list`) are the first page of every server's, merged. A
  `nextCursor` (opaque) is there while any server has more, and asks for those
  servers only.
- The `GET` stream carries the servers' `list_changed`, `resources/updated`
  and log notifications; a client's `notifications/cancelled` goes to every
  server; progress notifications ride the answer to their call. Sampling,
  elicitation, roots and completion are not offered. `ping` is answered.
- The request log has one entry per client request (`tools/call github__search`
  on path `/mcp`), with the server it was routed to as its provider.

A server that does not answer `initialize` is left out of that session; the
aggregate fails only if none does. A session's server sessions are ended by
the client's `DELETE`; one that never comes is left to each server's idle
timeout. Servers with `auth = "forward"` cannot be fronted (the sessions are the
gateway's own). Each server stays at `/mcp/{name}` too.

### Tool loop

With `[plugins.tool_loop]` the gateway runs MCP tools for the model, so a client that knows
nothing of MCP gets answers that used them. It is off unless configured:

```toml
[plugins.tool_loop]
servers = ["github", "files"]
# allow = ["github__search_*", "files__read_file"]
# deny = ["files__write_*"]         # Wins over allow
models = ["gpt", "claude"]          # Optional model prefixes
max_steps = 8                      # Model calls per request
# timeout = "5m"                   # Whole request's loop
# tool_timeout = "60s"             # One tool call
# tool_list_ttl = "5m"             # Tool list cache lifetime
# parallel = 4                     # Concurrent tool calls per step
# max_result = "64KiB"             # One tool answer
# max_results = "256KiB"           # All tool answers per request
```

`allow` and `deny` name tools as the model has them, `<server>__<tool>` with
any character other than letters, digits and `-` written `_`, and `*` for any
characters. Every client key can make the model call any tool offered, with
the servers' own credentials, so offer no more than you would let a client
do: list the tools you want in `allow`, or the dangerous ones in `deny`.
`check --online` lists the tools that would be offered (it contacts the
servers; plain `check` does not).

On a chat request in scope (any chat format, translated or not) the gateway
adds the servers' tools to the request, named `<server>__<tool>` (such as
`github__search_issues`), calls the provider, and while the reply calls only
those tools it runs them (in parallel), appends the calls and results and
calls again. The client sees one reply: the final step's, with the usage of
every step summed. A client can leave the loop out of one request with the
header `x-respan-tool-loop: off`.

A live session (OpenAI Realtime, Gemini Live, either way across) gets the
servers' tools too, beside its own, in each configuration that sets tools.
When the model calls one, the gateway runs it while the session goes on,
sends the model the answer and asks it to respond; the client sees neither the
call nor its answer, nor the response that only made it, but the response that
answers. A call of the client's own tool is the client's, as is asking for the
next response when a response called both. Each call is bounded by
`tool_timeout`, and the session's answers by `max_results`; `max_steps` and
`timeout` are a chat's.

- It stops at a reply that calls no gateway tool; at a reply that calls one of
  the client's *own* tools (the client runs those; the gateway's calls in that
  reply are left out of what it sees); at `max_steps`, where the last call
  forbids tools and a model that still asks for one fails the request (502,
  `tool_loop_max_steps`); and at `timeout` (504, `tool_loop_timeout`).
- A tool's failure, a timeout, an unreachable server or malformed arguments
  is a tool result the model reads (flagged as an error where the provider's
  format has the flag, else text beginning `Error:`), not a failed request.
  Answers longer than `max_result` are cut, saying so, and once a request's
  answers add up to `max_results` the rest are left out.
- A tool's description and answer is untrusted input to the model: a server
  (or a page or file it reads) can try to instruct it. Offer tools whose
  servers you trust, and keep destructive ones out with `deny`.
- Streaming: a step's text and the client's own tool calls stream as they
  arrive, the gateway's tool calls are held back and run, and the next step's
  text follows in the same reply, its blocks and their native events
  (such as an Anthropic `signature_delta`) renumbered without gaps. Usage is
  sent once, before the finish. Events about a step as a whole (a ping, a stop
  sequence) are those of the first step.
- Reasoning is kept: what the model said in a step goes back as it said it, so
  Anthropic's signed `thinking` (and `redacted_thinking`) blocks and Gemini's
  `thoughtSignature` are in the assistant message of the next step, as those
  providers require. A provider-run tool's blocks are not replayed.
- Tools run with the server's own credentials, never a client's (a server with
  `auth = "forward"` cannot be a tool server). The gateway is an MCP client of its
  own: one session per server, kept across requests and started again when
  lost, and each tool list kept for `tool_list_ttl`. At a config reload the old
  build's sessions are ended (an HTTP server's `DELETE`d, a stdio server's
  child stopped) as soon as its requests are done.
- The request log shows the provider's steps and each tool call (name,
  duration, bytes, whether it failed) in the request detail drawer; `check`
  prints the section.

Not covered: a step's extended thinking or Gemini thought signatures are not
replayed to the provider, so models that insist on them with tool use may
refuse the second step; native events of steps after the first are dropped.

## What it speaks

Each kind translates within itself; a client and a provider may use
different formats of one kind.

| Kind | Formats (client and provider side) |
|---|---|
| chat | `openai-chat`, `anthropic-messages`, `gemini-generate`, `openai-responses`; plus `passthrough` for a vendor's other endpoints |
| realtime | `openai-realtime`, `gemini-live`, translated both ways; `responses-ws` relayed to the one provider of its format |
| image | `openai-images` (Imagen through Gemini's OpenAI-compatible endpoint) |
| transcription, speech | `openai-transcriptions`, `openai-speech` |
| embedding | `openai-embeddings` |
| video | `openai-videos`, `gemini-veo` (Veo's `predictLongRunning`, its operations and file downloads) |

Realtime usage after a tool call or an interruption may arrive in the next
`response.done` rather than the one that ended the turn
([details](crates/formats/realtime-ws/README.md)).

Reasoning controls (effort, budget, summaries) and `top_k` map between the
chat formats, or are refused where a format has no form for them
([table](crates/formats/conversation-http/README.md#reasoning-and-sampling)).
Details per format: [chat](crates/formats/conversation-http/README.md),
[media](crates/formats/media-http/README.md),
[decision](crates/formats/decision-http/README.md),
[realtime](crates/formats/realtime-ws/README.md).

## Vendors

`openai`, `anthropic`, `gemini`, `groq`, `deepseek`, `openrouter`, `xai`,
`mistral`, `together`, `fireworks`, `ollama`, `azure`, `vllm`, `typesafe` and
`cloudflare` (Workers AI, at `CLOUDFLARE_ACCOUNT_ID`'s base URL), a file each
in [`crates/vendors`](crates/vendors/README.md), which lists the formats
each speaks, how the gateway handles where it departs from a spec, and the
base URL each format takes. Any other compatible endpoint is a
provider with a `base_url` and a format; `formats` can also give each
format a base URL of its own, `formats = { "openai-chat" = "https://h/v1" }`,
as moving Gemini, whose formats sit at different URLs, needs.

## Web UI

`/ui/` shows providers, routes, each request live (and a playground for
chat, images, transcription, speech and video) through the admin API under
`/ui/api` (read-only views and reload). `ui = false` turns both off. See
[`apps/ui`](apps/ui/README.md).

## Python

`pip install respan-gateway` gives the same gateway in process: official
OpenAI and Anthropic SDK clients with no socket, a background server, and
the CLI. See [`python/README.md`](python/README.md).

## Library use

The binary is a library too: `respan_gateway::App::new(Config::from_env()?)?`
gives an `axum::Router`. To build your own gateway from the crates, with
your own plugins, read [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and the
runnable, tested [`examples/`](examples/README.md) of the
[use cases](docs/use-cases.md).

## Project layout

| Path | What it is |
|---|---|
| `crates/core/` | The neutral kernel and plugin API: typed streams, plugins, lifecycle, faults |
| `crates/kinds/` | Event types per kind: `conversation`, `media`, `decision`, `realtime`, `mcp` |
| `crates/wires/` | Bytes at both edges, no kind: `http` (bodies, credentials, path matching), `http-transport` (Reqwest), `http-faults`, `sse`, `ws` |
| `crates/formats/` | A kind over a wire, no vendor: `conversation-http`, `media-http`, `decision-http`, `realtime-ws`, `mcp-http` |
| `crates/vendors/kit/` | A vendor as a template, and a provider of it: register its formats, list its models; what vendors share |
| `crates/vendors/` | Every vendor, a file each, in two tiers: `green/`, verified end to end against the live API, and `yellow/`, tested against fakes |
| `crates/plugins/routing`, `fallback`, `tool-loop` | First-party plugins: model routing and dispatch to a route's candidates; retry, fallback and race; the tool loop |
| `crates/serve/` | Hosting: one Axum router for HTTP and WebSocket sessions |
| `apps/gateway/` | The binary and its library: config, composition, client keys, `/v1/models`, request log, admin API |
| `apps/ui/` | The web UI (React, shadcn/ui), embedded in the binary |
| `python/` | The PyPI package: a PyO3 extension over `apps/gateway` |
| `examples/` | Runnable integrations of the use cases |
| `validation/` | Opt-in live-provider and external-tool checks, outside the workspace |

Layers, dependencies and contracts: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
Tests: [`docs/TESTING.md`](docs/TESTING.md). Contributing: [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Roadmap

Goal for 0.1: all 14 [use cases](docs/use-cases.md) pass as integration
tests, and a 10-minute plugin really takes 10 minutes.

| Milestone | Delivers | Status |
|---|---|---|
| M1 Relay kernel | Plugins, byte-for-byte relay, OpenAI and Anthropic, routing, TOML config, `check` | done |
| M2 Plugin API | Calls of their own input, async stream filters, unread bodies | done |
| M3 Formats | Gemini Live and `generateContent`, Responses over HTTP and WebSocket, OpenAI media endpoints, Veo | done |
| M4 Plugins and examples | Fallback, hedge, limits, memo, billing | done, as [examples](examples/README.md) |
| M5 Translation | Tools, images and reasoning between the chat formats | done |
| M6 Hard cases | Realtime translation, video jobs, PII rewriting, admission | done |
| M7 0.1 release | Release binaries, Docker image and PyPI wheels are built by tag workflows; first tag pending | open |
| M8 MCP gateway | `mcp` servers (streamable HTTP or stdio) served at `/mcp/{name}` with client keys, the request log and the web UI | done |
| M9 Tool loop | `[plugins.tool_loop]` runs the tools of `mcp` servers inside chat requests, for every chat format and provider | done |

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option.
