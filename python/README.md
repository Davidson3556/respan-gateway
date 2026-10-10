# respan-gateway for Python

The [respan-gateway](https://github.com/respanai/respan-gateway) LLM gateway as
a Python package: use it in-process with the official OpenAI and Anthropic
SDKs (no socket, no server), run it as a local server, or use the same
command line as the Rust binary. The gateway is the Rust one, compiled into a
native extension (`abi3`, Python 3.9+).

> **Status: pre-alpha**, like the gateway itself.

## Install

```bash
pip install respan-gateway            # add `openai` and/or `anthropic` for gw.openai() / gw.anthropic()
```

## Use the SDKs in-process

```python
import respan_gateway as rg

gw = rg.Gateway()                    # from OPENAI_API_KEY, ANTHROPIC_API_KEY, GEMINI_API_KEY, ...
client = gw.openai()                 # an openai.OpenAI that talks to gw directly

# A Claude model through the OpenAI SDK is translated to Anthropic.
reply = client.chat.completions.create(
    model="claude-sonnet-4-5", messages=[{"role": "user", "content": "hi"}]
)
```

`gw.anthropic()` is the same for `anthropic.Anthropic`; `asynchronous=True`
gives the async clients. For any other httpx-based client, `gw.transport()`
and `gw.async_transport()` are `httpx` transports:
`httpx.Client(transport=gw.transport())`. Raw requests:
`gw.request("POST", "/v1/chat/completions", json=...)` returns an
`httpx.Response`; `gw.stream(...)` yields the body chunk by chunk as it
arrives. HTTP error statuses are responses, as in httpx. `arequest` and
`astream` are the asyncio versions.

## Serve

```python
with gw.serve(port=0) as server:     # a real socket, in the background
    print(server.url)                # http://127.0.0.1:54321

gw.serve_forever(port=8080)          # blocks; Ctrl-C stops it
```

The server is the binary's, web UI included (`server.url + "/ui/"`). Without
client `keys` in the config, anyone who reaches it spends your provider keys:
keep it on a local address. `Gateway.openai()`'s `api_key` is one of those
client keys when the config sets them; otherwise a placeholder, forwarded
only to a provider with no key of its own.

## Command line

```bash
respan-gateway                                # also: init, check, proxy --anthropic|--openai, --help
python -m respan_gateway proxy --openai
```

## Configuration

`Gateway(config=...)` takes a dict or a YAML string in the format of
[`gateway.example.yaml`](../gateway.example.yaml); `Gateway.from_file(path)`
reads a file. With no config, the gateway is built from the environment as
the binary is: every vendor whose key is set (`OPENAI_API_KEY`,
`ANTHROPIC_API_KEY`, `GEMINI_API_KEY`, `GROQ_API_KEY`, ...) becomes a
provider, models route by name (`claude-*` to Anthropic, `gemini-*` to
Gemini, or explicitly `groq/llama-3.3-70b-versatile`), and
`RESPAN_<VENDOR>_BASE_URL` points one elsewhere. An invalid configuration
raises `respan_gateway.ConfigError` (a `ValueError`); a failure to carry out
a request or start a server raises `respan_gateway.GatewayError`.

## Development

```bash
cd python
uv venv && uv pip install maturin pytest httpx openai anthropic
.venv/bin/maturin develop        # builds the extension into .venv
.venv/bin/pytest                 # tests run against a fake provider, offline
```

The extension is the `respan-gateway-python` crate (`src/`), a workspace
member that depends on the `respan-gateway` library in `apps/gateway`. The
`extension-module` feature is enabled by maturin only, so `cargo test` and
`cargo clippy` of the workspace link normally. One tokio runtime, started on
first use, serves every gateway, server and the command line; every call that
waits releases the GIL. The async transport runs the blocking native calls on
the event loop's thread-pool executor rather than bridging futures: the
calls are short or interruptible (closing a response wakes a pending read),
and it needs no second native API.
