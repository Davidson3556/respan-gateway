"""Types for the configuration mapping, for editor completion and checking.

They describe what `Gateway(config=...)` takes (the format of
`gateway.example.toml`). At run time a config is a plain dict, and a mistake
in one is a `ConfigError`.
"""

from __future__ import annotations

from typing import Dict, List, TypedDict, Union


class Auth(TypedDict, total=False):
    """A provider's credentials as one header."""

    type: str  # "bearer" or "header"
    key: str  # literal or "env:NAME"
    name: str  # the header, for type "header"


class ProviderConfig(TypedDict, total=False):
    """One provider: a vendor preset, explicit formats, or a preset with some settings replaced."""

    vendor: str  # "openai", "anthropic", "gemini", "groq", ...
    formats: Union[str, List[str]]  # e.g. "openai-chat", ["openai-images", "openai-embeddings"]
    base_url: str
    key: str  # literal or "env:NAME"
    auth: Auth


class OAuthConfig(TypedDict, total=False):
    """The gateway's own OAuth client for an HTTP MCP server (machine to machine)."""

    token_url: str  # required
    client_id: str  # required; literal or "env:NAME"
    client_secret: str  # required; literal or "env:NAME"
    scopes: List[str]
    refresh_token: str  # literal or "env:NAME"; the refresh token grant, else client credentials


class McpAggregateConfig(TypedDict, total=False):
    """One MCP endpoint, /mcp, that fronts the `mcp` servers as a single server."""

    servers: List[str]  # the `mcp` servers fronted; all without forward_auth if unset
    allow: List[str]  # tool names "server__tool", "*" for any characters; all if unset
    deny: List[str]  # never offered; wins over `allow`


class McpConfig(TypedDict, total=False):
    """One MCP server, served at /mcp/{name}: an HTTP `url` or a stdio `command`."""

    url: str  # a streamable-HTTP endpoint
    key: str  # literal or "env:NAME", sent as a bearer token
    auth: Auth  # one header, in place of `key`
    oauth: OAuthConfig  # a token from a token endpoint, in place of `key`
    forward_auth: bool  # send the client's credentials when there is no key
    command: List[str]  # a stdio server's program and arguments
    env: Dict[str, str]  # values literal or "env:NAME"
    cwd: str


class ToolLoopConfig(TypedDict, total=False):
    """The tool loop: the gateway runs the tools of `mcp` servers inside chat requests."""

    servers: List[str]  # the `mcp` servers whose tools are offered
    allow: List[str]  # tool names as the model has them, "github__*"; all if unset
    deny: List[str]  # never offered; wins over `allow`
    models: List[str]  # model prefixes the loop applies to; every chat model if unset
    max_steps: int
    timeout: Union[int, float, str]  # seconds, or "5m"
    tool_timeout: Union[int, float, str]
    tool_list_ttl: Union[int, float, str]  # how long a server's tool list is kept
    parallel: int  # tool calls of one step that run at once
    max_result: Union[int, str]  # bytes, or a size such as "64KiB"
    max_results: Union[int, str]  # bytes of tool answers in one request


class CorsConfig(TypedDict):
    """Browser access: pages on these origins may call the gateway."""

    origins: List[str]  # "https://app.example.com"; "*" needs client_keys


class RoutingConfig(TypedDict, total=False):
    """Which provider each request goes to."""

    models: List[Dict[str, str]]  # [{"prefix": "claude-", "provider": "anthropic"}]; first match wins
    default: str  # the provider for requests nothing else routes


class FallbackConfig(TypedDict, total=False):
    """Retry a provider, then try each provider of its chain in turn."""

    after: Dict[str, List[str]]  # {"openai": ["openai-eu"]}
    tries: int  # per provider, retries included; 1 to 10
    backoff: Union[int, float, str]  # the first wait before a retry; "250ms"
    max_backoff: Union[int, float, str]  # the longest wait; "10s"
    until: str  # "head" or "end": how far an answer is read before it counts


class HedgeConfig(TypedDict, total=False):
    """Race a provider against its chain, each started `stagger` after the last."""

    after: Dict[str, List[str]]
    stagger: Union[int, float, str]  # "200ms"
    until: str  # "head" or "end"


class RequestLogConfig(TypedDict, total=False):
    """The request history the web UI and admin API read."""

    history: int  # finished requests kept in memory
    file: str  # append each finished request as a JSON line; reloaded at start
    file_max: Union[int, str]  # bytes, or "64MiB": rotated to `<file>.1` at this size


class PluginsConfig(TypedDict, total=False):
    """The compiled-in plugins, each under its name; the gateway orders them."""

    routing: RoutingConfig
    fallback: FallbackConfig
    hedge: HedgeConfig
    tool_loop: ToolLoopConfig
    mcp_aggregate: McpAggregateConfig
    request_log: RequestLogConfig


class _Rates(TypedDict):
    input: float
    output: float


class Price(_Rates, total=False):
    """Dollars per million tokens."""

    cached_input: float  # for cached input tokens; `input` if unset


class _NamedKey(TypedDict):
    name: str  # what the request log calls this client
    key: str  # literal or env:NAME


class NamedClientKey(_NamedKey, total=False):
    # Whether the key reaches the admin API (`/ui/api/*`). Unset on every key:
    # all keys do. Once any key sets True, only the keys that do.
    admin: bool
    # The tool loop and aggregate tools (`server__tool` globs) this key's requests
    # may use, within the global allow/deny; all if unset.
    tools: List[str]


class GatewayConfig(TypedDict, total=False):
    """The top level of a gateway configuration."""

    providers: Dict[str, ProviderConfig]
    formats: List[str]  # the formats served to clients
    client_keys: List[Union[str, NamedClientKey]]  # when set, every request needs one; a bare key is named key-1, key-2...
    plugins: PluginsConfig
    listen: str
    body_limit: Union[int, str]  # bytes, or a size such as "32MiB"
    log: str  # "text", "json" or "off"
    ws_message_limit: Union[int, str]  # bytes, or a size such as "16MiB"
    prices: Dict[str, Price]  # by model prefix; requests then get a `cost_usd`
    shutdown_grace: Union[int, float, str]  # seconds, or "30s", "2m", "500ms"
    mcp: Dict[str, McpConfig]
    cors: CorsConfig
    ui: bool  # False also turns off the admin API behind models()/requests()/providers()
