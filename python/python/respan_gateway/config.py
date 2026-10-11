"""Types for the configuration mapping, for editor completion and checking.

They describe what `Gateway(config=...)` takes (the format of
`gateway.example.yaml`). At run time a config is a plain dict, and a mistake
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


class McpConfig(TypedDict, total=False):
    """One MCP server, served at /mcp/{name}: an HTTP `url` or a stdio `command`."""

    url: str  # a streamable-HTTP endpoint
    key: str  # literal or "env:NAME", sent as a bearer token
    auth: Auth  # one header, in place of `key`
    forward_auth: bool  # send the client's credentials when there is no key
    command: List[str]  # a stdio server's program and arguments
    env: Dict[str, str]  # values literal or "env:NAME"
    cwd: str


class AgentConfig(TypedDict, total=False):
    """The agent loop: the gateway runs the tools of `mcp` servers inside chat requests."""

    tools: List[str]  # the `mcp` servers whose tools are offered
    models: List[str]  # model prefixes the loop applies to; every chat model if unset
    max_steps: int
    timeout: Union[int, float, str]  # seconds, or "5m"
    tool_timeout: Union[int, float, str]
    cache: Union[int, float, str]  # how long a server's tool list is kept
    max_result: Union[int, str]  # bytes, or a size such as "64KiB"


class PluginConfig(TypedDict, total=False):
    models: List[Dict[str, str]]  # [{"prefix": "claude-", "provider": "anthropic"}]


class GatewayConfig(TypedDict, total=False):
    """The top level of a gateway configuration."""

    providers: Dict[str, ProviderConfig]
    formats: List[str]  # the formats served to clients
    default: str  # the default provider
    client_keys: List[str]  # when set, every request needs one
    plugins: List[PluginConfig]
    listen: str
    body_limit: Union[int, str]  # bytes, or a size such as "32MiB"
    log: str  # "text", "json" or "off"
    ws_message_limit: Union[int, str]  # bytes, or a size such as "16MiB"
    request_log: int  # finished requests the web UI keeps
    shutdown_grace: Union[int, float, str]  # seconds, or "30s", "2m", "500ms"
    mcp: Dict[str, McpConfig]
    agent: AgentConfig
    ui: bool  # False also turns off the admin API behind models()/requests()/providers()
