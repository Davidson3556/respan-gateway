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
    format: Union[str, List[str]]  # e.g. "openai-chat", ["openai-images", "openai-embeddings"]
    base_url: str
    key: str  # literal or "env:NAME"
    auth: Auth


class PluginConfig(TypedDict, total=False):
    name: str
    after: List[str]
    models: List[Dict[str, str]]  # [{"prefix": "claude-", "provider": "anthropic"}]


class GatewayConfig(TypedDict, total=False):
    """The top level of a gateway configuration."""

    providers: Dict[str, ProviderConfig]
    serve: List[str]  # the formats served to clients
    target: str  # the default provider
    keys: List[str]  # client keys; when set, every request needs one
    plugins: List[PluginConfig]
    listen: str
    body_limit: int
    log: bool
    ui: bool  # False also turns off the admin API behind models()/requests()/providers()
