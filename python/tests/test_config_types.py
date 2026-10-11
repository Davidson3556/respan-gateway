"""The config TypedDicts describe a full config the binary parses."""

from __future__ import annotations

from typing import Any, Dict, cast

import respan_gateway as rg
from respan_gateway.config import GatewayConfig


def test_a_full_typed_config_parses() -> None:
    config: GatewayConfig = {
        "listen": "127.0.0.1:0",
        "body_limit": "32MiB",
        "ws_message_limit": "16MiB",
        "request_log": 500,
        "shutdown_grace": "10s",
        "client_keys": ["client-key"],
        "log": "off",
        "ui": True,
        "providers": {
            "openai": {"vendor": "openai", "key": "sk-fake"},
            "custom": {
                "formats": ["openai-chat"],
                "base_url": "http://127.0.0.1:9/v1",
                "auth": {"type": "header", "name": "x-api-key", "key": "k"},
            },
        },
        "formats": ["openai-chat", "anthropic-messages", "mcp"],
        "mcp": {
            "remote": {
                "url": "http://127.0.0.1:9/mcp",
                "key": "fake",
                "forward_auth": False,
            },
            "keyed": {"url": "http://127.0.0.1:9/mcp", "auth": {"type": "bearer", "key": "k"}},
            "local": {
                "command": ["true"],
                "env": {"TOKEN": "literal"},
                "cwd": "/tmp",
            },
        },
        "agent": {
            "tools": ["remote", "local"],
            "models": ["gpt-"],
            "max_steps": 4,
            "timeout": "2m",
            "tool_timeout": 30,
            "cache": "1m",
            "max_result": "64KiB",
        },
        "plugins": [{"models": [{"prefix": "gpt-", "provider": "openai"}]}],
        "default": "openai",
    }
    assert rg.Gateway(config=cast(Dict[str, Any], config)) is not None
