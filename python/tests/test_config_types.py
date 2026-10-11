"""The config TypedDicts describe a full config the binary parses."""

from __future__ import annotations

import sys
import tempfile
from typing import Any, Dict, cast

import respan_gateway as rg
from respan_gateway.config import GatewayConfig


def test_a_full_typed_config_parses() -> None:
    config: GatewayConfig = {
        "listen": "127.0.0.1:0",
        "body_limit": "32MiB",
        "ws_message_limit": "16MiB",
        "request_history": 500,
        "request_history_file_max": "64MiB",
        "prices": {"gpt-4o": {"input": 2.5, "output": 10, "cached_input": 1.25}},
        "shutdown_grace": "10s",
        "client_keys": ["client-key", {"name": "ci", "key": "ci-key"}],
        "log": "off",
        "ui": True,
        "admin_writes": False,
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
            "oauthed": {
                "url": "http://127.0.0.1:9/mcp",
                "oauth": {
                    "token_url": "http://127.0.0.1:9/token",
                    "client_id": "id",
                    "client_secret": "secret",
                    "scopes": ["read"],
                    "refresh_token": "refresh",
                },
            },
            "local": {
                "command": [sys.executable],
                "env": {"TOKEN": "literal"},
                "cwd": tempfile.gettempdir(),
            },
        },
        "mcp_aggregate": {"servers": ["remote", "local"], "allow": ["remote__*"], "deny": ["remote__x"]},
        "agent": {
            "servers": ["remote", "local"],
            "allow": ["remote__*"],
            "deny": ["remote__delete_*"],
            "models": ["gpt-"],
            "max_steps": 4,
            "timeout": "2m",
            "tool_timeout": 30,
            "tool_list_ttl": "1m",
            "parallel": 2,
            "max_result": "64KiB",
            "max_results": "256KiB",
        },
        "cors": {"origins": ["https://app.example.com"]},
        "plugins": [{"models": [{"prefix": "gpt-", "provider": "openai"}]}],
        "default": "openai",
    }
    assert rg.Gateway(config=cast(Dict[str, Any], config)) is not None
