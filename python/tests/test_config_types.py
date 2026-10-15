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
        "models": {"openai/gpt-4o": {"pricing": {"input": 2.5, "output": 10, "cached_input": 1.25}}},
        "shutdown_grace": "10s",
        "client_keys": ["client-key", {"name": "ci", "key": "ci-key"}],
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
        "routes": {
            "smart": {
                "to": ["openai/gpt-4o", {"target": "custom/{model}", "weight": 3}],
                "pick": "weighted",
                "needs": ["tools"],
                "when": {"clients": ["ci"], "kinds": ["chat"]},
                "fallback": True,
                "tries": 2,
                "backoff": "100ms",
                "max_backoff": "5s",
                "until": "head",
            },
            "fast": {"to": ["openai", "custom"], "pick": "fastest", "hedge": "200ms", "until": "end"},
            "llama-*": {"to": "custom"},
        },
        "mcp": {
            "remote": {
                "url": "http://127.0.0.1:9/mcp",
                "auth": {"bearer": "fake"},
            },
            "headered": {"url": "http://127.0.0.1:9/mcp", "headers": {"x-api-key": "k"}},
            "forwarded": {"url": "http://127.0.0.1:9/mcp", "auth": "forward"},
            "oauthed": {
                "url": "http://127.0.0.1:9/mcp",
                "auth": {
                    "oauth": {
                        "token_url": "http://127.0.0.1:9/token",
                        "client_id": "id",
                        "client_secret": "secret",
                        "scopes": ["read"],
                        "refresh_token": "refresh",
                    },
                },
            },
            "local": {
                "command": sys.executable,
                "args": ["-c", "pass"],
                "env": {"TOKEN": "literal"},
                "cwd": tempfile.gettempdir(),
            },
        },
        "cors": {"origins": ["https://app.example.com"]},
        "plugins": {
            "routing": {"default": "openai"},
            "request_log": {"history": 500, "file_max": "64MiB"},
            "mcp_aggregate": {"servers": ["remote", "local"], "allow": ["remote__*"], "deny": ["remote__x"]},
            "tool_loop": {
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
        },
    }
    assert rg.Gateway(config=cast(Dict[str, Any], config)) is not None
