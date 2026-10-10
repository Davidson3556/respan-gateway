"""A strict fake OpenAI-compatible provider, and gateways in front of it."""

from __future__ import annotations

import json
import select
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import respan_gateway as rg

KEY = "sk-fake"
WORDS = ["Hello", " from", " the", " fake"]


class FakeOpenAI:
    """Checks each request the way OpenAI would, and answers in its framing."""

    def __init__(self) -> None:
        self.requests: list[dict] = []
        # Set to let a streamed reply continue after its first chunk.
        self.gate = threading.Event()
        self.gate.set()
        # Set when a client hangs up while a "slow" or "stall" reply waits.
        self.disconnected = threading.Event()
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args) -> None:
                pass

            def reply(self, status: int, body: dict) -> None:
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def hang(self, seconds: float) -> bool:
                """Waits; returns True (and records it) if the client hung up."""
                deadline = time.monotonic() + seconds
                while time.monotonic() < deadline:
                    readable, _, _ = select.select([self.connection], [], [], 0.05)
                    if readable and self.connection.recv(1, socket.MSG_PEEK) == b"":
                        fake.disconnected.set()
                        return True
                return False

            def do_GET(self) -> None:
                if self.path == "/v1/models":
                    self.reply(200, {"object": "list", "data": [{"id": "fake-1", "object": "model"}]})
                else:
                    self.reply(404, {"error": {"message": f"no route {self.path}"}})

            def do_POST(self) -> None:
                if self.path != "/v1/chat/completions":
                    return self.reply(404, {"error": {"message": f"no route {self.path}"}})
                if self.headers.get("authorization") != f"Bearer {KEY}":
                    return self.reply(401, {"error": {"message": "bad key", "type": "invalid_request_error"}})
                raw = self.rfile.read(int(self.headers.get("content-length", 0)))
                body = json.loads(raw)
                fake.requests.append(body)
                if not isinstance(body.get("model"), str) or not body.get("messages"):
                    return self.reply(400, {"error": {"message": "model and messages are required"}})
                if body["model"] == "missing":
                    return self.reply(404, {"error": {"message": "model not found", "type": "invalid_request_error"}})
                if body["model"] == "slow" and self.hang(5):  # no head for 5 s
                    return
                if body["model"] == "stall" and body.get("stream"):  # a head, then silence
                    self.send_response(200)
                    self.send_header("content-type", "text/event-stream")
                    self.end_headers()
                    self.chunk("stall", {"role": "assistant", "content": "Hi"})
                    self.hang(8)
                    return
                if body.get("stream"):
                    return self.stream(body["model"])
                self.reply(200, {
                    "id": "chatcmpl-1", "object": "chat.completion", "created": 1, "model": body["model"],
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": "".join(WORDS)},
                                 "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": 3, "completion_tokens": 4, "total_tokens": 7},
                })

            def chunk(self, model: str, delta: dict, finish: str | None = None) -> None:
                event = {"id": "chatcmpl-1", "object": "chat.completion.chunk", "created": 1, "model": model,
                         "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
                self.wfile.write(f"data: {json.dumps(event)}\n\n".encode())
                self.wfile.flush()

            def stream(self, model: str) -> None:
                self.send_response(200)
                self.send_header("content-type", "text/event-stream")
                self.end_headers()
                self.chunk(model, {"role": "assistant", "content": ""})
                for i, word in enumerate(WORDS):
                    if i == 1:
                        fake.gate.wait(10)
                    self.chunk(model, {"content": word})
                self.chunk(model, {}, "stop")
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.origin = f"http://127.0.0.1:{self.server.server_port}"

    @property
    def base_url(self) -> str:
        return self.origin + "/v1"

    def close(self) -> None:
        self.gate.set()
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def fake():
    f = FakeOpenAI()
    yield f
    f.close()


def config_for(fake: FakeOpenAI) -> dict:
    return {
        "providers": {
            "openai": {
                "format": "openai-chat",
                "base_url": fake.base_url,
                "auth": {"type": "bearer", "key": KEY},
            },
            "any": {"format": "passthrough", "base_url": fake.origin},
        },
        "serve": ["openai-chat", "anthropic-messages", "passthrough"],
        "target": "openai",
    }


@pytest.fixture
def gw(fake):
    return rg.Gateway(config=config_for(fake))


CHAT = {"model": "fake-1", "messages": [{"role": "user", "content": "hi"}]}
