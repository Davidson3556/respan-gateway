"""A strict fake OpenAI-compatible provider, and gateways in front of it."""

from __future__ import annotations

import base64
import json
import select
import socket
import struct
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import respan_gateway as rg

KEY = "sk-fake"
WORDS = ["Hello", " from", " the", " fake"]
MODELS = ["fake-1", "text-embedding-3-small", "gpt-image-1", "whisper-1", "tts-1", "sora-2"]
# Bytes no text decoder would pass through unchanged.
AUDIO = b"ID3\x00\xff\xfe\x80audio"
VIDEO = b"\x00\x00\x00\x18ftypmp42\xff\x00video"
PNG_B64 = base64.b64encode(b"\x89PNG\xff\x00image").decode()


MEDIA = ("/v1/images/generations", "/v1/audio/transcriptions", "/v1/audio/speech", "/v1/videos")


class FakeOpenAI:
    """Checks each request the way OpenAI would, and answers in its framing."""

    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.uploads: list[bytes] = []  # multipart bodies
        self.videos: list[str] = []  # ids of the video jobs made
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

            def raw(self, status: int, content_type: str, data: bytes) -> None:
                self.send_response(status)
                self.send_header("content-type", content_type)
                self.send_header("content-length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self) -> None:
                if self.path == "/v1/models":
                    return self.reply(200, {"object": "list", "data": [{"id": m, "object": "model"} for m in MODELS]})
                video = self.path.removeprefix("/v1/videos/")
                if video != self.path and video.split("/")[0] in fake.videos:
                    if video.endswith("/content"):
                        return self.raw(200, "video/mp4", VIDEO)
                    return self.reply(200, fake.job(video))
                self.reply(404, {"error": {"message": f"no route {self.path}"}})

            def embeddings(self, body: dict) -> None:
                # Answers in the form asked: the openai SDK asks for base64 unless told otherwise.
                vector = struct.pack("<3f", 0.25, -0.5, 1.0)
                encoded = base64.b64encode(vector).decode() if body.get("encoding_format") == "base64" else [0.25, -0.5, 1.0]
                self.reply(200, {
                    "object": "list", "model": body["model"],
                    "data": [{"object": "embedding", "index": 0, "embedding": encoded}],
                    "usage": {"prompt_tokens": 2, "total_tokens": 2},
                })

            def media(self, raw: bytes) -> None:
                multipart = self.headers.get("content-type", "").startswith("multipart/form-data")
                if self.path == "/v1/audio/transcriptions":
                    if not multipart or b'name="file"' not in raw:
                        return self.reply(400, {"error": {"message": "file is required"}})
                    fake.uploads.append(raw)
                    return self.reply(200, {"text": "hello from the fake", "usage": {"type": "duration", "seconds": 4}})
                body = {} if multipart else json.loads(raw)
                fake.requests.append(body)
                if self.path == "/v1/images/generations":
                    if not isinstance(body.get("prompt"), str):
                        return self.reply(400, {"error": {"message": "prompt is required"}})
                    return self.reply(200, {"created": 1, "data": [{"b64_json": PNG_B64}],
                                            "usage": {"input_tokens": 9, "output_tokens": 70, "total_tokens": 79}})
                if self.path == "/v1/audio/speech":
                    if not isinstance(body.get("input"), str) or not isinstance(body.get("voice"), str):
                        return self.reply(400, {"error": {"message": "input and voice are required"}})
                    return self.raw(200, "audio/mpeg", AUDIO)
                # /v1/videos: a job that is done as soon as it is asked for.
                if not multipart and not isinstance(body.get("prompt"), str):
                    return self.reply(400, {"error": {"message": "prompt is required"}})
                job_id = f"video_{len(fake.videos) + 1}"
                fake.videos.append(job_id)
                self.reply(200, fake.job(job_id, "queued"))

            def do_POST(self) -> None:
                if self.path not in ("/v1/chat/completions", "/v1/embeddings", *MEDIA):
                    return self.reply(404, {"error": {"message": f"no route {self.path}"}})
                if self.headers.get("authorization") != f"Bearer {KEY}":
                    return self.reply(401, {"error": {"message": "bad key", "type": "invalid_request_error"}})
                raw = self.rfile.read(int(self.headers.get("content-length", 0)))
                if self.path in MEDIA:
                    return self.media(raw)
                body = json.loads(raw)
                fake.requests.append(body)
                if self.path == "/v1/embeddings":
                    return self.embeddings(body)
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

    @staticmethod
    def job(job_id: str, status: str = "completed") -> dict:
        return {"id": job_id, "object": "video", "status": status, "model": "sora-2", "progress": 100,
                "created_at": 1, "seconds": "4", "size": "720x1280"}

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
                "format": ["openai-chat", "openai-embeddings", "openai-images", "openai-transcriptions",
                           "openai-speech", "openai-videos"],
                "base_url": fake.base_url,
                "auth": {"type": "bearer", "key": KEY},
            },
            "any": {"format": "passthrough", "base_url": fake.origin},
        },
        "serve": ["openai-chat", "anthropic-messages", "openai-embeddings", "openai-images",
                  "openai-transcriptions", "openai-speech", "openai-videos", "passthrough"],
        "target": "openai",
    }


@pytest.fixture
def gw(fake):
    return rg.Gateway(config=config_for(fake))


CHAT = {"model": "fake-1", "messages": [{"role": "user", "content": "hi"}]}
