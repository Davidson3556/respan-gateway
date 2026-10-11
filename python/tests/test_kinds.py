"""Every kind through the OpenAI SDK, in process, against the fake provider."""

from __future__ import annotations

import asyncio
import base64

import pytest

import respan_gateway as rg
from conftest import AUDIO, PNG_B64, VIDEO, config_for

openai = pytest.importorskip("openai")


def test_images(gw, fake):
    done = gw.openai().images.generate(model="gpt-image-1", prompt="a red fox")
    assert base64.b64decode(done.data[0].b64_json) == base64.b64decode(PNG_B64)
    assert done.usage.total_tokens == 79
    assert fake.requests[-1]["prompt"] == "a red fox"


def test_transcription_uploads_the_file(gw, fake):
    file = ("a.wav", b"RIFF\0\0\0\0WAVE", "audio/wav")
    done = gw.openai().audio.transcriptions.create(model="whisper-1", file=file)
    assert done.text == "hello from the fake"
    assert b"RIFF\0\0\0\0WAVE" in fake.uploads[-1]


def test_speech_is_bytes(gw):
    audio = gw.openai().audio.speech.create(model="tts-1", voice="alloy", input="hello")
    assert audio.read() == AUDIO


def test_embeddings_travel_as_base64(gw, fake):
    done = gw.openai().embeddings.create(model="text-embedding-3-small", input="hi")
    assert fake.requests[-1]["encoding_format"] == "base64"
    assert done.data[0].embedding == [0.25, -0.5, 1.0]


def test_video_job_create_poll_download(gw):
    client = gw.openai()
    job = client.videos.create(model="sora-2", prompt="waves")
    assert job.status == "queued"
    assert client.videos.retrieve(job.id).status == "completed"
    assert client.videos.download_content(job.id).read() == VIDEO
    with pytest.raises(openai.NotFoundError):
        client.videos.retrieve("video_nope")


def test_async_kinds(gw):
    async def main():
        client = gw.openai_async()
        image = await client.images.generate(model="gpt-image-1", prompt="a red fox")
        assert image.data[0].b64_json == PNG_B64
        audio = await client.audio.speech.create(model="tts-1", voice="alloy", input="hello")
        assert audio.read() == AUDIO
        text = await client.audio.transcriptions.create(model="whisper-1", file=("a.wav", b"RIFF", "audio/wav"))
        assert text.text == "hello from the fake"

    asyncio.run(main())


@pytest.fixture
def gw(fake):
    """Without `passthrough`, which would relay `/v1/models` to one provider unread."""
    config = config_for(fake)
    del config["providers"]["any"]
    config["formats"].remove("passthrough")
    return rg.Gateway(config=config)


def test_models_lists_every_kind(gw):
    models = {m["id"]: m for m in gw.models()}
    kinds = {i: m["kind"] for i, m in models.items()}
    assert kinds == {
        "fake-1": "chat", "text-embedding-3-small": "embedding", "gpt-image-1": "image",
        "whisper-1": "transcription", "tts-1": "speech", "sora-2": "video",
    }
    assert models["gpt-image-1"]["owned_by"] == "openai"
    assert [m["id"] for m in gw.models(kind="image")] == ["gpt-image-1"]
    assert gw.models(kind="realtime") == []
    assert [m["id"] for m in asyncio.run(gw.amodels("speech"))] == ["tts-1"]


def test_client_keys(fake):
    config = {**config_for(fake), "client_keys": ["gw-secret"]}
    with rg.Gateway(config=config) as gw:
        with pytest.raises(openai.AuthenticationError):
            gw.openai().images.generate(model="gpt-image-1", prompt="x")
        assert gw.openai(api_key="gw-secret").images.generate(model="gpt-image-1", prompt="x").data
