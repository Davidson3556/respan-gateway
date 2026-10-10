from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import threading

import httpx
import pytest

import respan_gateway as rg
from conftest import CHAT, KEY, WORDS, config_for


# -- configuration ---------------------------------------------------------


def test_config_from_dict_yaml_and_file(fake, tmp_path):
    cfg = config_for(fake)
    yaml = (
        "providers:\n"
        f"  p: {{ format: openai-chat, base_url: '{fake.base_url}', auth: {{ type: bearer, key: {KEY} }} }}\n"
        "serve: [openai-chat]\ntarget: p\n"
    )
    path = tmp_path / "gateway.yaml"
    path.write_text(yaml)
    for gw in (rg.Gateway(config=cfg), rg.Gateway(config=yaml), rg.Gateway.from_file(path)):
        assert gw.request("POST", "/v1/chat/completions", json=CHAT).status_code == 200


def test_bad_config_raises_config_error(tmp_path):
    with pytest.raises(rg.ConfigError, match="unknown format 'nope'"):
        rg.Gateway(config={"providers": {"p": {"format": "nope", "base_url": "http://x"}}, "serve": []})
    with pytest.raises(rg.ConfigError):
        rg.Gateway(config="serve: [unclosed")
    with pytest.raises(ValueError):  # ConfigError is a ValueError
        rg.Gateway(config={"surprise": 1})
    with pytest.raises(rg.ConfigError, match="missing.yaml"):
        rg.Gateway.from_file(tmp_path / "missing.yaml")


def test_zero_config_from_env(fake, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", KEY)
    monkeypatch.setenv("RESPAN_OPENAI_BASE_URL", fake.base_url)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("RESPAN_ANTHROPIC_BASE_URL", raising=False)
    r = rg.Gateway().request("POST", "/v1/chat/completions", json=CHAT)
    assert r.status_code == 200
    assert r.json()["choices"][0]["message"]["content"] == "".join(WORDS)


def test_zero_config_without_providers_is_an_error(monkeypatch):
    for var in list(os.environ):
        if var.endswith("_API_KEY") or var.startswith("RESPAN_") or var == "OLLAMA_HOST":
            monkeypatch.delenv(var)
    with pytest.raises(rg.ConfigError, match="no provider key"):
        rg.Gateway()


# -- raw requests ----------------------------------------------------------


def test_request_and_json(gw, fake):
    r = gw.request("POST", "/v1/chat/completions", json=CHAT)
    assert r.status_code == 200
    body = r.json()
    assert body["choices"][0]["message"]["content"] == "".join(WORDS)
    assert body["usage"]["total_tokens"] == 7
    assert fake.requests[-1]["messages"] == CHAT["messages"]


def test_http_errors_are_responses(gw):
    r = gw.request("POST", "/v1/chat/completions", json={**CHAT, "model": "missing"})
    assert r.status_code == 404
    assert "model not found" in r.text
    assert gw.request("GET", "/nope").status_code == 404


def test_passthrough_request(gw):
    r = gw.request("GET", "/v1/models", headers={"authorization": f"Bearer {KEY}"})
    assert r.status_code == 200
    assert r.json()["data"][0]["id"] == "fake-1"


def test_stream_yields_chunks_incrementally(gw, fake):
    fake.gate.clear()  # the fake holds back everything after the first words
    chunks = gw.stream("POST", "/v1/chat/completions", json={**CHAT, "stream": True})
    first = next(chunks)  # would hang until the gate opens, if buffered
    assert first.startswith(b"data:")
    fake.gate.set()
    rest = b"".join(chunks)
    text = (first + rest).decode()
    assert "".join(WORDS) == "".join(
        json.loads(line[5:])["choices"][0]["delta"].get("content", "")
        for line in text.splitlines()
        if line.startswith("data:") and "[DONE]" not in line
    )
    assert text.rstrip().endswith("data: [DONE]")


def test_astream_and_arequest(gw):
    async def main():
        r = await gw.arequest("POST", "/v1/chat/completions", json=CHAT)
        assert r.json()["usage"]["total_tokens"] == 7
        chunks = [c async for c in gw.astream("POST", "/v1/chat/completions", json={**CHAT, "stream": True})]
        assert b"[DONE]" in b"".join(chunks)

    asyncio.run(main())


def test_unreachable_provider_is_a_response_not_a_crash(gw, fake):
    fake.close()
    r = gw.request("POST", "/v1/chat/completions", json=CHAT)
    assert r.status_code >= 500


# -- httpx transports and SDKs ---------------------------------------------


def test_httpx_transport(gw):
    with httpx.Client(base_url="http://anything.test", transport=gw.transport()) as client:
        r = client.post("/v1/chat/completions", json=CHAT)
        assert r.status_code == 200 and r.json()["model"] == "fake-1"
        with client.stream("POST", "/v1/chat/completions", json={**CHAT, "stream": True}) as s:
            assert any(line == "data: [DONE]" for line in s.iter_lines())


def test_async_transport(gw):
    async def main():
        async with httpx.AsyncClient(base_url="http://anything.test", transport=gw.async_transport()) as client:
            r = await client.post("/v1/chat/completions", json=CHAT)
            assert r.status_code == 200
            async with client.stream("POST", "/v1/chat/completions", json={**CHAT, "stream": True}) as s:
                lines = [line async for line in s.aiter_lines()]
            assert "data: [DONE]" in lines
            results = await asyncio.gather(*(client.post("/v1/chat/completions", json=CHAT) for _ in range(8)))
            assert all(r.status_code == 200 for r in results)

    asyncio.run(main())


def test_openai_sdk(gw):
    openai = pytest.importorskip("openai")
    client = gw.openai()
    done = client.chat.completions.create(model="fake-1", messages=CHAT["messages"])
    assert done.choices[0].message.content == "".join(WORDS)
    stream = client.chat.completions.create(model="fake-1", messages=CHAT["messages"], stream=True)
    assert "".join(c.choices[0].delta.content or "" for c in stream if c.choices) == "".join(WORDS)
    with pytest.raises(openai.NotFoundError):
        client.chat.completions.create(model="missing", messages=CHAT["messages"])


def test_async_openai_sdk(gw):
    pytest.importorskip("openai")

    async def main():
        client = gw.openai(asynchronous=True)
        done = await client.chat.completions.create(model="fake-1", messages=CHAT["messages"])
        assert done.choices[0].message.content == "".join(WORDS)
        stream = await client.chat.completions.create(model="fake-1", messages=CHAT["messages"], stream=True)
        parts = [c.choices[0].delta.content or "" async for c in stream if c.choices]
        assert "".join(parts) == "".join(WORDS)

    asyncio.run(main())


def test_anthropic_sdk_is_translated_to_openai(gw):
    pytest.importorskip("anthropic")
    reply = gw.anthropic().messages.create(
        model="fake-1", max_tokens=16, messages=[{"role": "user", "content": "hi"}]
    )
    assert reply.content[0].text == "".join(WORDS)


# -- servers ---------------------------------------------------------------


def test_serve_context_manager(gw):
    with gw.serve(port=0) as server:
        assert server.url.startswith("http://127.0.0.1:")
        r = httpx.post(server.url + "/v1/chat/completions", json=CHAT)
        assert r.status_code == 200 and r.json()["usage"]["total_tokens"] == 7
    with pytest.raises(httpx.TransportError):
        httpx.get(server.url + "/v1/models")
    server.stop()  # idempotent


def test_serve_bad_address_is_a_gateway_error(gw):
    with gw.serve() as taken:
        port = int(taken.url.rsplit(":", 1)[1])
        with pytest.raises(rg.GatewayError, match="listen on"):
            gw.serve(port=port)


def test_serve_forever_stops_on_keyboard_interrupt(gw):
    import _thread

    def interrupt():
        threading.Event().wait(0.5)
        _thread.interrupt_main()

    threading.Thread(target=interrupt, daemon=True).start()
    gw.serve_forever(port=0)  # returns


# -- command line ----------------------------------------------------------


def test_console_script_version():
    out = subprocess.run([sys.executable, "-m", "respan_gateway", "--version"], capture_output=True, text=True)
    assert out.returncode == 0
    assert out.stdout.startswith("respan-gateway ")


def test_cli_check_and_bad_usage(fake, tmp_path):
    ok = tmp_path / "gateway.yaml"
    ok.write_text("providers: {}\nserve: []\n")
    run = lambda *a: subprocess.run([sys.executable, "-m", "respan_gateway", *a], capture_output=True, text=True)
    assert run("-c", str(ok), "check").stdout.strip().endswith(": ok")
    assert run("--bogus").returncode == 2


# -- timeouts, cancellation, threads ---------------------------------------


def until(event: threading.Event, seconds: float = 3) -> bool:
    return event.wait(seconds)


def test_sync_timeout_stops_the_provider_call(gw, fake):
    import time

    import openai

    client = gw.openai(timeout=0.5, max_retries=0)
    start = time.monotonic()
    with pytest.raises(openai.APITimeoutError):
        client.chat.completions.create(model="slow", messages=CHAT["messages"])
    assert time.monotonic() - start < 3
    assert until(fake.disconnected)


def test_async_timeout_stops_the_provider_call(gw, fake):
    import time

    import openai

    async def main():
        client = gw.openai(asynchronous=True, timeout=0.5, max_retries=0)
        with pytest.raises(openai.APITimeoutError):
            await client.chat.completions.create(model="slow", messages=CHAT["messages"])

    start = time.monotonic()
    asyncio.run(main())
    assert time.monotonic() - start < 3  # no executor thread holds the exit
    assert until(fake.disconnected)


def test_async_cancellation_stops_the_provider_call(gw, fake):
    import time

    async def main():
        client = gw.openai(asynchronous=True)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(client.chat.completions.create(model="slow", messages=CHAT["messages"]), 0.5)

    start = time.monotonic()
    asyncio.run(main())
    assert time.monotonic() - start < 3
    assert until(fake.disconnected)


def test_read_timeout_between_stream_chunks(gw, fake):
    body = {**CHAT, "model": "stall", "stream": True}
    with httpx.Client(transport=gw.transport(), base_url="http://respan-gateway") as client:
        with client.stream("POST", "/v1/chat/completions", json=body, timeout=httpx.Timeout(0.5)) as r:
            assert r.status_code == 200
            with pytest.raises(httpx.ReadTimeout):
                for _ in r.iter_raw():
                    pass
    assert until(fake.disconnected)  # closing the response hangs up on the provider


def test_async_read_timeout_between_stream_chunks(gw, fake):
    async def main():
        body = {**CHAT, "model": "stall", "stream": True}
        async with httpx.AsyncClient(transport=gw.async_transport(), base_url="http://respan-gateway") as client:
            async with client.stream("POST", "/v1/chat/completions", json=body, timeout=httpx.Timeout(0.5)) as r:
                with pytest.raises(httpx.ReadTimeout):
                    async for _ in r.aiter_raw():
                        pass

    asyncio.run(main())
    assert until(fake.disconnected)


def test_many_open_streams_do_not_starve_other_requests(gw, fake):
    import time

    async def main():
        body = {**CHAT, "model": "stall", "stream": True}
        async with httpx.AsyncClient(transport=gw.async_transport(), base_url="http://respan-gateway",
                                     timeout=None) as client:
            async def open_stream():
                async with client.stream("POST", "/v1/chat/completions", json=body) as r:
                    async for _ in r.aiter_raw():
                        pass

            streams = [asyncio.create_task(open_stream()) for _ in range(64)]
            await asyncio.sleep(1)  # every one is now waiting for a chunk
            start = time.monotonic()
            r = await client.post("/v1/chat/completions", json=CHAT)
            assert r.status_code == 200
            assert time.monotonic() - start < 2
            for s in streams:
                s.cancel()
            await asyncio.gather(*streams, return_exceptions=True)

    asyncio.run(main())


def test_requests_send_the_allowed_host_whatever_the_url(gw):
    from respan_gateway._transport import HOST, _call

    assert HOST == "respan-gateway"
    for url in ("http://evil.example:9/v1/models", "http://respan-gateway/v1/models", "http://localhost/v1/models"):
        request = httpx.Request("GET", url, headers={"Host": "attacker.example"})
        hosts = [v for k, v in _call(request)[2] if k.lower() == "host"]
        assert hosts == ["respan-gateway"]
    # And through a real client aimed at another host.
    with httpx.Client(transport=gw.transport()) as client:
        assert client.get("http://evil.example/v1/models").status_code == 200


def test_bare_serve_keeps_running(gw):
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    gw.serve(port=port)  # the handle is dropped at once
    import gc
    import time

    gc.collect()
    time.sleep(0.3)
    assert httpx.post(f"http://127.0.0.1:{port}/v1/chat/completions", json=CHAT).status_code == 200


def test_gateway_reads_gateway_yaml_in_the_working_directory(fake, tmp_path, monkeypatch):
    (tmp_path / "gateway.yaml").write_text(json.dumps(config_for(fake)))
    for var in list(os.environ):
        if var.endswith("_API_KEY") or var.startswith("RESPAN_"):
            monkeypatch.delenv(var)
    monkeypatch.chdir(tmp_path)
    r = rg.Gateway().request("POST", "/v1/chat/completions", json=CHAT)
    assert r.status_code == 200 and r.json()["choices"][0]["message"]["content"] == "".join(WORDS)


def test_cli_exits_quietly_on_sigint(tmp_path):
    import signal
    import time

    cfg = tmp_path / "gateway.yaml"
    cfg.write_text("providers: {}\nserve: []\n")
    proc = subprocess.Popen(
        [sys.executable, "-m", "respan_gateway", "-c", str(cfg), "--listen", "127.0.0.1:0"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    time.sleep(1.5)
    proc.send_signal(signal.SIGINT)
    out, err = proc.communicate(timeout=15)
    assert proc.returncode == 0, (out, err)
    assert "Traceback" not in err and "KeyboardInterrupt" not in err
