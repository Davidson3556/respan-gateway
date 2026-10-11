"""Lifecycle, introspection, names."""

from __future__ import annotations

import asyncio
import json
import threading

import httpx
import pytest

import respan_gateway as rg
from conftest import CHAT, WORDS, config_for


def test_context_manager_stops_its_servers_and_reopens(fake):
    with rg.Gateway(config=config_for(fake)) as gw:
        server = gw.serve()
        assert httpx.post(server.url + "/v1/chat/completions", json=CHAT).status_code == 200
    with pytest.raises(httpx.TransportError):
        httpx.get(server.url + "/v1/models")
    # Closing releases clients; the gateway still answers.
    assert gw.request("POST", "/v1/chat/completions", json=CHAT).status_code == 200


def test_async_context_manager_and_aserve(fake):
    async def main():
        async with rg.Gateway(config=config_for(fake)) as gw:
            async with gw.aserve() as server:
                async with httpx.AsyncClient() as client:
                    r = await client.post(server.url + "/v1/chat/completions", json=CHAT)
                assert r.status_code == 200
            with pytest.raises(httpx.TransportError):
                httpx.get(server.url + "/v1/models")
            other = gw.serve()
            client = gw.openai_async()
            assert (await client.chat.completions.create(model="fake-1", messages=CHAT["messages"])).choices
        with pytest.raises(httpx.TransportError):
            httpx.get(other.url + "/v1/models")

    asyncio.run(main())


def test_server_async_context_manager_and_repr(gw):
    async def main():
        async with gw.serve() as server:
            assert repr(server) == f"Server(url={server.url!r})"
        with pytest.raises(httpx.TransportError):
            httpx.get(server.url + "/v1/models")

    asyncio.run(main())


def test_server_wait_returns_once_stopped(gw):
    server = gw.serve()
    threading.Timer(0.3, server.stop).start()
    server.wait()


def test_requests_and_providers(gw):
    assert gw.requests() == []
    gw.request("POST", "/v1/chat/completions", json=CHAT)
    gw.request("POST", "/v1/chat/completions", json={**CHAT, "model": "missing"})
    log = gw.requests()
    assert [r["status"] for r in log] == [404, 200]  # newest first
    assert len(gw.requests(limit=1)) == 1
    assert {p["name"] for p in gw.providers()} == {"openai", "any"}
    assert [r["status"] for r in asyncio.run(gw.arequests())] == [404, 200]
    assert {p["name"] for p in asyncio.run(gw.aproviders())} == {"openai", "any"}


def test_export_requests_filters_and_costs(fake, tmp_path):
    history = tmp_path / "requests.jsonl"
    config = {
        **config_for(fake),
        "request_history_file": str(history),
        "prices": {"fake": {"input": 1_000_000, "output": 1_000_000}},
    }
    with rg.Gateway(config=config) as gw:
        gw.request("POST", "/v1/chat/completions", json=CHAT)
        gw.request("POST", "/v1/chat/completions", json={**CHAT, "model": "missing"})
        lines = gw.export_requests().splitlines()
        assert [json.loads(line)["status"] for line in lines] == [200, 404]  # oldest first
        assert len(gw.export_requests(status="error").splitlines()) == 1
        csv_rows = asyncio.run(gw.aexport_requests("csv", kind="chat")).splitlines()
        assert csv_rows[0].startswith("id,started_at,") and len(csv_rows) == 3
        with pytest.raises(rg.GatewayError, match="jsonl or csv"):
            gw.export_requests("xml")
        record = json.loads(lines[0])
        assert record["cost_usd"] == record["usage"]["input_tokens"] + record["usage"]["output_tokens"]
    assert history.exists()


def test_admin_api_off_is_a_gateway_error(fake):
    gw = rg.Gateway(config={**config_for(fake), "ui": False})
    with pytest.raises(rg.GatewayError, match="ui: false"):
        gw.requests()
    assert "providers=[?]" in repr(gw)


def test_gateway_repr(gw, tmp_path):
    assert repr(gw) == "Gateway(source='dict', providers=[openai, any])"
    path = tmp_path / "g.yaml"
    path.write_text(json.dumps({"providers": {"p": {"vendor": "openai", "key": "k"}}}))
    assert repr(rg.Gateway.from_file(path)) == f"Gateway(source='{path}', providers=[p])"


def test_public_names():
    assert issubclass(rg.GatewayTimeout, rg.GatewayError)
    assert rg.__version__ and set(rg.__all__) >= {"Gateway", "Server", "ConfigError", "GatewayTimeout"}


def test_request_timeout_is_an_httpx_timeout(gw):
    with pytest.raises(httpx.ReadTimeout):
        gw.request("POST", "/v1/chat/completions", json={**CHAT, "model": "slow"}, timeout=0.3)


def test_anthropic_async(gw):
    pytest.importorskip("anthropic")

    async def main():
        reply = await gw.anthropic_async().messages.create(
            model="fake-1", max_tokens=16, messages=[{"role": "user", "content": "hi"}]
        )
        assert reply.content[0].text == "".join(WORDS)

    asyncio.run(main())
