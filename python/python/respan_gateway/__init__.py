"""respan-gateway: an LLM gateway you can run in-process or as a server.

    import respan_gateway as rg

    gw = rg.Gateway(config={...})   # or rg.Gateway() to use OPENAI_API_KEY etc.
    client = gw.openai()            # an openai.OpenAI that talks to gw directly
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, AsyncIterator, Iterator, Mapping, Optional, Union

import httpx

from . import _native
from ._native import ConfigError, GatewayError, Server, __version__
from ._transport import transports

__all__ = [
    "ConfigError",
    "Gateway",
    "GatewayError",
    "Server",
    "__version__",
]

# The host in URLs of in-process requests; the transport ignores it.
_BASE = "http://respan-gateway"


class Gateway:
    """A configured gateway.

    `config` is a mapping, or a YAML string, in the format of
    `gateway.example.yaml`. Without it, the gateway is built from the
    environment, as the binary is with no config file: every vendor whose
    key is set (`OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `GEMINI_API_KEY`,
    ...), with `RESPAN_<VENDOR>_BASE_URL` to point one elsewhere.

    Raises `ConfigError` (a `ValueError`) if the configuration is invalid.
    """

    def __init__(self, config: Union[Mapping[str, Any], str, None] = None) -> None:
        if config is None:
            self._native = _native.Native.from_env()
        elif isinstance(config, str):
            self._native = _native.Native.from_yaml(config)
        else:
            # JSON is YAML.
            self._native = _native.Native.from_yaml(json.dumps(config))

    @classmethod
    def from_file(cls, path: Union[str, "os.PathLike[str]"]) -> "Gateway":
        """A gateway from the YAML file at `path`."""
        gateway = cls.__new__(cls)
        gateway._native = _native.Native.from_file(os.fspath(path))
        return gateway

    def transport(self) -> httpx.BaseTransport:
        """An httpx transport that sends requests to this gateway in-process."""
        return transports(httpx)[0](self._native)

    def async_transport(self) -> httpx.AsyncBaseTransport:
        """An httpx async transport that sends requests to this gateway in-process."""
        return transports(httpx)[1](self._native)

    def openai(self, *, api_key: Optional[str] = None, asynchronous: bool = False, **kwargs: Any) -> Any:
        """An `openai.OpenAI` (or `AsyncOpenAI`) client wired to this gateway.

        `api_key` is one of the config's client `keys` if it sets any;
        otherwise a placeholder, which reaches a provider only if the gateway
        holds no key of its own for it. Other keyword arguments go to the
        client.
        """
        try:
            import openai
        except ImportError:
            raise ImportError("gw.openai() needs the openai package: pip install openai") from None
        if asynchronous:
            http = httpx.AsyncClient(transport=self.async_transport())
            return openai.AsyncOpenAI(base_url=_BASE + "/v1", api_key=api_key or "unused", http_client=http, **kwargs)
        http = httpx.Client(transport=self.transport())
        return openai.OpenAI(base_url=_BASE + "/v1", api_key=api_key or "unused", http_client=http, **kwargs)

    def anthropic(self, *, api_key: Optional[str] = None, asynchronous: bool = False, **kwargs: Any) -> Any:
        """An `anthropic.Anthropic` (or `AsyncAnthropic`) client wired to this gateway.

        `api_key` is as in `openai()`.
        """
        try:
            import anthropic
        except ImportError:
            raise ImportError("gw.anthropic() needs the anthropic package: pip install anthropic") from None

        def build(http: Any) -> Any:
            transport = transports(http)[1 if asynchronous else 0](self._native)
            client = (http.AsyncClient if asynchronous else http.Client)(transport=transport)
            cls = anthropic.AsyncAnthropic if asynchronous else anthropic.Anthropic
            return cls(base_url=_BASE, api_key=api_key or "unused", http_client=client, **kwargs)

        try:
            return build(httpx)
        except TypeError:
            # Recent SDK versions use httpx2, whose clients are not httpx's.
            try:
                import httpx2
            except ImportError:
                raise TypeError("the anthropic package does not accept an httpx client") from None
            return build(httpx2)

    def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Sends one request and reads the whole response.

        `kwargs` are those of `httpx.Client.request` (`json`, `content`,
        `headers`, `params`...). An HTTP error status is a response, not an
        exception.
        """
        with httpx.Client(base_url=_BASE, transport=self.transport()) as client:
            return client.request(method, path, **kwargs)

    async def arequest(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """`request` for asyncio."""
        async with httpx.AsyncClient(base_url=_BASE, transport=self.async_transport()) as client:
            return await client.request(method, path, **kwargs)

    def stream(self, method: str, path: str, **kwargs: Any) -> Iterator[bytes]:
        """Sends one request and yields the response body as it arrives."""
        with httpx.Client(base_url=_BASE, transport=self.transport()) as client:
            with client.stream(method, path, **kwargs) as response:
                yield from response.iter_raw()

    async def astream(self, method: str, path: str, **kwargs: Any) -> AsyncIterator[bytes]:
        """`stream` for asyncio."""
        async with httpx.AsyncClient(base_url=_BASE, transport=self.async_transport()) as client:
            async with client.stream(method, path, **kwargs) as response:
                async for chunk in response.aiter_raw():
                    yield chunk

    def serve(self, host: str = "127.0.0.1", port: int = 0) -> Server:
        """Serves this gateway on a real socket in the background.

        Returns a `Server` (`.url`, `.stop()`), also a context manager that
        stops on exit. `port=0` picks a free port. Without client `keys` in
        the config, anyone who reaches it spends your keys: keep it local.
        """
        return self._native.serve(host, port)

    def serve_forever(self, host: str = "127.0.0.1", port: int = 8080) -> None:
        """Serves until interrupted (Ctrl-C)."""
        with self.serve(host, port) as server:
            print(server.banner, flush=True)
            try:
                while True:
                    time.sleep(0.2)  # wakes to deliver KeyboardInterrupt
            except KeyboardInterrupt:
                pass
