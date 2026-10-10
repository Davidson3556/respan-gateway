"""httpx transports that deliver requests to an in-process gateway.

The native side blocks (with the GIL released) until the response head and
then each body chunk arrive. The async transport runs those same blocking
calls on the event loop's thread-pool executor: a pending call can always be
interrupted by closing the response, so cancellation works, and no second
(async) native API has to be kept in step with the first.

The classes are built against an httpx-compatible module, because some SDKs
(recent `anthropic`) use `httpx2`, which has the same transport API but whose
classes are distinct from `httpx`'s.
"""

from __future__ import annotations

import asyncio
import functools
from types import ModuleType
from typing import Any, Tuple, Type

from . import _native


def _call(request: Any) -> Tuple[str, str, list]:
    """The method, target and headers for `Native.send`."""
    headers = [(k.decode("latin-1"), v.decode("latin-1")) for k, v in request.headers.raw]
    return request.method, request.url.raw_path.decode("ascii"), headers


@functools.lru_cache(maxsize=None)
def transports(httpx: ModuleType) -> Tuple[Type[Any], Type[Any]]:
    """The sync and async transport classes for the module `httpx`."""

    def response(request: Any, reply: "_native.Reply", stream: Any) -> Any:
        headers = [(k.encode("latin-1"), v.encode("latin-1")) for k, v in reply.headers]
        return httpx.Response(reply.status, headers=headers, stream=stream, request=request)

    class SyncBody(httpx.SyncByteStream):  # type: ignore[misc,name-defined]
        def __init__(self, reply: "_native.Reply") -> None:
            self._reply = reply

        def __iter__(self):
            while (chunk := self._reply.next_chunk()) is not None:
                yield chunk

        def close(self) -> None:
            self._reply.close()

    class AsyncBody(httpx.AsyncByteStream):  # type: ignore[misc,name-defined]
        def __init__(self, reply: "_native.Reply") -> None:
            self._reply = reply

        async def __aiter__(self):
            loop = asyncio.get_running_loop()
            while (chunk := await loop.run_in_executor(None, self._reply.next_chunk)) is not None:
                yield chunk

        async def aclose(self) -> None:
            self._reply.close()

    class GatewayTransport(httpx.BaseTransport):  # type: ignore[misc,name-defined]
        """Sends every request to the gateway, whatever its host."""

        def __init__(self, native: "_native.Native") -> None:
            self._native = native

        def handle_request(self, request):
            method, target, headers = _call(request)
            reply = self._native.send(method, target, headers, request.read())
            return response(request, reply, SyncBody(reply))

    class AsyncGatewayTransport(httpx.AsyncBaseTransport):  # type: ignore[misc,name-defined]
        """Sends every request to the gateway, whatever its host; for asyncio."""

        def __init__(self, native: "_native.Native") -> None:
            self._native = native

        async def handle_async_request(self, request):
            method, target, headers = _call(request)
            body = await request.aread()
            loop = asyncio.get_running_loop()
            reply = await loop.run_in_executor(None, self._native.send, method, target, headers, body)
            return response(request, reply, AsyncBody(reply))

    return GatewayTransport, AsyncGatewayTransport
