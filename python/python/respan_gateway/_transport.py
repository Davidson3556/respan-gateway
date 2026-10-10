"""httpx transports that deliver requests to an in-process gateway.

Sync requests block (GIL released) in the native side, which wakes to deliver
Ctrl-C. Async requests use the native callback API: the runtime calls back
into the event loop when the head or a body chunk is ready, so a waiting
request holds no thread, and cancelling the awaiting task cancels the work in
flight; closing a response drops the provider call behind it.

httpx timeouts: the read timeout bounds the wait for the response head (the
provider call) and for each body chunk. Connect, write and pool timeouts have
nothing to bound in-process and are ignored.

The classes are built against an httpx-compatible module, because some SDKs
(recent `anthropic`) use `httpx2`, which has the same transport API but whose
classes are distinct from `httpx`'s.
"""

from __future__ import annotations

import asyncio
import functools
from types import ModuleType
from typing import Any, Callable, Optional, Tuple, Type

from . import _native


# The one Host an in-process request carries, whatever URL the client was
# given: the server side only accepts its own names (DNS-rebinding guard).
HOST = "respan-gateway"


def _call(request: Any) -> Tuple[str, str, list]:
    """The method, target and headers for `Native.send`."""
    headers = [
        (k.decode("latin-1"), v.decode("latin-1")) for k, v in request.headers.raw if k.lower() != b"host"
    ]
    headers.append(("host", HOST))
    return request.method, request.url.raw_path.decode("ascii"), headers


def _read_timeout(request: Any) -> Optional[float]:
    return (request.extensions.get("timeout") or {}).get("read")


async def _await(start: Callable[[Callable[[Any, Any], None]], Any]) -> Any:
    """The result of a native call started by `start(done)`, which returns a
    handle with `cancel()`. Cancelling the awaiting task cancels the call."""
    loop = asyncio.get_running_loop()
    future = loop.create_future()

    def settle(value: Any, error: Any) -> None:
        if future.done():  # abandoned meanwhile
            if hasattr(value, "close"):
                value.close()
        elif error is not None:
            future.set_exception(error)
        else:
            future.set_result(value)

    def done(value: Any, error: Any) -> None:  # on a runtime thread
        try:
            loop.call_soon_threadsafe(settle, value, error)
        except RuntimeError:  # the loop is closed
            pass

    pending = start(done)
    try:
        return await future
    except BaseException:
        pending.cancel()
        raise


@functools.lru_cache(maxsize=None)
def transports(httpx: ModuleType) -> Tuple[Type[Any], Type[Any]]:
    """The sync and async transport classes for the module `httpx`."""

    def response(request: Any, reply: "_native.Reply", stream: Any) -> Any:
        headers = [(k.encode("latin-1"), v.encode("latin-1")) for k, v in reply.headers]
        return httpx.Response(reply.status, headers=headers, stream=stream, request=request)

    def timed_out(request: Any) -> Any:
        return httpx.ReadTimeout("the gateway did not answer in time", request=request)

    class SyncBody(httpx.SyncByteStream):  # type: ignore[misc,name-defined]
        def __init__(self, reply: "_native.Reply", request: Any) -> None:
            self._reply = reply
            self._request = request

        def __iter__(self):
            timeout = _read_timeout(self._request)
            while True:
                try:
                    chunk = self._reply.next_chunk(timeout)
                except _native.GatewayTimeout:
                    raise timed_out(self._request) from None
                if chunk is None:
                    return
                yield chunk

        def close(self) -> None:
            self._reply.close()

    class AsyncBody(httpx.AsyncByteStream):  # type: ignore[misc,name-defined]
        def __init__(self, reply: "_native.Reply", request: Any) -> None:
            self._reply = reply
            self._request = request

        async def __aiter__(self):
            timeout = _read_timeout(self._request)
            while True:
                try:
                    chunk = await _await(lambda done: self._reply.next_chunk_async(done, timeout))
                except _native.GatewayTimeout:
                    raise timed_out(self._request) from None
                if chunk is None:
                    return
                yield chunk

        async def aclose(self) -> None:
            self._reply.close()

    class GatewayTransport(httpx.BaseTransport):  # type: ignore[misc,name-defined]
        """Sends every request to the gateway, whatever its host."""

        def __init__(self, native: "_native.Native") -> None:
            self._native = native

        def handle_request(self, request):
            method, target, headers = _call(request)
            try:
                reply = self._native.send(method, target, headers, request.read(), _read_timeout(request))
            except _native.GatewayTimeout:
                raise timed_out(request) from None
            return response(request, reply, SyncBody(reply, request))

    class AsyncGatewayTransport(httpx.AsyncBaseTransport):  # type: ignore[misc,name-defined]
        """Sends every request to the gateway, whatever its host; for asyncio."""

        def __init__(self, native: "_native.Native") -> None:
            self._native = native

        async def handle_async_request(self, request):
            method, target, headers = _call(request)
            body = await request.aread()
            timeout = _read_timeout(request)
            try:
                reply = await _await(
                    lambda done: self._native.send_async(method, target, headers, body, done, timeout)
                )
            except _native.GatewayTimeout:
                raise timed_out(request) from None
            return response(request, reply, AsyncBody(reply, request))

    return GatewayTransport, AsyncGatewayTransport
