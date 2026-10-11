"""A gateway listening on a real socket."""

from __future__ import annotations

from typing import Any

from . import _native
from ._transport import _await


class Server:
    """A gateway serving on a real socket in the background.

    A context manager (`with` and `async with`) that stops on exit. Dropping
    the object leaves the server running until the process ends.
    """

    def __init__(self, native: _native.Server) -> None:
        self._native = native

    @property
    def url(self) -> str:
        """The base URL, e.g. `http://127.0.0.1:54321`."""
        return self._native.url

    @property
    def banner(self) -> str:
        """What the binary prints on start: providers, endpoints, the UI."""
        return self._native.banner

    def stop(self) -> None:
        """Stops accepting, lets open responses finish (up to 30 s) and returns. Idempotent."""
        self._native.stop()

    async def astop(self) -> None:
        """`stop` for asyncio."""
        await _await(self._native.stop_async)

    def wait(self) -> None:
        """Blocks until the server ends; Ctrl-C raises `KeyboardInterrupt`."""
        self._native.wait()

    def __enter__(self) -> Server:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.stop()

    async def __aenter__(self) -> Server:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.astop()

    def __repr__(self) -> str:
        return f"Server(url={self.url!r})"
