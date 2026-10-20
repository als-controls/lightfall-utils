"""Qt-free asyncio NATS core: one connection, one capability."""

from __future__ import annotations

import inspect
import json
import logging
import ssl
from collections.abc import Awaitable, Callable
from typing import Any

import nats
from nats.aio.msg import Msg

from lightfall_utils.ipc.capability import (
    ConnectionCapability,
    new_capability,
    private_request,
)
from lightfall_utils.ipc.services.protocol import DEFAULT_TIMEOUT_S

_PRIV_PREFIX = "_PRIV."
log = logging.getLogger(__name__)

ConnCallback = Callable[[], Any]  # sync or async, no arguments


class NatsCore:
    """Asyncio NATS connection bound to a single :class:`ConnectionCapability`."""

    def __init__(
        self,
        url: str,
        *,
        capability: ConnectionCapability | None = None,
        tls: ssl.SSLContext | None = None,
        name: str = "",
        max_reconnect_attempts: int = 60,
        on_disconnected: ConnCallback | None = None,
        on_reconnected: ConnCallback | None = None,
        on_closed: ConnCallback | None = None,
    ) -> None:
        """``max_reconnect_attempts=-1`` keeps reconnecting forever (nats-py's default is
        60 attempts 2 s apart, after which the connection closes for good).  The ``on_*``
        callbacks (sync or async, no arguments) run on the loop when the broker connection
        drops, comes back, or closes permanently (also after :meth:`close`)."""
        self._url = url
        self._cap = capability or new_capability()
        self._tls = tls
        self._name = name
        self._max_reconnect = max_reconnect_attempts
        self._on_disconnected = on_disconnected
        self._on_reconnected = on_reconnected
        self._on_closed = on_closed
        self._nc: Any = None

    @property
    def capability(self) -> ConnectionCapability:
        return self._cap

    @property
    def priv_root(self) -> str:
        return self._cap.priv_root

    @property
    def is_connected(self) -> bool:
        nc = self._nc
        return nc is not None and bool(getattr(nc, "is_connected", False))

    @staticmethod
    def _wrap(cb: ConnCallback | None, what: str):
        if cb is None:
            return None

        async def run() -> None:
            try:
                r = cb()
                if inspect.isawaitable(r):
                    await r
            except Exception:
                log.exception("NatsCore %s callback failed", what)

        return run

    async def connect(self) -> None:
        kwargs: dict[str, Any] = dict(self._cap.connect_kwargs())
        if self._name:
            kwargs["name"] = self._name
        if self._tls is not None:
            kwargs["tls"] = self._tls
        kwargs["max_reconnect_attempts"] = self._max_reconnect
        for key, cb, what in (
            ("disconnected_cb", self._on_disconnected, "disconnected"),
            ("reconnected_cb", self._on_reconnected, "reconnected"),
            ("closed_cb", self._on_closed, "closed"),
        ):
            wrapped = self._wrap(cb, what)
            if wrapped is not None:
                kwargs[key] = wrapped
        self._nc = await nats.connect(self._url, **kwargs)

    async def close(self) -> None:
        nc, self._nc = self._nc, None
        if nc is not None:
            await nc.drain()

    def _conn(self) -> Any:
        if self._nc is None:
            raise RuntimeError("not connected")
        return self._nc

    async def flush(self, timeout: float = DEFAULT_TIMEOUT_S) -> None:
        """Return once every command issued so far (SUB, PUB, UNSUB) has reached the server.

        nats-py's ``flush()`` writes its PING straight to the socket, ahead of commands still
        in the client's pending buffer, so its PONG can arrive before those SUBs are applied.
        Push the buffer out first.
        """
        nc = self._conn()
        push = getattr(nc, "_flush_pending", None)
        if push is not None:
            await push(force_flush=True)
        await nc.flush(timeout=timeout)

    async def subscribe(self, subject: str, cb: Callable[[Msg], Awaitable[None]]):
        return await self._conn().subscribe(subject, cb=cb)

    async def publish(
        self, subject: str, data: bytes, headers: dict[str, str] | None = None
    ) -> None:
        await self._conn().publish(subject, data, headers=headers)

    async def publish_json(self, subject: str, obj: Any) -> None:
        await self.publish(subject, json.dumps(obj).encode())

    async def request_public(
        self, subject: str, payload: dict, timeout: float = DEFAULT_TIMEOUT_S
    ) -> dict:
        if subject.startswith(_PRIV_PREFIX):
            raise ValueError("request_public may not target a _PRIV.* subject")
        msg = await self._conn().request(subject, json.dumps(payload).encode(), timeout=timeout)
        return json.loads(msg.data)

    async def request_private(
        self, subject: str, payload: dict, timeout: float = DEFAULT_TIMEOUT_S
    ) -> dict:
        return await private_request(
            self._conn(), subject, payload, timeout, inbox_prefix=self._cap.inbox_prefix
        )
