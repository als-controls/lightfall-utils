"""Qt-free asyncio NATS core: one connection, one capability."""

from __future__ import annotations

import json
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


class NatsCore:
    """Asyncio NATS connection bound to a single :class:`ConnectionCapability`."""

    def __init__(
        self,
        url: str,
        *,
        capability: ConnectionCapability | None = None,
        tls: ssl.SSLContext | None = None,
        name: str = "",
    ) -> None:
        self._url = url
        self._cap = capability or new_capability()
        self._tls = tls
        self._name = name
        self._nc: Any = None

    @property
    def capability(self) -> ConnectionCapability:
        return self._cap

    @property
    def priv_root(self) -> str:
        return self._cap.priv_root

    async def connect(self) -> None:
        kwargs: dict[str, Any] = dict(self._cap.connect_kwargs())
        if self._name:
            kwargs["name"] = self._name
        if self._tls is not None:
            kwargs["tls"] = self._tls
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
        await self._conn().flush(timeout=timeout)

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
