"""Permission-model tests against a running bcgnats-auth-callout replica.

Skipped unless ``BCGNATS_REPLICA_URL`` is set (e.g. ``nats://127.0.0.1:4299`` for
``go run ./cmd/replica --port 4299`` in ``bcgnats-auth-callout``).
"""

import asyncio
import os
import time

import nats
import pytest

from lightfall_utils.ipc.services import echo
from lightfall_utils.ipc.services.client import ServiceClient
from lightfall_utils.ipc.services.core import NatsCore
from lightfall_utils.ipc.services.descriptor import ServiceDescriptor

URL = os.environ.get("BCGNATS_REPLICA_URL")
pytestmark = pytest.mark.skipif(not URL, reason="BCGNATS_REPLICA_URL not set")

SCOPE = "als.svctest"


class Spy(NatsCore):
    """A connection with its own capability that records async server errors."""

    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.errors: list[str] = []

    async def connect(self) -> None:
        async def on_error(e):
            self.errors.append(str(e))

        self._nc = await nats.connect(
            self._url, error_cb=on_error, allow_reconnect=False, **self._cap.connect_kwargs()
        )

    async def wait_violation(self, timeout: float = 2.0) -> bool:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if any("permissions violation" in e.lower() for e in self.errors):
                return True
            await asyncio.sleep(0.05)
        return False


async def _noop(msg):
    pass


async def _until(pred, timeout=3.0):
    end = time.monotonic() + timeout
    while not pred():
        if time.monotonic() > end:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)


async def _setup():
    host = echo.make_host(SCOPE, heartbeat_s=0.2, single_instance=False)
    svc = NatsCore(URL)
    await svc.connect()
    await host.start(svc)
    cli_core = NatsCore(URL)
    await cli_core.connect()
    desc = ServiceDescriptor(name="echo", scope=SCOPE, contract="1.0", launch="external")
    client = ServiceClient(desc, cli_core, scope=SCOPE, app_name="tester", app_version="1.0")
    await client.start()
    return host, svc, cli_core, client


async def _teardown(host, svc, cli_core, client, *extra):
    await client.close()
    await host.stop()
    for c in (svc, cli_core, *extra):
        await c.close()


def test_call_works_through_callout():
    async def go():
        host, svc, cli_core, client = await _setup()
        try:
            assert client.health == "up"
            reply = await client.call("echo", {"x": 1})
            assert reply["status"] == "ok"
            assert reply["echo"] == {"x": 1}
        finally:
            await _teardown(host, svc, cli_core, client)

    asyncio.run(go())


def test_spy_cannot_read_handshake_or_session():
    async def go():
        host, svc, cli_core, client = await _setup()
        spy = Spy(URL)
        await spy.connect()
        try:
            await spy.subscribe(f"{svc.priv_root}.>", _noop)
            await spy.flush()
            assert await spy.wait_violation()
        finally:
            await _teardown(host, svc, cli_core, client, spy)

    asyncio.run(go())


def test_forged_delivery_without_token_not_received():
    async def go():
        host, svc, cli_core, client = await _setup()
        spy = Spy(URL)
        await spy.connect()
        seen: list[dict] = []
        client.on_event("forged", seen.append)
        try:
            # Forgery: guessed delivery token (may be refused by the broker, or ignored).
            await spy.publish(f"{cli_core.priv_root}.d.guess.forged", b'{"bad": true}')
            await spy.flush()
            await asyncio.sleep(0.5)
            assert seen == []
            # Positive control: the service emits with the real delivery token.
            session = next(iter(host.sessions.values()))
            await host.emit(session, "forged", {"ok": True})
            await svc.flush()
            await _until(lambda: seen)
            assert seen[0]["ok"] is True
        finally:
            await _teardown(host, svc, cli_core, client, spy)

    asyncio.run(go())
