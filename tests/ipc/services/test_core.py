import asyncio
import json
import ssl

import nats
import pytest

from lightfall_utils.ipc.services.core import NatsCore


def test_publish_bytes_with_headers_roundtrip(broker_url):
    async def go():
        a, b = NatsCore(broker_url), NatsCore(broker_url)
        await a.connect()
        await b.connect()
        got = asyncio.get_running_loop().create_future()

        async def cb(msg):
            got.set_result((msg.data, dict(msg.headers or {})))

        await b.subscribe("t.hdr", cb)
        await b.flush()
        await a.publish("t.hdr", b"\x00\x01bin", headers={"Svc-Seq": "3", "Svc-Final": "1"})
        out = await asyncio.wait_for(got, 3)
        await a.close()
        await b.close()
        return out

    data, hdrs = asyncio.run(go())
    assert data == b"\x00\x01bin"
    assert hdrs == {"Svc-Seq": "3", "Svc-Final": "1"}


def test_publish_json(broker_url):
    async def go():
        a = NatsCore(broker_url)
        await a.connect()
        got = asyncio.get_running_loop().create_future()

        async def cb(msg):
            got.set_result(json.loads(msg.data))

        await a.subscribe("t.json", cb)
        await a.publish_json("t.json", {"x": 1})
        out = await asyncio.wait_for(got, 3)
        await a.close()
        return out

    assert asyncio.run(go()) == {"x": 1}


def test_request_public_rejects_priv():
    async def go():
        core = NatsCore("nats://127.0.0.1:1")
        await core.request_public("_PRIV.abc.x", {})

    with pytest.raises(ValueError):
        asyncio.run(go())


def test_request_public_roundtrip(broker_url):
    async def go():
        a, b = NatsCore(broker_url), NatsCore(broker_url)
        await a.connect()
        await b.connect()

        async def cb(msg):
            await b.publish(msg.reply, json.dumps({"ok": True}).encode())

        await b.subscribe("t.pub", cb)
        await b.flush()
        out = await a.request_public("t.pub", {"q": 1})
        await a.close()
        await b.close()
        return out

    assert asyncio.run(go()) == {"ok": True}


def test_request_private_roundtrip(broker_url):
    async def go():
        a, b = NatsCore(broker_url), NatsCore(broker_url)
        await a.connect()
        await b.connect()
        subject = f"{b.priv_root}.x"

        async def cb(msg):
            req = json.loads(msg.data)
            await b.publish(msg.reply, json.dumps({"echo": req["v"]}).encode())

        await b.subscribe(subject, cb)
        await b.flush()
        out = await a.request_private(subject, {"v": 7})
        await a.close()
        await b.close()
        return out

    assert asyncio.run(go()) == {"echo": 7}


def test_request_private_uses_unguessable_reply():
    class Reply:
        data = b'{"ok": 1}'

    class FakeSub:
        async def next_msg(self, timeout=None):
            return Reply()

        async def unsubscribe(self):
            pass

    class FakeNC:
        def __init__(self):
            self.published = []
            self.sub_subject = None

        async def request(self, *a, **k):
            raise AssertionError("nc.request must not be used for private subjects")

        async def subscribe(self, subject, **k):
            self.sub_subject = subject
            return FakeSub()

        async def publish(self, subject, data, reply=None, **k):
            self.published.append((subject, reply))

    async def go():
        core = NatsCore("nats://unused")
        fake = FakeNC()
        core._nc = fake
        out = await core.request_private("_PRIV.abc.x", {"a": 1})
        return out, fake, core

    out, fake, core = asyncio.run(go())
    assert out == {"ok": 1}
    assert fake.sub_subject.startswith(core.capability.inbox_prefix + ".")
    assert fake.published[0][1] == fake.sub_subject


def test_connect_passes_capability(monkeypatch):
    seen = {}

    async def fake_connect(url, **kwargs):
        seen["kwargs"] = kwargs
        return object()

    monkeypatch.setattr(nats, "connect", fake_connect)
    core = NatsCore("nats://h:1", name="svc")
    asyncio.run(core.connect())
    kw = seen["kwargs"]
    assert kw["token"] == core.capability.secret
    assert kw["inbox_prefix"] == core.capability.inbox_prefix
    assert "tls" not in kw


def test_connect_passes_tls_when_given(monkeypatch):
    seen = {}

    async def fake_connect(url, **kwargs):
        seen.update(kwargs)
        return object()

    monkeypatch.setattr(nats, "connect", fake_connect)
    ctx = ssl.create_default_context()
    asyncio.run(NatsCore("tls://h:1", tls=ctx).connect())
    assert seen["tls"] is ctx


def test_methods_raise_before_connect():
    core = NatsCore("nats://unused")

    async def go():
        for coro in (
            core.subscribe("x", None),
            core.publish("x", b""),
            core.publish_json("x", {}),
            core.request_public("x", {}),
            core.request_private("_PRIV.abc.x", {}),
            core.flush(),
        ):
            with pytest.raises(RuntimeError, match="not connected"):
                await coro

    asyncio.run(go())


def test_methods_raise_after_close(broker_url):
    async def go():
        core = NatsCore(broker_url)
        await core.connect()
        await core.flush()
        await core.close()
        with pytest.raises(RuntimeError, match="not connected"):
            await core.publish("x", b"")
        with pytest.raises(RuntimeError, match="not connected"):
            await core.flush()

    asyncio.run(go())


def test_flush_pushes_pending_buffer_before_ping():
    """nats-py writes PING ahead of still-buffered SUBs; flush() must push the buffer first.

    Otherwise a peer's request issued after flush() can race the SUBs and see no responders.
    """
    calls = []

    class FakeNC:
        async def _flush_pending(self, force_flush=False):
            calls.append(("pending", force_flush))

        async def flush(self, timeout):
            calls.append(("ping", timeout))

    core = NatsCore("nats://unused")
    core._nc = FakeNC()
    asyncio.run(core.flush(1.5))
    assert calls == [("pending", True), ("ping", 1.5)]


def test_connect_passes_reconnect_limit_and_callbacks(monkeypatch):
    seen = {}
    calls = []

    async def fake_connect(url, **kwargs):
        seen.update(kwargs)
        return object()

    def bad():
        calls.append("closed")
        raise RuntimeError("boom")  # swallowed and logged

    async def reconnected():
        calls.append("reconnected")

    monkeypatch.setattr(nats, "connect", fake_connect)
    core = NatsCore(
        "nats://h:1",
        max_reconnect_attempts=-1,
        on_disconnected=lambda: calls.append("disconnected"),
        on_reconnected=reconnected,
        on_closed=bad,
    )

    async def go():
        await core.connect()
        for key in ("disconnected_cb", "reconnected_cb", "closed_cb"):
            await seen[key]()

    asyncio.run(go())
    assert seen["max_reconnect_attempts"] == -1
    assert calls == ["disconnected", "reconnected", "closed"]


def test_default_core_keeps_nats_reconnect_default(monkeypatch):
    seen = {}

    async def fake_connect(url, **kwargs):
        seen.update(kwargs)
        return object()

    monkeypatch.setattr(nats, "connect", fake_connect)
    asyncio.run(NatsCore("nats://h:1").connect())
    assert seen["max_reconnect_attempts"] == 60
    assert "closed_cb" not in seen


def test_reconnects_after_broker_restart_on_same_port():
    from lightfall_utils.ipc import LocalNatsServer

    from .conftest import _free_port

    port = _free_port()
    server = LocalNatsServer(port=port, host="127.0.0.1")
    server.start(timeout_s=10.0)
    events: list[str] = []

    async def go():
        nonlocal server
        core = NatsCore(
            f"nats://127.0.0.1:{port}",
            max_reconnect_attempts=-1,
            on_disconnected=lambda: events.append("down"),
            on_reconnected=lambda: events.append("up"),
        )
        await core.connect()
        got = asyncio.Queue()

        async def cb(msg):
            await got.put(msg.data)

        await core.subscribe("lfcore.restart", cb)
        await core.flush()
        server.stop()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 10
        while "down" not in events and loop.time() < deadline:
            await asyncio.sleep(0.05)
        server = LocalNatsServer(port=port, host="127.0.0.1")
        await asyncio.to_thread(server.start, 10.0)
        while "up" not in events and loop.time() < deadline + 10:
            await asyncio.sleep(0.05)
        connected = core.is_connected
        await core.publish("lfcore.restart", b"again")  # the subscription was restored
        data = await asyncio.wait_for(got.get(), 5)
        await core.close()
        return connected, data

    try:
        connected, data = asyncio.run(go())
    finally:
        server.stop()
    assert events[:2] == ["down", "up"]
    assert connected and data == b"again"
