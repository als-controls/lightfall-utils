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
        await b._nc.flush()
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
        await b._nc.flush()
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
        await b._nc.flush()
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
