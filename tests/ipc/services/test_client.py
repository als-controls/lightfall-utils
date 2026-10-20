import asyncio
import json

from lightfall_utils.ipc.services import echo
from lightfall_utils.ipc.services import protocol as P
from lightfall_utils.ipc.services.client import ServiceClient
from lightfall_utils.ipc.services.core import NatsCore
from lightfall_utils.ipc.services.descriptor import ServiceDescriptor
from lightfall_utils.ipc.services.host import ServiceHost, Session, allow_apps

SCOPE = "lfclient"


class FakeClock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


class RecordingCore(NatsCore):
    """A core that records every request and refuses public requests to _PRIV subjects."""

    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.public: list[str] = []
        self.private: list[str] = []

    async def request_public(self, subject, payload, timeout=P.DEFAULT_TIMEOUT_S):
        assert not subject.startswith("_PRIV"), subject
        self.public.append(subject)
        return await super().request_public(subject, payload, timeout)

    async def request_private(self, subject, payload, timeout=P.DEFAULT_TIMEOUT_S):
        self.private.append(subject)
        return await super().request_private(subject, payload, timeout)


def _desc(contract="1.0", name="echo"):
    return ServiceDescriptor(name=name, scope=SCOPE, contract=contract, launch="external")


def _echo_host(scope=SCOPE, **kw):
    kw.setdefault("heartbeat_s", 0.1)
    kw.setdefault("single_instance", False)
    return echo.make_host(scope, **kw)


async def _core(url, cls=NatsCore):
    c = cls(url)
    await c.connect()
    return c


def _client(core, desc=None, **kw):
    kw.setdefault("app_name", "tester")
    kw.setdefault("app_version", "1.0")
    return ServiceClient(desc or _desc(), core, scope=SCOPE, **kw)


async def _until(pred, timeout=3.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not pred():
        if loop.time() > end:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


def test_call_roundtrip_via_private_request_only(broker_url):
    async def go():
        host = _echo_host()
        svc = await _core(broker_url)
        await host.start(svc)
        cli = await _core(broker_url, RecordingCore)
        client = _client(cli)
        await client.start()
        health = client.health
        reply = await client.call("echo", {"x": 1})
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return health, reply, cli

    health, reply, cli = asyncio.run(go())
    assert health == "up"
    assert reply["status"] == "ok"
    assert reply["echo"] == {"x": 1}
    assert reply["contract_version"] == echo.CONTRACT
    assert all(not s.startswith("_PRIV") for s in cli.public)
    assert any(s.endswith(".rpc.echo") for s in cli.private)
    assert any(s.endswith(".auth.request") for s in cli.private)
    assert any(s.endswith(".rpc.session.close") for s in cli.private)


def _incompatible(broker_url, host_contract, min_contract):
    async def go():
        host = _echo_host(contract=host_contract)
        svc = await _core(broker_url)
        await host.start(svc)
        cli = await _core(broker_url, RecordingCore)
        client = _client(cli, _desc(contract=min_contract))
        await client.start()
        reply = await client.call("echo", {})
        result = (client.health, client.last_error, reply, list(cli.private), host.sessions)
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return result

    return asyncio.run(go())


def test_incompatible_contract_major(broker_url):
    health, err, reply, private, sessions = _incompatible(broker_url, "2.0", "1.0")
    assert health == "incompatible"
    assert "2.0" in err
    assert reply["status"] == "error"
    assert private == []
    assert sessions == {}


def test_incompatible_minor_below_min(broker_url):
    health, _err, reply, private, sessions = _incompatible(broker_url, "1.1", "1.2")
    assert health == "incompatible"
    assert reply["status"] == "error"
    assert private == []
    assert sessions == {}


def test_bad_priv_root_unauthorized(broker_url):
    async def go():
        fake = await _core(broker_url)

        async def on_info(msg):
            info = {
                "instance_id": "abc",
                "name": "echo",
                "state": "ready",
                "version": "0",
                "svc_protocol": P.SVC_PROTOCOL,
                "contract": "1.0",
                "priv_root": "_PRIV.short",
                "single_instance": False,
                "summary": {},
                "ts": 0,
            }
            await fake.publish(msg.reply, json.dumps(info).encode())

        await fake.subscribe(P.meta_info(P.root(SCOPE, "echo")), on_info)
        await fake.flush()
        cli = await _core(broker_url, RecordingCore)
        client = _client(cli)
        await client.start()
        result = (client.health, client.last_error, list(cli.private))
        await client.close()
        await fake.close()
        await cli.close()
        return result

    health, err, private = asyncio.run(go())
    assert health == "unauthorized"
    assert err == "bad priv_root"
    assert private == []


def test_denied_handshake_unauthorized(broker_url):
    async def go():
        host = _echo_host(trust=allow_apps(["someone-else"]))
        svc = await _core(broker_url)
        await host.start(svc)
        cli = await _core(broker_url)
        client = _client(cli)
        await client.start()
        result = (client.health, client.last_error)
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return result

    health, err = asyncio.run(go())
    assert health == "unauthorized"
    assert "not trusted" in err


def test_health_recovers_after_gap(broker_url):
    async def go():
        clock = FakeClock()
        # A host that beats once at start, then never again: the test drives beats by hand.
        host = _echo_host(heartbeat_s=1000.0)
        svc = await _core(broker_url)
        await host.start(svc)
        cli = await _core(broker_url, RecordingCore)
        client = _client(cli, clock=clock)
        states: list[str] = []
        restarts: list[int] = []
        client.on_state(states.append)
        client.on_restarted(lambda: restarts.append(1))
        await client.start()
        auths = sum(s.endswith(".auth.request") for s in cli.private)
        clock.t += 1.6 * P.HEARTBEAT_S
        await client.tick()
        degraded = client.health
        clock.t += 2.0 * P.HEARTBEAT_S
        await client.tick()
        down = client.health
        await svc.publish_json(P.heartbeat(host.root), host.info())
        await _until(lambda: client.health == "up")
        reply = await client.call("echo", {"ok": True})
        auths_after = sum(s.endswith(".auth.request") for s in cli.private)
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return states, degraded, down, auths, auths_after, restarts, reply

    states, degraded, down, auths, auths_after, restarts, reply = asyncio.run(go())
    assert degraded == "degraded"
    assert down == "down"
    assert states == ["starting", "up", "degraded", "down", "up"]
    assert auths == auths_after == 1
    assert restarts == []
    assert reply["status"] == "ok"


def test_restart_rehandshakes(broker_url):
    async def go():
        host1 = _echo_host()
        svc1 = await _core(broker_url)
        await host1.start(svc1)
        cli = await _core(broker_url)
        client = _client(cli)
        restarted = asyncio.Event()
        client.on_restarted(restarted.set)
        await client.start()
        first = (client.info["instance_id"], client.info["priv_root"])
        await host1.stop()
        await svc1.close()
        host2 = _echo_host()
        svc2 = await _core(broker_url)
        await host2.start(svc2)
        await asyncio.wait_for(restarted.wait(), 5.0)
        reply = await client.call("echo", {"n": 2})
        second = (client.info["instance_id"], client.info["priv_root"])
        result = (first, second, reply, client.health, host2.instance_id, svc2.priv_root)
        await client.close()
        await host2.stop()
        await svc2.close()
        await cli.close()
        return result

    first, second, reply, health, iid2, pr2 = asyncio.run(go())
    assert first != second
    assert second == (iid2, pr2)
    assert reply["echo"] == {"n": 2}
    assert health == "up"


def test_delivery_event_received(broker_url):
    async def go():
        host = _echo_host()
        svc = await _core(broker_url)
        await host.start(svc)
        cli = await _core(broker_url)
        client = _client(cli)
        got: asyncio.Future = asyncio.get_running_loop().create_future()
        client.on_event("stream_total", lambda ev: got.done() or got.set_result(ev))
        await client.start()
        sid = await client.open_stream("bytes", {})
        await client.send_chunk(sid, 0, b"abc")
        await client.send_chunk(sid, 1, b"defg", final=True)
        ev = await asyncio.wait_for(got, 3.0)
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return sid, ev, host.instance_id

    sid, ev, iid = asyncio.run(go())
    assert ev["stream_id"] == sid
    assert ev["bytes"] == 7
    assert ev["chunks"] == 2
    assert ev["instance_id"] == iid
    assert isinstance(ev["seq"], int)


def test_close_stream_emits_total(broker_url):
    async def go():
        host = _echo_host()
        svc = await _core(broker_url)
        await host.start(svc)
        cli = await _core(broker_url)
        client = _client(cli)
        got: asyncio.Future = asyncio.get_running_loop().create_future()
        client.on_event("stream_total", lambda ev: got.done() or got.set_result(ev))
        await client.start()
        sid = await client.open_stream("bytes", {})
        await client.send_chunk(sid, 0, b"12345")
        await client.close_stream(sid)
        ev = await asyncio.wait_for(got, 3.0)
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return ev

    ev = asyncio.run(go())
    assert ev["bytes"] == 5


def test_service_stream_chunks_reach_on_stream(broker_url):
    async def go():
        host = ServiceHost(
            "pusher", SCOPE, contract="1.0", version="0", heartbeat_s=0.1, single_instance=False
        )

        @host.rpc("push")
        async def push(session: Session, payload: dict) -> dict:
            await host.emit_chunk(session, "s1", 0, b"aa", kind="k")
            await host.emit_chunk(session, "s1", 1, b"bb", final=True, kind="k")
            await host.emit(session, "done", {})
            return {}

        svc = await _core(broker_url)
        await host.start(svc)
        cli = await _core(broker_url)
        client = _client(cli, _desc(name="pusher"))
        chunks: list = []
        events: list = []
        client.on_stream(lambda sid, seq, data, final: chunks.append((sid, seq, data, final)))
        client.on_event("done", events.append)
        await client.start()
        await client.call("push", {})
        await _until(lambda: events)
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return chunks, events

    chunks, events = asyncio.run(go())
    assert chunks == [("s1", 0, b"aa", False), ("s1", 1, b"bb", True)]
    assert len(events) == 1


def test_ping_keeps_session_alive(broker_url):
    async def go():
        clock = FakeClock()
        host = _echo_host(clock=clock)
        svc = await _core(broker_url)
        await host.start(svc)
        cli = await _core(broker_url, RecordingCore)
        client = _client(cli, clock=clock)
        await client.start()
        for _ in range(12):
            clock.t += P.PING_S
            await client.tick()
            await asyncio.sleep(0.15)  # let a real heartbeat and the host sweep run
        sessions = host.sessions
        reply = await client.call("echo", {"alive": True})
        pings = sum(s.endswith(".rpc.session.ping") for s in cli.private)
        auths = sum(s.endswith(".auth.request") for s in cli.private)
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return sessions, reply, pings, auths

    sessions, reply, pings, auths = asyncio.run(go())
    assert len(sessions) == 1
    assert reply["status"] == "ok"
    assert pings >= 12
    assert auths == 1


def test_timeout_maps_to_error_dict(broker_url):
    async def go():
        host = _echo_host()
        svc = await _core(broker_url)
        await host.start(svc)
        cli = await _core(broker_url)
        client = _client(cli)
        await client.start()
        reply = await client.call("slow", {"seconds": 1.0}, timeout=0.1)
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return reply

    reply = asyncio.run(go())
    assert reply["status"] == "error"
    assert reply["code"] == "timeout"


def test_no_responders_sets_down(broker_url):
    async def go():
        host = _echo_host()
        svc = await _core(broker_url)
        await host.start(svc)
        cli = await _core(broker_url)
        client = _client(cli)
        await client.start()
        # Simulate the service vanishing without a final heartbeat.
        for t in host._tasks:
            t.cancel()
        for sub in host._subs:
            await sub.unsubscribe()
        host._subs.clear()
        await svc.flush()
        reply = await client.call("echo", {})
        health = client.health
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return reply, health

    reply, health = asyncio.run(go())
    assert reply["status"] == "error"
    assert health == "down"


def test_start_with_no_service_is_down_then_connects(broker_url):
    async def go():
        cli = await _core(broker_url)
        client = _client(cli)
        restarts: list[int] = []
        client.on_restarted(lambda: restarts.append(1))
        await client.start()
        before = client.health
        host = _echo_host()
        svc = await _core(broker_url)
        await host.start(svc)
        await _until(lambda: client.health == "up")
        reply = await client.call("echo", {})
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return before, reply, restarts

    before, reply, restarts = asyncio.run(go())
    assert before == "down"
    assert reply["status"] == "ok"
    assert restarts == []
