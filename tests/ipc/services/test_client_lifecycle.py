"""Client lease tolerance (I1), stream adoption (I2), session reset (I3), launch state (I5),
info/public events (I7) and nats error mapping (M6)."""

import asyncio

import pytest
from nats.errors import ConnectionClosedError

from lightfall_utils.ipc.services import echo
from lightfall_utils.ipc.services import protocol as P
from lightfall_utils.ipc.services.client import ServiceClient, StreamClosedError
from lightfall_utils.ipc.services.core import NatsCore
from lightfall_utils.ipc.services.descriptor import ServiceDescriptor
from lightfall_utils.ipc.services.host import ServiceError

SCOPE = "lfclife"


class FakeClock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


class FlakyCore(NatsCore):
    """Records private requests; can time out pings or raise on any call."""

    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.private: list[str] = []
        self.fail_pings = 0
        self.raise_exc: Exception | None = None

    async def request_private(self, subject, payload, timeout=P.DEFAULT_TIMEOUT_S):
        self.private.append(subject)
        if self.raise_exc is not None:
            raise self.raise_exc
        if subject.endswith(".rpc.session.ping") and self.fail_pings > 0:
            self.fail_pings -= 1
            raise TimeoutError
        return await super().request_private(subject, payload, timeout)


def _desc(name="echo"):
    return ServiceDescriptor(name=name, scope=SCOPE, contract="1.0", launch="external")


def _host(**kw):
    kw.setdefault("heartbeat_s", 0.1)
    kw.setdefault("single_instance", False)
    return echo.make_host(SCOPE, **kw)


async def _core(url, cls=NatsCore):
    c = cls(url)
    await c.connect()
    return c


def _client(core, **kw):
    kw.setdefault("app_name", "tester")
    kw.setdefault("app_version", "1.0")
    return ServiceClient(_desc(), core, scope=SCOPE, **kw)


async def _until(pred, timeout=3.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not pred():
        if loop.time() > end:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


def _auths(core):
    return sum(s.endswith(".auth.request") for s in core.private)


async def _ping_round(client, clock):
    clock.t += P.PING_S
    await asyncio.sleep(0.15)  # a real heartbeat refreshes _last_beat at the new fake time
    await client.tick()


def test_single_ping_timeout_is_tolerated_then_reset_fires(broker_url):
    async def go():
        clock = FakeClock()
        host = _host()
        svc = await _core(broker_url)
        await host.start(svc)
        cli = await _core(broker_url, FlakyCore)
        client = _client(cli, clock=clock)
        resets: list[list[str]] = []
        restarts: list[int] = []
        states: list[str] = []
        client.on_session_reset(resets.append)
        client.on_restarted(lambda: restarts.append(1))
        client.on_state(states.append)
        await client.start()
        sid = await client.open_stream("bytes")
        subj0 = client.session_subject

        cli.fail_pings = 1
        await _ping_round(client, clock)
        after_one = (client.session_subject == subj0, _auths(cli), list(resets))
        await _ping_round(client, clock)  # an OK ping resets the failure count

        cli.fail_pings = 3
        for _ in range(3):
            await _ping_round(client, clock)
        await _until(lambda: resets, 5)
        subj1 = client.session_subject
        with pytest.raises(StreamClosedError):
            await client.send_chunk(sid, 0, b"x")
        reply = await client.call("echo", {"after": "reset"})
        result = (after_one, subj0, subj1, resets, restarts, states, reply, _auths(cli))
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return sid, result

    sid, (after_one, subj0, subj1, resets, restarts, states, reply, auths) = asyncio.run(go())
    assert after_one == (True, 1, [])  # one slow ping: same session, no handshake, no reset
    assert subj1 is not None and subj1 != subj0
    assert resets == [[sid]]
    assert restarts == []
    assert "starting" not in states[1:]  # no flicker during the re-handshake
    assert reply["echo"] == {"after": "reset"}
    assert auths == 2


def test_pause_longer_than_lease_rehandshakes_instead_of_up(broker_url):
    async def go():
        clock = FakeClock()
        host = _host()
        svc = await _core(broker_url)
        await host.start(svc)
        cli = await _core(broker_url, FlakyCore)
        client = _client(cli, clock=clock)
        resets: list[list[str]] = []
        client.on_session_reset(resets.append)
        await client.start()
        subj0 = client.session_subject
        clock.t += P.LEASE_S + 5  # e.g. the laptop slept; the service expired the lease
        await _until(lambda: resets, 5)
        await _until(lambda: client.health == "up", 5)
        result = (subj0, client.session_subject, resets, _auths(cli))
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return result

    subj0, subj1, resets, auths = asyncio.run(go())
    assert subj1 is not None and subj1 != subj0
    assert resets == [[]]
    assert auths == 2


def test_expect_launch_reports_starting_until_instance_seen(broker_url):
    async def go():
        cli = await _core(broker_url)
        client = _client(cli)
        states: list[str] = []
        client.on_state(states.append)
        client.expect_launch()
        await client.start()
        before = (client.health, client.last_error)
        host = _host()
        svc = await _core(broker_url)
        await host.start(svc)
        await _until(lambda: client.health == "up")
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return before, states

    before, states = asyncio.run(go())
    assert before == ("starting", "meta.info: NoRespondersError")
    assert states == ["starting", "up"]  # never "down"


def test_new_instance_beat_not_gated_by_retry_backoff(broker_url):
    async def go():
        clock = FakeClock()  # frozen: a backoff gate would never open
        cli = await _core(broker_url)
        client = _client(cli, clock=clock)
        await client.start()
        gated = client._retry_at > clock.t
        host = _host()
        svc = await _core(broker_url)
        await host.start(svc)
        await _until(lambda: client.health == "up", 3)
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return gated

    assert asyncio.run(go())


def test_on_info_fires_on_state_or_summary_change(broker_url):
    async def go():
        host = _host()
        svc = await _core(broker_url)
        await host.start(svc)
        cli = await _core(broker_url)
        client = _client(cli)
        infos: list[tuple] = []
        client.on_info(lambda i: infos.append((i.get("state"), i.get("summary"))))
        await client.start()
        await asyncio.sleep(0.35)  # identical beats: no extra callbacks
        n_idle = len(infos)
        host.set_state("starting", {"model": "loading"})
        await _until(lambda: infos[-1] == ("starting", {"model": "loading"}))
        host.set_state("ready", {"model": "ready", "device": "cpu"})
        await _until(lambda: infos[-1][0] == "ready")
        info = dict(client.info)
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return n_idle, infos, info

    n_idle, infos, info = asyncio.run(go())
    assert n_idle == 1  # the meta.info fetch
    assert infos[-1] == ("ready", {"model": "ready", "device": "cpu"})
    assert len(infos) == 3
    assert info["summary"] == {"model": "ready", "device": "cpu"}


def test_public_event_reaches_client(broker_url):
    async def go():
        host = _host()
        svc = await _core(broker_url)
        await host.start(svc)
        cli = await _core(broker_url)
        client = _client(cli)
        got: list[dict] = []
        other: list[dict] = []
        client.on_public_event("state", got.append)
        client.on_public_event("model.loaded", other.append)
        await client.start()
        await host.publish_event("state", {"state": "ready"})
        await host.publish_event("model.loaded", {"model": "small"})
        await host.publish_event("unwatched", {})
        await _until(lambda: got and other)
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return host, got, other

    host, got, other = asyncio.run(go())
    assert got == [{"state": "ready", "instance_id": host.instance_id, "seq": 1}]
    assert other[0]["model"] == "small"


def test_adopt_stream_for_rpc_opened_stream(broker_url):
    async def go():
        host = _host()
        chunks: list[tuple] = []
        ended: list[str] = []
        spy_headers: list[dict] = []

        @host.stream("audio")
        async def on_audio(session, sid, seq, data, final):
            chunks.append((seq, data, final))

        host.on_stream_end(lambda s, sid, reason: ended.append(reason))

        @host.rpc("utterance.start")
        async def start(session, payload):
            if host.streams:
                raise ServiceError("busy", "one utterance at a time")
            return {"stream_id": host.open_stream(session, "audio", payload)}

        svc = await _core(broker_url)
        await host.start(svc)
        cli = await _core(broker_url)
        client = _client(cli)
        await client.start()

        async def spy(msg):
            spy_headers.append(dict(msg.headers or {}))

        await svc.subscribe(f"{client.session_subject}.stream.>", spy)
        await svc.flush()
        rep = await client.call("utterance.start", {"rate": 16000})
        busy = await client.call("utterance.start", {})
        sid = rep["stream_id"]
        with pytest.raises(StreamClosedError):
            await client.send_chunk(sid, 0, b"a")  # not adopted yet
        client.adopt_stream(sid)
        await client.send_chunk(sid, 0, b"a")
        await client.send_chunk(sid, 1, b"b", final=True)
        await _until(lambda: ended)
        with pytest.raises(StreamClosedError):
            await client.send_chunk(sid, 2, b"c")  # finished
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return busy, chunks, ended, spy_headers

    busy, chunks, ended, spy_headers = asyncio.run(go())
    assert busy["code"] == "busy"
    assert chunks == [(0, b"a", False), (1, b"b", True)]
    assert ended == ["final"]
    assert all(P.HDR_KIND not in h for h in spy_headers)


def test_call_maps_nats_errors_to_error_dict(broker_url):
    async def go():
        host = _host()
        svc = await _core(broker_url)
        await host.start(svc)
        cli = await _core(broker_url, FlakyCore)
        client = _client(cli)
        await client.start()
        cli.raise_exc = ConnectionClosedError()
        closed = await client.call("echo", {})
        cli.raise_exc = None
        await client.close()
        await host.stop()
        await svc.close()
        await cli.close()
        return closed

    closed = asyncio.run(go())
    assert closed["status"] == "error" and closed["code"] == "unknown"
    assert "ConnectionClosedError" in closed["message"]


def test_rehandshake_that_finds_new_instance_fires_restarted(broker_url):
    async def go():
        clock = FakeClock()
        host1 = _host(heartbeat_s=1000.0)  # one beat at start, then silent
        svc1 = await _core(broker_url)
        await host1.start(svc1)
        cli = await _core(broker_url, FlakyCore)
        client = _client(cli, clock=clock)
        resets, restarts = [], []
        client.on_session_reset(resets.append)
        client.on_restarted(lambda: restarts.append(1))
        await client.start()
        # The process vanishes (no final beat) and is replaced; the new one's only beat is
        # missed, and pings time out.
        for t in host1._tasks:
            t.cancel()
        for sub in host1._subs:
            await sub.unsubscribe()
        host1._subs.clear()
        await svc1.close()
        host2 = _host(heartbeat_s=1000.0)
        client._retired.add(host2.instance_id)  # the client misses host2's only beat
        svc2 = await _core(broker_url)
        await host2.start(svc2)
        await asyncio.sleep(0.2)
        client._retired.discard(host2.instance_id)
        cli.fail_pings = 3
        for _ in range(3):
            clock.t += P.PING_S
            client._last_beat = clock.t  # keep health "up" so pings are sent
            await client.tick()
        await _until(lambda: restarts or resets, 5)
        result = (resets, restarts, client.info["instance_id"] == host2.instance_id)
        await client.close()
        await host2.stop()
        await svc2.close()
        await cli.close()
        return result

    resets, restarts, new_iid = asyncio.run(go())
    assert restarts == [1] and resets == []
    assert new_iid
