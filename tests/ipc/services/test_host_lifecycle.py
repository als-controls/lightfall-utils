"""Stream concurrency (I1), stream lifecycle hooks (I2), public events and state updates (I7)."""

import asyncio
import json
import time

import pytest

from lightfall_utils.ipc.capability import new_token
from lightfall_utils.ipc.services import protocol as P
from lightfall_utils.ipc.services.core import NatsCore
from lightfall_utils.ipc.services.host import ServiceError, ServiceHost, Session

SCOPE = "lflife"
CONTRACT = "1.0"


class FakeClock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def _host(name="echo", **kw):
    kw.setdefault("contract", CONTRACT)
    kw.setdefault("version", "0.1.0")
    kw.setdefault("heartbeat_s", 0.1)
    kw.setdefault("single_instance", False)
    return ServiceHost(name, kw.pop("scope", SCOPE), **kw)


async def _cores(broker_url, n):
    out = []
    for _ in range(n):
        c = NatsCore(broker_url)
        await c.connect()
        out.append(c)
    return out


async def _auth(host, cli, **extra):
    dt = new_token()
    rep = await cli.request_private(
        P.auth_subject(host.priv_root),
        {
            "app_name": "tester",
            "app_version": "1",
            "delivery_token": dt,
            "client_priv_root": cli.priv_root,
            **extra,
        },
    )
    assert rep["status"] == "approved", rep
    return rep["session_subject"], P.delivery_root(cli.priv_root, dt)


async def _chunk(cli, subj, sid, seq, data, final=False, kind="bytes"):
    headers = {P.HDR_SEQ: str(seq), P.HDR_FINAL: "1" if final else "0"}
    if kind is not None:
        headers[P.HDR_KIND] = kind
    await cli.publish(P.stream_subject(subj, sid), data, headers=headers)


async def _open(cli, subj, kind="bytes", params=None):
    return await cli.request_private(
        P.rpc_subject(subj, "stream.open"), {"kind": kind, "params": params or {}}
    )


async def _until(pred, timeout=3.0):
    end = time.monotonic() + timeout
    while not pred():
        if time.monotonic() > end:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


async def _stop(host, *cores):
    await host.stop()
    for c in cores:
        await c.close()


# -- I1: slow handlers never block the shared session subscription ----------------------


@pytest.mark.parametrize("sync_handler", [False, True])
def test_slow_stream_handler_does_not_block_pings_or_rpcs(broker_url, sync_handler):
    async def go():
        host = _host()
        got = []
        release = asyncio.Event()

        if sync_handler:

            @host.stream("bytes")
            def on_chunk(session, sid, seq, data, final):
                if final:
                    time.sleep(2.5)  # e.g. a CPU transcription
                got.append((seq, final))

        else:

            @host.stream("bytes")
            async def on_chunk(session, sid, seq, data, final):
                if final:
                    await asyncio.sleep(2.5)
                got.append((seq, final))

        @host.rpc("hi")
        async def hi(session, payload):
            return {"hi": 1}

        svc, a, b = await _cores(broker_url, 3)
        await host.start(svc)
        subj_a, _ = await _auth(host, a)
        subj_b, _ = await _auth(host, b)
        sid = (await _open(a, subj_a))["stream_id"]
        for i in range(3):
            await _chunk(a, subj_a, sid, i, b"x")
        await _chunk(a, subj_a, sid, 3, b"y", final=True)
        await a.flush()
        await asyncio.sleep(0.2)  # the final handler is now running
        t0 = time.monotonic()
        ping_b = await b.request_private(P.rpc_subject(subj_b, "session.ping"), {}, 1.0)
        ping_a = await a.request_private(P.rpc_subject(subj_a, "session.ping"), {}, 1.0)
        hi_b = await b.request_private(P.rpc_subject(subj_b, "hi"), {}, 1.0)
        elapsed = time.monotonic() - t0
        await _until(lambda: len(got) == 4, 5)
        release.set()
        await _stop(host, svc, a, b)
        return ping_a, ping_b, hi_b, elapsed, got

    ping_a, ping_b, hi_b, elapsed, got = asyncio.run(go())
    assert ping_a["status"] == ping_b["status"] == hi_b["status"] == "ok"
    assert elapsed < 1.0
    assert got == [(0, False), (1, False), (2, False), (3, True)]


def test_chunk_order_and_close_after_chunks_with_slow_handler(broker_url):
    async def go():
        host = _host()
        got = []

        @host.stream("bytes")
        async def on_chunk(session, sid, seq, data, final):
            await asyncio.sleep(0.02)
            got.append((seq, data, final))

        svc, cli = await _cores(broker_url, 2)
        await host.start(svc)
        subj, _ = await _auth(host, cli)
        sid = (await _open(cli, subj))["stream_id"]
        for i in range(10):
            await _chunk(cli, subj, sid, i, bytes([i]))
        t0 = time.monotonic()
        closed = await cli.request_private(P.rpc_subject(subj, "stream.close"), {"stream_id": sid})
        close_s = time.monotonic() - t0
        await _until(lambda: got and got[-1][2], 3)
        await _stop(host, svc, cli)
        return got, closed, close_s

    got, closed, close_s = asyncio.run(go())
    assert closed["status"] == "ok"
    assert close_s < 0.5  # the reply does not wait for the queued handler calls
    assert [g[0] for g in got] == list(range(11))
    assert got[-1] == (10, b"", True)
    assert all(not g[2] for g in got[:-1])


# -- I2: stream lifecycle API -----------------------------------------------------------


def _voice_like_host(**kw):
    """A host shaped like the stt service: utterance.start/stop/cancel, one active stream."""
    host = _host(**kw)
    got: list = []
    ended: list = []
    active: dict = {}

    @host.stream("audio")
    async def on_audio(session, sid, seq, data, final):
        got.append((sid, seq, data, final))

    @host.on_stream_end
    async def on_end(session, sid, reason):
        ended.append((sid, reason, session.app_name))
        active.pop("sid", None)

    @host.rpc("utterance.start")
    async def start(session, payload):
        if "sid" in active:
            raise ServiceError("busy", "an utterance is already active")
        active["sid"] = host.open_stream(session, "audio", payload)
        return {"stream_id": active["sid"]}

    @host.rpc("utterance.stop")
    async def stop(session, payload):
        return {"ended": await host.end_stream(payload["stream_id"], final=True)}

    @host.rpc("utterance.cancel")
    async def cancel(session, payload):
        return {"ended": await host.end_stream(payload["stream_id"])}

    return host, got, ended


def test_rpc_opened_stream_accepts_chunks_without_kind_and_busy(broker_url):
    async def go():
        host, got, ended = _voice_like_host()
        svc, cli = await _cores(broker_url, 2)
        await host.start(svc)
        subj, _ = await _auth(host, cli)
        start = await cli.request_private(P.rpc_subject(subj, "utterance.start"), {"rate": 16000})
        sid = start["stream_id"]
        params = host.streams[sid].params
        busy = await cli.request_private(P.rpc_subject(subj, "utterance.start"), {})
        await _chunk(cli, subj, sid, 0, b"a", kind=None)  # header absent
        await _chunk(cli, subj, sid, 1, b"b", kind="")  # header empty
        await _chunk(cli, subj, sid, 2, b"c", kind="audio")
        await _chunk(cli, subj, sid, 3, b"d", kind="other")  # wrong kind: dropped
        stop = await cli.request_private(P.rpc_subject(subj, "utterance.stop"), {"stream_id": sid})
        await _until(lambda: ended, 3)
        again = await cli.request_private(P.rpc_subject(subj, "utterance.start"), {})
        await _stop(host, svc, cli)
        return sid, params, busy, stop, got, ended, again

    sid, params, busy, stop, got, ended, again = asyncio.run(go())
    assert params == {"rate": 16000}
    assert busy["status"] == "error" and busy["code"] == "busy"
    assert stop == {"status": "ok", "ended": True, "contract_version": CONTRACT}
    assert [(g[1], g[2], g[3]) for g in got] == [
        (0, b"a", False),
        (1, b"b", False),
        (2, b"c", False),
        (3, b"", True),
    ]
    assert ended[0] == (sid, "close", "tester")
    assert again["status"] == "ok"  # the slot was freed by on_stream_end
    assert (again["stream_id"], "stop", "tester") in ended  # host stop ends it


def test_end_stream_cancel_drops_without_final(broker_url):
    async def go():
        host, got, ended = _voice_like_host()
        svc, cli = await _cores(broker_url, 2)
        await host.start(svc)
        subj, _ = await _auth(host, cli)
        sid = (await cli.request_private(P.rpc_subject(subj, "utterance.start"), {}))["stream_id"]
        await _chunk(cli, subj, sid, 0, b"a", kind=None)
        await cli.request_private(P.rpc_subject(subj, "session.ping"), {})  # orders the chunk
        await _until(lambda: got, 3)
        cancel = await cli.request_private(
            P.rpc_subject(subj, "utterance.cancel"), {"stream_id": sid}
        )
        await _chunk(cli, subj, sid, 1, b"b", kind=None)
        twice = await cli.request_private(
            P.rpc_subject(subj, "utterance.cancel"), {"stream_id": sid}
        )
        await asyncio.sleep(0.2)
        await _stop(host, svc, cli)
        return sid, got, ended, cancel, twice

    sid, got, ended, cancel, twice = asyncio.run(go())
    assert cancel["ended"] is True and twice["ended"] is False
    assert [(g[1], g[3]) for g in got] == [(0, False)]  # no final call, chunk 1 refused
    assert ended == [(sid, "cancel", "tester")]


def test_on_stream_end_reasons_final_idle_session_end(broker_url):
    clock = FakeClock()

    async def go():
        host = _host(clock=clock)
        ended = []

        @host.stream("bytes")
        async def on_chunk(session, sid, seq, data, final):
            pass

        host.on_stream_end(lambda session, sid, reason: ended.append((sid, reason)))
        svc, cli = await _cores(broker_url, 2)
        await host.start(svc)
        subj, _ = await _auth(host, cli)
        s_final = (await _open(cli, subj))["stream_id"]
        s_idle = (await _open(cli, subj))["stream_id"]
        await _chunk(cli, subj, s_final, 0, b"z", final=True)
        await _until(lambda: (s_final, "final") in ended, 3)
        clock.t += P.STREAM_IDLE_S + 1
        await _until(lambda: (s_idle, "idle") in ended, 3)
        s_sess = (await _open(cli, subj))["stream_id"]
        await cli.request_private(P.rpc_subject(subj, "session.close"), {})
        await _until(lambda: (s_sess, "session_end") in ended, 3)
        await _stop(host, svc, cli)
        return ended

    ended = asyncio.run(go())
    assert len(ended) == 3  # each stream ends exactly once


def test_on_open_validator_refuses_stream_open(broker_url):
    async def go():
        host = _host()
        seen = []

        def only_one(session, params):
            seen.append(params)
            if host.streams:
                raise ServiceError("busy", "one stream at a time")

        @host.stream("bytes", on_open=only_one)
        async def on_chunk(session, sid, seq, data, final):
            pass

        svc, cli = await _cores(broker_url, 2)
        await host.start(svc)
        subj, _ = await _auth(host, cli)
        first = await _open(cli, subj, params={"n": 1})
        second = await _open(cli, subj, params={"n": 2})
        await _stop(host, svc, cli)
        return first, second, seen

    first, second, seen = asyncio.run(go())
    assert first["status"] == "ok"
    assert second["status"] == "error" and second["code"] == "busy"
    assert second["message"] == "one stream at a time"
    assert seen == [{"n": 1}, {"n": 2}]


def test_end_stream_from_inside_its_handler(broker_url):
    async def go():
        host = _host()
        got, ended = [], []

        @host.stream("bytes")
        async def on_chunk(session, sid, seq, data, final):
            got.append((seq, final))
            if seq == 1:  # e.g. the 60 s cap: end it from the handler
                await host.end_stream(sid, reason="max")

        host.on_stream_end(lambda s, sid, reason: ended.append(reason))
        svc, cli = await _cores(broker_url, 2)
        await host.start(svc)
        subj, _ = await _auth(host, cli)
        sid = (await _open(cli, subj))["stream_id"]
        for i in range(4):
            await _chunk(cli, subj, sid, i, b"x")
        await _until(lambda: ended, 3)
        await asyncio.sleep(0.1)
        await _stop(host, svc, cli)
        return got, ended

    got, ended = asyncio.run(go())
    assert got[:2] == [(0, False), (1, False)]
    assert ended == ["max"]


def test_service_error_from_rpc_handler(broker_url):
    async def go():
        host = _host()

        @host.rpc("work")
        def work(session, payload):
            raise ServiceError("busy", "try later")

        svc, cli = await _cores(broker_url, 2)
        await host.start(svc)
        subj, _ = await _auth(host, cli)
        rep = await cli.request_private(P.rpc_subject(subj, "work"), {})
        await _stop(host, svc, cli)
        return rep

    rep = asyncio.run(go())
    assert rep == {
        "status": "error",
        "code": "busy",
        "message": "try later",
        "contract_version": CONTRACT,
    }


def test_service_error_rejects_unknown_code():
    with pytest.raises(ValueError):
        ServiceError("nope")


def test_open_stream_validation():
    host = _host()

    @host.stream("bytes")
    async def on_chunk(*a):
        pass

    s = Session("t", "app", None, "_PRIV.x.d.y", 0.0)
    with pytest.raises(ValueError):
        host.open_stream(s, "zz")
    with pytest.raises(ValueError):
        host.open_stream(s, "bytes")  # not a live session


# -- events -----------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["stream", "stream.x", "", "a..b", "a.*", "a b", "x.>"])
def test_emit_rejects_bad_names(name):
    host = _host()
    s = Session("t", "app", None, "_PRIV.x.d.y", 0.0)

    async def go():
        with pytest.raises(ValueError):
            await host.emit(s, name, {})

    asyncio.run(go())


def test_emit_seq_is_per_session(broker_url):
    async def go():
        host = _host()
        sessions = {}

        @host.rpc("who")
        async def who(session, payload):
            sessions[payload["n"]] = session
            return {}

        svc, a, b = await _cores(broker_url, 3)
        await host.start(svc)
        subj_a, dr_a = await _auth(host, a)
        subj_b, dr_b = await _auth(host, b)
        await a.request_private(P.rpc_subject(subj_a, "who"), {"n": "a"})
        await b.request_private(P.rpc_subject(subj_b, "who"), {"n": "b"})
        got_a, got_b = [], []

        async def rec_a(m):
            got_a.append(json.loads(m.data)["seq"])

        async def rec_b(m):
            got_b.append(json.loads(m.data)["seq"])

        await a.subscribe(f"{dr_a}.>", rec_a)
        await b.subscribe(f"{dr_b}.>", rec_b)
        await a.flush()
        await b.flush()
        await host.emit(sessions["a"], "utterance.listening", {})
        await host.emit(sessions["b"], "utterance.listening", {})
        await host.emit(sessions["a"], "utterance.transcript", {"text": "hi"})
        await _until(lambda: len(got_a) == 2 and len(got_b) == 1, 3)
        await _stop(host, svc, a, b)
        return got_a, got_b

    got_a, got_b = asyncio.run(go())
    assert got_a == [1, 2]  # no false gap from b's event
    assert got_b == [1]


def test_publish_event_is_public_under_root(broker_url):
    async def go():
        host = _host()
        svc, spy = await _cores(broker_url, 2)
        await host.start(svc)
        got = []

        async def rec(m):
            got.append((m.subject, json.loads(m.data)))

        await spy.subscribe(f"{host.root}.events.>", rec)
        await spy.flush()
        await host.publish_event("state", {"state": "ready"})
        await host.publish_event("model.loaded", {"model": "small"})
        await _until(lambda: len(got) == 2, 3)
        await _stop(host, svc, spy)
        return host, got

    host, got = asyncio.run(go())
    assert got[0][0] == f"{host.root}.events.state"
    assert got[0][1] == {"state": "ready", "instance_id": host.instance_id, "seq": 1}
    assert got[1][0] == f"{host.root}.events.model.loaded" and got[1][1]["seq"] == 2


def test_set_state_publishes_heartbeat_immediately(broker_url):
    async def go():
        host = _host(heartbeat_s=5.0)
        svc, cli = await _cores(broker_url, 2)
        beats = []

        async def rec(m):
            beats.append(json.loads(m.data))

        await cli.subscribe(P.heartbeat(host.root), rec)
        await cli.flush()
        await host.start(svc)
        await _until(lambda: beats, 3)
        t0 = time.monotonic()
        host.set_state("ready", {"model": "loading"})
        await _until(lambda: len(beats) >= 2, 2)
        dt = time.monotonic() - t0
        await _stop(host, svc, cli)
        return beats, dt

    beats, dt = asyncio.run(go())
    assert dt < 1.0
    assert beats[1]["state"] == "ready" and beats[1]["summary"] == {"model": "loading"}


# -- I5 / M7: no single-instance probe for local (stdin-token) hosts ----------------------


def test_single_instance_probe_skipped_with_stdin_token(broker_url):
    scope = f"lfsi{new_token()[:8]}"

    async def go():
        first = _host(scope=scope, single_instance=True)
        svc1, svc2 = await _cores(broker_url, 2)
        await first.start(svc1)
        await asyncio.sleep(0.25)  # first is beating
        local = _host(scope=scope, single_instance=True, stdin_token=new_token())
        t0 = time.monotonic()
        await local.start(svc2)  # no InstanceConflictError, no probe delay
        dt = time.monotonic() - t0
        await local.stop()
        await _stop(first, svc1, svc2)
        return dt

    assert asyncio.run(go()) < 0.1
