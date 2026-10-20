import asyncio
import functools
import json

import pytest

from lightfall_utils.ipc.capability import new_token
from lightfall_utils.ipc.services import protocol as P
from lightfall_utils.ipc.services.core import NatsCore
from lightfall_utils.ipc.services.host import InstanceConflictError, ServiceHost

SCOPE = "lfstream"
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


async def _start(broker_url, host):
    svc, cli = NatsCore(broker_url), NatsCore(broker_url)
    await svc.connect()
    await cli.connect()
    await host.start(svc)
    return svc, cli


async def _auth(host, cli, delivery_token=None):
    dt = delivery_token or new_token()
    rep = await cli.request_private(
        P.auth_subject(host.priv_root),
        {
            "app_name": "tester",
            "app_version": "1",
            "delivery_token": dt,
            "client_priv_root": cli.priv_root,
        },
    )
    assert rep["status"] == "approved", rep
    return rep["session_subject"], P.delivery_root(cli.priv_root, dt)


async def _chunk(cli, subj, sid, seq, data, final=False, kind="bytes"):
    await cli.publish(
        P.stream_subject(subj, sid),
        data,
        headers={P.HDR_SEQ: str(seq), P.HDR_KIND: kind, P.HDR_FINAL: "1" if final else "0"},
    )


def _recorder(host, kind="bytes"):
    got = []
    done = asyncio.Event()

    @host.stream(kind)
    async def on_chunk(session, stream_id, seq, data, final):
        got.append((stream_id, seq, data, final))
        if final:
            done.set()

    return got, done


async def _stop(host, *cores):
    await host.stop()
    for c in cores:
        await c.close()


def test_stream_chunks_in_order_and_final(broker_url):
    async def go():
        host = _host()
        got, done = _recorder(host)
        svc, cli = await _start(broker_url, host)
        subj, _ = await _auth(host, cli)
        opened = await cli.request_private(
            P.rpc_subject(subj, "stream.open"), {"kind": "bytes", "params": {"rate": 16000}}
        )
        sid = opened["stream_id"]
        assert host.streams[sid].params == {"rate": 16000}
        for i in range(20):
            await _chunk(cli, subj, sid, i, bytes([i]) * 3, final=(i == 19))
        await asyncio.wait_for(done.wait(), 3)
        gone = sid not in host.streams
        bad_open = await cli.request_private(P.rpc_subject(subj, "stream.open"), {"kind": "zz"})
        await _stop(host, svc, cli)
        return opened, sid, got, gone, bad_open, host

    opened, sid, got, gone, bad_open, host = asyncio.run(go())
    assert opened["status"] == "ok" and opened["contract_version"] == CONTRACT
    assert [g[1] for g in got] == list(range(20))
    assert all(g[0] == sid for g in got)
    assert got[5][2] == b"\x05\x05\x05"
    assert [g[3] for g in got] == [False] * 19 + [True]
    assert gone  # a final chunk ends the stream
    assert bad_open["status"] == "error" and bad_open["code"] == "bad_request"
    assert "stream_gaps" not in host.summary


def test_stream_close_delivers_final_after_pending_chunks(broker_url):
    async def go():
        host = _host()
        got, done = _recorder(host)
        svc, cli = await _start(broker_url, host)
        subj, _ = await _auth(host, cli)
        sid = (await cli.request_private(P.rpc_subject(subj, "stream.open"), {"kind": "bytes"}))[
            "stream_id"
        ]
        for i in range(5):
            await _chunk(cli, subj, sid, i, b"ab")
        closed = await cli.request_private(P.rpc_subject(subj, "stream.close"), {"stream_id": sid})
        again = await cli.request_private(P.rpc_subject(subj, "stream.close"), {"stream_id": sid})
        await _stop(host, svc, cli)
        return got, closed, again

    got, closed, again = asyncio.run(go())
    assert closed["status"] == "ok"
    assert [g[1] for g in got] == [0, 1, 2, 3, 4, 5]
    assert got[-1][2:] == (b"", True)
    assert again["status"] == "error" and again["code"] == "bad_request"


def test_stream_gap_counted(broker_url, caplog):
    async def go():
        host = _host()
        got, done = _recorder(host)
        svc, cli = await _start(broker_url, host)
        subj, _ = await _auth(host, cli)
        sid = (await cli.request_private(P.rpc_subject(subj, "stream.open"), {"kind": "bytes"}))[
            "stream_id"
        ]
        await _chunk(cli, subj, sid, 0, b"a")
        await _chunk(cli, subj, sid, 2, b"c", final=True)
        await asyncio.wait_for(done.wait(), 3)
        await _stop(host, svc, cli)
        return host, got

    with caplog.at_level("WARNING"):
        host, got = asyncio.run(go())
    assert [g[1] for g in got] == [0, 2]  # gaps are tolerated, not fatal
    assert host.summary["stream_gaps"] == 1
    assert "gap" in caplog.text


def test_stream_idle_dropped(broker_url, caplog):
    clock = FakeClock()

    async def go():
        host = _host(clock=clock)
        got, _ = _recorder(host)
        svc, cli = await _start(broker_url, host)
        subj, _ = await _auth(host, cli)
        sid = (await cli.request_private(P.rpc_subject(subj, "stream.open"), {"kind": "bytes"}))[
            "stream_id"
        ]
        await _chunk(cli, subj, sid, 0, b"a")
        await cli.request_private(P.rpc_subject(subj, "session.ping"), {})  # orders the chunk
        clock.t += P.STREAM_IDLE_S - 1
        await asyncio.sleep(0.3)
        alive = sid in host.streams
        clock.t += 2
        for _ in range(30):
            if sid not in host.streams:
                break
            await asyncio.sleep(0.1)
        dropped = sid not in host.streams
        await _chunk(cli, subj, sid, 1, b"b")
        close = await cli.request_private(P.rpc_subject(subj, "stream.close"), {"stream_id": sid})
        await _stop(host, svc, cli)
        return alive, dropped, got, close

    with caplog.at_level("INFO"):
        alive, dropped, got, close = asyncio.run(go())
    assert alive
    assert dropped
    assert [g[1] for g in got] == [0]  # nothing delivered after the drop
    assert close["code"] == "bad_request"
    assert "idle" in caplog.text


def test_session_end_drops_streams(broker_url):
    async def go():
        host = _host()
        _recorder(host)
        svc, cli = await _start(broker_url, host)
        subj, _ = await _auth(host, cli)
        sid = (await cli.request_private(P.rpc_subject(subj, "stream.open"), {"kind": "bytes"}))[
            "stream_id"
        ]
        await cli.request_private(P.rpc_subject(subj, "session.close"), {})
        gone = sid not in host.streams
        await _stop(host, svc, cli)
        return gone

    assert asyncio.run(go())


def test_emit_reaches_only_delivery_root(broker_url):
    async def go():
        host = _host()
        sessions = {}

        @host.rpc("whoami")
        async def whoami(session, payload):
            sessions[payload["who"]] = session
            return {}

        svc, cli_a = await _start(broker_url, host)
        cli_b = NatsCore(broker_url)
        await cli_b.connect()
        subj_a, dr_a = await _auth(host, cli_a)
        subj_b, dr_b = await _auth(host, cli_b)
        await cli_a.request_private(P.rpc_subject(subj_a, "whoami"), {"who": "a"})
        await cli_b.request_private(P.rpc_subject(subj_b, "whoami"), {"who": "b"})

        a_msgs, b_msgs, spy_msgs, pub_msgs = [], [], [], []

        async def rec(lst, m):
            lst.append(m)

        await cli_a.subscribe(f"{dr_a}.>", functools.partial(rec, a_msgs))
        await cli_b.subscribe(f"{dr_b}.>", functools.partial(rec, b_msgs))
        spy = NatsCore(broker_url)
        await spy.connect()
        await spy.subscribe("_PRIV.>", functools.partial(rec, spy_msgs))
        await spy.subscribe(f"{SCOPE}.>", functools.partial(rec, pub_msgs))
        for c in (cli_a, cli_b, spy):
            await c.flush()

        await host.emit(sessions["a"], "note", {"text": "hi"})
        await host.emit(sessions["a"], "note", {"text": "again"})
        await host.emit_chunk(sessions["a"], "s1", 0, b"\x00\x01", final=True)
        await svc.flush()
        await asyncio.sleep(0.3)
        await host.stop()
        for c in (svc, cli_a, cli_b, spy):
            await c.close()
        return host, dr_a, a_msgs, b_msgs, spy_msgs, pub_msgs

    host, dr_a, a_msgs, b_msgs, spy_msgs, pub_msgs = asyncio.run(go())
    assert [m.subject for m in a_msgs] == [f"{dr_a}.note", f"{dr_a}.note", f"{dr_a}.stream.s1"]
    e1, e2 = json.loads(a_msgs[0].data), json.loads(a_msgs[1].data)
    assert e1["text"] == "hi" and e1["instance_id"] == host.instance_id
    assert e2["seq"] == e1["seq"] + 1  # per-instance monotonic event seq
    chunk = a_msgs[2]
    assert chunk.data == b"\x00\x01"
    assert chunk.headers[P.HDR_SEQ] == "0"
    assert chunk.headers[P.HDR_FINAL] == "1"
    assert P.HDR_KIND in chunk.headers
    assert b_msgs == []
    assert [m.subject for m in spy_msgs] == [m.subject for m in a_msgs]
    assert not [m for m in pub_msgs if not m.subject.endswith(".heartbeat")]


def test_emit_before_start_raises():
    host = _host()

    async def go():
        from lightfall_utils.ipc.services.host import Session

        s = Session("t", "app", None, "_PRIV.x.d.y", 0.0)
        with pytest.raises(RuntimeError):
            await host.emit(s, "x", {})

    asyncio.run(go())


def test_rpc_shutdown_stops_host(broker_url):
    async def go():
        host = _host()
        svc, cli = await _start(broker_url, host)
        subj, _ = await _auth(host, cli)
        serving = asyncio.create_task(host.serve_forever())
        rep = await cli.request_private(P.rpc_subject(subj, "shutdown"), {})
        await asyncio.wait_for(serving, 3)
        await svc.close()
        await cli.close()
        return rep, host

    rep, host = asyncio.run(go())
    assert rep["status"] == "ok"
    assert host.state == "stopping"


@pytest.mark.parametrize("action", ["shutdown", "stream.open", "stream.close"])
def test_builtin_actions_reserved(action):
    with pytest.raises(ValueError):
        _host().rpc(action)


def test_single_instance_in_process(broker_url):
    scope = "lfsingle"

    async def go():
        first = _host(scope=scope, single_instance=True)
        svc1 = NatsCore(broker_url)
        await svc1.connect()
        await first.start(svc1)
        second = _host(scope=scope, single_instance=True)
        svc2 = NatsCore(broker_url)
        await svc2.connect()
        with pytest.raises(InstanceConflictError) as ei:
            await second.start(svc2)
        lax = _host(scope=scope, single_instance=False)
        await lax.start(svc2)  # opt-out skips the check
        await lax.stop()
        await first.stop()
        third = _host(scope=scope, single_instance=True)
        await third.start(svc2)  # the stopped host's final "stopping" beat does not count
        await third.stop()
        await svc1.close()
        await svc2.close()
        return ei.value

    err = asyncio.run(go())
    assert str(err) == f"another instance is already serving {P.root(scope, 'echo')}"
