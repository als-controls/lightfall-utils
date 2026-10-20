import asyncio
import json
import time

import pytest
from nats.errors import TimeoutError as NatsTimeout

from lightfall_utils.ipc.capability import new_token
from lightfall_utils.ipc.services import protocol as P
from lightfall_utils.ipc.services.core import NatsCore
from lightfall_utils.ipc.services.host import ServiceHost, Session, allow_apps

SCOPE = "lftest"
CONTRACT = "1.2"

INFO_KEYS = {
    "instance_id",
    "name",
    "state",
    "version",
    "svc_protocol",
    "contract",
    "priv_root",
    "single_instance",
    "summary",
    "ts",
}


class FakeClock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


def _host(name="echo", **kw):
    kw.setdefault("contract", CONTRACT)
    kw.setdefault("version", "0.1.0")
    kw.setdefault("heartbeat_s", 0.1)
    return ServiceHost(name, SCOPE, **kw)


async def _pair(broker_url, host):
    svc, cli = NatsCore(broker_url), NatsCore(broker_url)
    await svc.connect()
    await cli.connect()
    await host.start(svc)
    return svc, cli


async def _teardown(host, *cores):
    await host.stop()
    for c in cores:
        await c.close()


def _auth_payload(cli, **extra):
    p = {
        "app_name": "tester",
        "app_version": "9.9",
        "user": "ron",
        "delivery_token": new_token(),
        "client_priv_root": cli.priv_root,
    }
    p.update(extra)
    return p


async def _auth(host, cli, **extra):
    return await cli.request_private(P.auth_subject(host.priv_root), _auth_payload(cli, **extra))


def test_meta_info_and_discover_carry_priv_root(broker_url):
    async def go():
        host = _host()
        svc, cli = await _pair(broker_url, host)
        info = await cli.request_public(P.meta_info(P.root(SCOPE, "echo")), {})
        disc = await cli.request_public(P.discover(SCOPE), {})
        await _teardown(host, svc, cli)
        return host, svc, info, disc

    host, svc, info, disc = asyncio.run(go())
    for d in (info, disc):
        assert set(d) == INFO_KEYS
        assert d["priv_root"] == svc.priv_root == host.priv_root
        assert d["instance_id"] == host.instance_id
        assert d["name"] == "echo"
        assert d["contract"] == CONTRACT
        assert d["svc_protocol"] == P.SVC_PROTOCOL
        assert d["single_instance"] is True


def test_heartbeat_payload_fields(broker_url):
    async def go():
        host = _host()
        host.summary = {"jobs": 3}
        cli = NatsCore(broker_url)
        await cli.connect()
        beats = []
        two = asyncio.get_running_loop().create_future()

        async def cb(msg):
            beats.append(json.loads(msg.data))
            if len(beats) >= 2 and not two.done():
                two.set_result(None)

        await cli.subscribe(P.heartbeat(P.root(SCOPE, "echo")), cb)
        await cli.flush()
        svc = NatsCore(broker_url)
        await svc.connect()
        await host.start(svc)
        host.state = "ready"
        await asyncio.wait_for(two, 3)
        got = list(beats)
        await _teardown(host, svc, cli)
        return host, got

    host, beats = asyncio.run(go())
    assert len(beats) >= 2
    for b in beats:
        assert set(b) == INFO_KEYS
        assert b["instance_id"] == host.instance_id
        assert b["summary"] == {"jobs": 3}
        assert abs(b["ts"] - time.time()) < 60
    assert beats[-1]["state"] == "ready"  # beats after the state change carry it


def test_auth_approves_and_rpc_roundtrip(broker_url):
    async def go():
        host = _host()
        seen = {}

        @host.rpc("echo")
        async def echo(session, payload):
            seen["session"] = session
            return {"echo": payload["v"]}

        @host.rpc("sync_echo")
        def sync_echo(session, payload):
            return {"sync": payload["v"]}

        svc, cli = await _pair(broker_url, host)
        auth = _auth_payload(cli)
        rep = await cli.request_private(P.auth_subject(host.priv_root), auth)
        subj = rep["session_subject"]
        out = await cli.request_private(P.rpc_subject(subj, "echo"), {"v": 5})
        out2 = await cli.request_private(P.rpc_subject(subj, "sync_echo"), {"v": 6})
        ping = await cli.request_private(P.rpc_subject(subj, "session.ping"), {})
        unknown = await cli.request_private(P.rpc_subject(subj, "nope"), {})
        await _teardown(host, svc, cli)
        return host, cli, auth, rep, out, out2, ping, unknown, seen

    host, cli, auth, rep, out, out2, ping, unknown, seen = asyncio.run(go())
    assert rep["status"] == "approved"
    assert rep["contract_version"] == CONTRACT
    tok = rep["session_token"]
    assert rep["session_subject"] == P.session_subject(host.priv_root, tok)
    assert out == {"status": "ok", "echo": 5, "contract_version": CONTRACT}
    assert out2["sync"] == 6
    assert ping["status"] == "ok"
    assert unknown["status"] == "error" and unknown["code"] == "unknown"
    s = seen["session"]
    assert isinstance(s, Session)
    assert s.token == tok
    assert s.app_name == "tester"
    assert s.user == "ron"
    assert s.delivery_root == P.delivery_root(cli.priv_root, auth["delivery_token"])


def test_auth_refuses_mismatched_client_priv_root(broker_url):
    async def go():
        host = _host()
        svc, cli = await _pair(broker_url, host)
        other = NatsCore(broker_url)  # never connected; just a different, valid priv_root
        bad_shape = await _auth(host, cli, client_priv_root="_PRIV.NOTVALID")
        mismatch = await _auth(host, cli, client_priv_root=other.priv_root)
        await _teardown(host, svc, cli)
        return bad_shape, mismatch

    for rep in asyncio.run(go()):
        assert rep["status"] == "error"
        assert rep["code"] == "denied"
        assert "session_token" not in rep


def test_trust_callable_denies(broker_url):
    async def go():
        host = _host(trust=allow_apps(["someone-else"]))
        svc, cli = await _pair(broker_url, host)
        rep = await _auth(host, cli)
        await _teardown(host, svc, cli)
        return rep

    rep = asyncio.run(go())
    assert rep["status"] == "error" and rep["code"] == "denied"


def test_allow_apps():
    check = allow_apps(["a", "b"])
    assert check({"app_name": "a"})
    assert check({"app_name": "b"})
    assert not check({"app_name": "c"})
    assert not check({})


def test_stdin_token_required(broker_url):
    secret = new_token()

    async def go():
        host = _host(stdin_token=secret)
        svc, cli = await _pair(broker_url, host)
        wrong = await _auth(host, cli, session_token="wrong")
        missing = await _auth(host, cli)
        right = await _auth(host, cli, session_token=secret)
        await _teardown(host, svc, cli)
        return wrong, missing, right

    wrong, missing, right = asyncio.run(go())
    assert wrong["code"] == "denied"
    assert missing["code"] == "denied"
    assert right["status"] == "approved"
    assert right["session_token"] == secret


def test_reauth_with_stdin_token_after_lease_expiry(broker_url):
    secret = new_token()
    clock = FakeClock()

    async def go():
        host = _host(stdin_token=secret, clock=clock)

        @host.rpc("hi")
        async def hi(session, payload):
            return {"hi": True}

        svc, cli = await _pair(broker_url, host)
        first = await _auth(host, cli, session_token=secret)
        subj = first["session_subject"]
        assert (await cli.request_private(P.rpc_subject(subj, "hi"), {}))["hi"]
        clock.t += P.LEASE_S + 1
        expired = None
        try:
            await cli.request_private(P.rpc_subject(subj, "hi"), {}, timeout=0.5)
        except NatsTimeout as e:
            expired = e
        again = await _auth(host, cli, session_token=secret)
        after = await cli.request_private(P.rpc_subject(again["session_subject"], "hi"), {})
        await _teardown(host, svc, cli)
        return expired, again, after

    expired, again, after = asyncio.run(go())
    assert expired is not None
    assert again["status"] == "approved"
    assert again["session_token"] == secret
    assert after["hi"] is True


def test_unknown_session_token_ignored(broker_url):
    async def go():
        host = _host()

        @host.rpc("hi")
        async def hi(session, payload):
            return {"hi": True}

        svc, cli = await _pair(broker_url, host)
        subj = P.session_subject(host.priv_root, new_token())
        with pytest.raises(NatsTimeout):
            await cli.request_private(P.rpc_subject(subj, "hi"), {}, timeout=0.5)
        await _teardown(host, svc, cli)

    asyncio.run(go())


def test_handler_exception_is_error_reply(broker_url, caplog):
    async def go():
        host = _host()

        @host.rpc("boom")
        async def boom(session, payload):
            raise RuntimeError("kaboom")

        @host.rpc("hi")
        async def hi(session, payload):
            return {"hi": True}

        svc, cli = await _pair(broker_url, host)
        subj = (await _auth(host, cli))["session_subject"]
        err = await cli.request_private(P.rpc_subject(subj, "boom"), {})
        ok = await cli.request_private(P.rpc_subject(subj, "hi"), {})
        await _teardown(host, svc, cli)
        return err, ok

    with caplog.at_level("ERROR"):
        err, ok = asyncio.run(go())
    assert err == P.svc_error(CONTRACT, "unknown", "kaboom")
    assert ok["hi"] is True
    assert "Traceback" in caplog.text or any(r.exc_info for r in caplog.records)


def test_lease_expiry_calls_on_session_end(broker_url):
    clock = FakeClock()

    async def go():
        host = _host(clock=clock)
        ended = []
        done = asyncio.get_running_loop().create_future()

        def on_end(session):
            ended.append(session)
            if not done.done():
                done.set_result(None)

        host.on_session_end(on_end)
        svc, cli = await _pair(broker_url, host)
        rep = await _auth(host, cli)
        clock.t += P.LEASE_S - 1
        await cli.request_private(P.rpc_subject(rep["session_subject"], "session.ping"), {})
        clock.t += P.LEASE_S - 1  # pinged recently: still alive
        await asyncio.sleep(0.3)
        alive = not ended
        clock.t += 2
        await asyncio.wait_for(done, 3)
        await _teardown(host, svc, cli)
        return rep, alive, ended

    rep, alive, ended = asyncio.run(go())
    assert alive
    assert [s.token for s in ended] == [rep["session_token"]]


def test_session_close_ends_session(broker_url):
    async def go():
        host = _host()
        ended = []
        host.on_session_end(lambda s: ended.append(s.token))

        @host.rpc("hi")
        async def hi(session, payload):
            return {"hi": True}

        svc, cli = await _pair(broker_url, host)
        rep = await _auth(host, cli)
        subj = rep["session_subject"]
        closed = await cli.request_private(P.rpc_subject(subj, "session.close"), {})
        with pytest.raises(NatsTimeout):
            await cli.request_private(P.rpc_subject(subj, "hi"), {}, timeout=0.5)
        await _teardown(host, svc, cli)
        return rep, closed, ended

    rep, closed, ended = asyncio.run(go())
    assert closed["status"] == "ok"
    assert ended == [rep["session_token"]]


def test_serve_forever_returns_after_stop(broker_url):
    async def go():
        host = _host()
        svc = NatsCore(broker_url)
        await svc.connect()
        await host.start(svc)
        task = asyncio.create_task(host.serve_forever())
        await asyncio.sleep(0.05)
        assert not task.done()
        await host.stop()
        await asyncio.wait_for(task, 2)
        await svc.close()

    asyncio.run(go())
