"""Subprocess tests for ``run_service`` via the echo reference service.

Windows has no SIGTERM delivery to a child (``terminate()`` is TerminateProcess), so
``test_sigterm_exits`` runs on POSIX only; the other shutdown paths (stdin EOF,
``rpc.shutdown``) are exercised everywhere.
"""

import asyncio
import json
import signal
import subprocess
import sys
import time
import uuid

import pytest

from lightfall_utils.ipc.capability import new_token
from lightfall_utils.ipc.services import cli as cli_mod
from lightfall_utils.ipc.services import protocol as P
from lightfall_utils.ipc.services.cli import run_service
from lightfall_utils.ipc.services.core import NatsCore
from lightfall_utils.ipc.services.echo import make_host

ECHO = [sys.executable, "-m", "lightfall_utils.ipc.services.echo"]


def _scope() -> str:
    return f"lfcli.{uuid.uuid4().hex[:8]}"


def _spawn(broker_url, scope, *extra, stdin=subprocess.DEVNULL):
    return subprocess.Popen(
        [*ECHO, "--scope", scope, "--nats-url", broker_url, "--log-level", "info", *extra],
        stdin=stdin,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


async def _wait_ready(cli, scope, timeout=15.0) -> dict:
    deadline = time.monotonic() + timeout
    subject = P.meta_info(P.root(scope, "echo"))
    while time.monotonic() < deadline:
        try:
            info = await cli.request_public(subject, {}, timeout=0.5)
            if info.get("state") == "ready":
                return info
        except Exception:
            pass
        await asyncio.sleep(0.1)
    raise AssertionError("echo service never became ready")


async def _auth(cli, info, **extra):
    payload = {
        "app_name": "tester",
        "app_version": "1",
        "delivery_token": new_token(),
        "client_priv_root": cli.priv_root,
        **extra,
    }
    return await cli.request_private(P.auth_subject(info["priv_root"]), payload)


def _kill(proc):
    if proc.poll() is None:
        proc.kill()
    # communicate() flushes stdin first; on POSIX before Python 3.13 a stdin
    # the test already closed raises "I/O operation on closed file".
    if proc.stdin is not None and proc.stdin.closed:
        proc.stdin = None
    proc.communicate(timeout=5)


def test_stdin_eof_exits(broker_url):
    scope = _scope()
    secret = new_token()
    proc = _spawn(broker_url, scope, "--token-stdin", stdin=subprocess.PIPE)
    try:
        proc.stdin.write(secret + "\n")
        proc.stdin.flush()

        async def go():
            cli = NatsCore(broker_url)
            await cli.connect()
            info = await _wait_ready(cli, scope)
            wrong = await _auth(cli, info)
            right = await _auth(cli, info, session_token=secret)
            await cli.close()
            return wrong, right

        wrong, right = asyncio.run(go())
        assert wrong["code"] == "denied"
        assert right["status"] == "approved" and right["session_token"] == secret
        proc.stdin.close()
        t0 = time.monotonic()
        rc = proc.wait(timeout=10)
        assert time.monotonic() - t0 < 2.0
        assert rc == 0
    finally:
        _kill(proc)


def test_rpc_shutdown_exits(broker_url):
    scope = _scope()
    proc = _spawn(broker_url, scope)
    try:

        async def go():
            cli = NatsCore(broker_url)
            await cli.connect()
            info = await _wait_ready(cli, scope)
            subj = (await _auth(cli, info))["session_subject"]
            echoed = await cli.request_private(P.rpc_subject(subj, "echo"), {"v": 1})
            slow = await cli.request_private(P.rpc_subject(subj, "slow"), {"seconds": 0.1})
            rep = await cli.request_private(P.rpc_subject(subj, "shutdown"), {})
            await cli.close()
            return echoed, slow, rep

        echoed, slow, rep = asyncio.run(go())
        assert echoed["echo"] == {"v": 1}
        assert slow["status"] == "ok"
        assert rep["status"] == "ok"
        assert proc.wait(timeout=5) == 0
    finally:
        _kill(proc)


@pytest.mark.skipif(sys.platform == "win32", reason="no SIGTERM delivery to children on Windows")
def test_sigterm_exits(broker_url):
    scope = _scope()
    proc = _spawn(broker_url, scope)
    try:

        async def go():
            cli = NatsCore(broker_url)
            await cli.connect()
            await _wait_ready(cli, scope)
            await cli.close()

        asyncio.run(go())
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=5) == 0
    finally:
        _kill(proc)


def test_single_instance_refusal(broker_url):
    scope = _scope()
    first = _spawn(broker_url, scope)
    second = None
    try:

        async def go():
            cli = NatsCore(broker_url)
            await cli.connect()
            await _wait_ready(cli, scope)
            await cli.close()

        asyncio.run(go())
        second = _spawn(broker_url, scope)
        _, err = second.communicate(timeout=20)
        assert second.returncode == 3
        assert f"another instance is already serving {P.root(scope, 'echo')}" in err
        assert first.poll() is None  # the first one keeps serving
    finally:
        _kill(first)
        if second is not None:
            _kill(second)


def test_echo_bytes_stream_emits_total(broker_url):
    """In-process: the echo host's ``bytes`` stream reports its total on close."""

    async def go():
        host = make_host(_scope(), single_instance=False, heartbeat_s=0.1)
        svc, cli = NatsCore(broker_url), NatsCore(broker_url)
        await svc.connect()
        await cli.connect()
        await host.start(svc)
        dt = new_token()
        rep = await cli.request_private(
            P.auth_subject(host.priv_root),
            {
                "app_name": "t",
                "app_version": "1",
                "delivery_token": dt,
                "client_priv_root": cli.priv_root,
            },
        )
        dr = P.delivery_root(cli.priv_root, dt)
        got = asyncio.get_running_loop().create_future()

        async def on_total(msg):
            if not got.done():
                got.set_result((msg.subject, msg.data))

        await cli.subscribe(f"{dr}.>", on_total)
        await cli.flush()
        subj = rep["session_subject"]
        sid = (await cli.request_private(P.rpc_subject(subj, "stream.open"), {"kind": "bytes"}))[
            "stream_id"
        ]
        for i in range(4):
            await cli.publish(
                P.stream_subject(subj, sid),
                b"x" * 10,
                headers={P.HDR_SEQ: str(i), P.HDR_KIND: "bytes", P.HDR_FINAL: "0"},
            )
        await cli.request_private(P.rpc_subject(subj, "stream.close"), {"stream_id": sid})
        subject, data = await asyncio.wait_for(got, 3)
        await host.stop()
        await svc.close()
        await cli.close()
        return dr, sid, subject, data

    dr, sid, subject, data = asyncio.run(go())
    assert subject == f"{dr}.stream_total"
    body = json.loads(data)
    assert body["stream_id"] == sid
    assert body["bytes"] == 40
    assert body["chunks"] == 4


def test_run_service_bad_args_returns_2():
    host = make_host("lfcli.x")
    assert run_service(host, ["--log-level", "nope"]) == 2


def test_run_service_unreachable_broker_returns_1(monkeypatch):
    monkeypatch.setattr(cli_mod, "CONNECT_TIMEOUT_S", 1.0)
    host = make_host("lfcli.x", single_instance=False)
    assert run_service(host, ["--nats-url", "nats://127.0.0.1:1"]) == 1


class _CoreSpy:
    """Wraps NatsCore construction in the cli to capture kwargs and the on_closed hook."""

    def __init__(self, monkeypatch):
        self.kwargs: dict = {}
        real = cli_mod.NatsCore
        spy = self

        def make(url, **kw):
            spy.kwargs = kw
            return real(url, **kw)

        monkeypatch.setattr(cli_mod, "NatsCore", make)


def test_external_service_exits_1_when_broker_connection_closes(broker_url, monkeypatch):
    spy = _CoreSpy(monkeypatch)
    host = make_host(_scope(), single_instance=False)
    orig_start = host.start

    async def start(core):
        await orig_start(core)
        # Simulate nats-py giving up on reconnecting: it calls closed_cb.
        asyncio.get_running_loop().call_later(
            0.3, lambda: asyncio.ensure_future(core._wrap(spy.kwargs["on_closed"], "closed")())
        )

    host.start = start
    t0 = time.monotonic()
    rc = run_service(host, ["--nats-url", broker_url])
    assert rc == 1
    assert time.monotonic() - t0 < 10
    assert spy.kwargs["max_reconnect_attempts"] == 60


def test_local_service_reconnects_forever(broker_url, monkeypatch):
    import io

    spy = _CoreSpy(monkeypatch)
    monkeypatch.setattr(sys, "stdin", io.StringIO(new_token() + "\n"))  # EOF right after
    host = make_host(_scope())
    rc = run_service(host, ["--nats-url", broker_url, "--token-stdin"])
    assert rc == 0  # stdin EOF is a clean stop, and our own close is not "broker lost"
    assert spy.kwargs["max_reconnect_attempts"] == -1


def test_local_child_survives_local_broker_restart():
    from lightfall_utils.ipc import LocalNatsServer

    from .conftest import _free_port

    port = _free_port()
    url = f"nats://127.0.0.1:{port}"
    server = LocalNatsServer(port=port, host="127.0.0.1")
    server.start(timeout_s=10.0)
    scope = _scope()
    proc = _spawn(url, scope, "--token-stdin", stdin=subprocess.PIPE)
    proc.stdin.write(new_token() + "\n")
    proc.stdin.flush()

    async def ready():
        c = NatsCore(url)
        await asyncio.wait_for(c.connect(), 5)
        try:
            return await _wait_ready(c, scope)
        finally:
            await c.close()

    try:
        first = asyncio.run(ready())
        server.stop()
        time.sleep(1.0)
        server = LocalNatsServer(port=port, host="127.0.0.1")
        server.start(timeout_s=10.0)
        second = asyncio.run(ready())  # the same process answers on the new broker
        assert proc.poll() is None
        assert second["instance_id"] == first["instance_id"]
    finally:
        _kill(proc)
        server.stop()
