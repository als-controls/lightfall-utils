"""Qt adapter: launch state (I5), hung-child restart and broker reconnect (I4), and the
info/reset/public-event signals (I3, I7)."""

import asyncio
import json
import socket
import sys
import uuid

from lightfall_utils.ipc import LocalNatsServer
from lightfall_utils.ipc.services import client as client_mod
from lightfall_utils.ipc.services import protocol as P
from lightfall_utils.ipc.services.core import NatsCore
from lightfall_utils.ipc.services.descriptor import ServiceDescriptor
from lightfall_utils.ipc.services.launcher import LocalLauncher
from lightfall_utils.ipc.services.qt import QtServiceClient

FREEZABLE = """
import sys, time
from lightfall_utils.ipc.services.cli import run_service
from lightfall_utils.ipc.services.echo import make_host

host = make_host()

@host.rpc("freeze")
async def freeze(session, payload):
    time.sleep(120)  # blocks the event loop: no heartbeats, but the process stays alive
    return {}

sys.exit(run_service(host))
"""


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _local(url, tmp_path, *, script=None, **kw):
    scope = f"t{uuid.uuid4().hex[:10]}.local"
    if script is None:
        prog = ("-m", "lightfall_utils.ipc.services.echo")
    else:
        path = tmp_path / "svc_script.py"
        path.write_text(script)
        prog = (str(path),)
    command = (sys.executable, *prog, "--scope", scope, "--nats-url", url, "--token-stdin")
    desc = ServiceDescriptor(
        name="echo", scope=scope, contract="1.0", launch="local", command=command
    )
    created: list[LocalLauncher] = []

    async def fast_sleep(d):
        await asyncio.sleep(0.2)

    def make_launcher(token):
        la = LocalLauncher(
            command, session_token=token, log_path=tmp_path / "svc.log", sleep=fast_sleep
        )
        created.append(la)
        return la

    client = QtServiceClient(
        desc, url, scope=scope, app_name="test", app_version="0", launcher=make_launcher, **kw
    )
    return client, created, scope


def _wait_health(qtbot, client, health, timeout=20000):
    qtbot.waitUntil(lambda: client.health == health, timeout=timeout)


def test_launch_reports_starting_never_down_and_info_changed(qtbot, broker_url, tmp_path):
    client, _, _ = _local(broker_url, tmp_path)
    states: list[str] = []
    infos: list[dict] = []
    client.state_changed.connect(states.append)
    client.info_changed.connect(infos.append)
    try:
        client.start()
        _wait_health(qtbot, client, "up")
        qtbot.waitUntil(lambda: bool(infos), timeout=5000)
    finally:
        client.stop()
    assert "down" not in states
    assert states[0] == "starting" and states[-1] == "up"
    assert infos[-1]["state"] == "ready"


def test_hung_child_is_restarted(qtbot, broker_url, tmp_path, monkeypatch):
    monkeypatch.setattr(client_mod, "_DEGRADED_AFTER_S", 0.5)
    monkeypatch.setattr(client_mod, "_DOWN_AFTER_S", 1.0)
    client, created, _ = _local(broker_url, tmp_path, script=FREEZABLE, hung_restart_s=1.0)
    try:
        client.start()
        _wait_health(qtbot, client, "up")
        old = created[0]._proc
        with qtbot.waitSignal(client.restarted, timeout=30000):
            client.call("freeze", {}, timeout=0.2)
        _wait_health(qtbot, client, "up")
        assert old.returncode is not None  # the hung process was killed
        assert created[0]._proc is not old
        assert any("restarted by supervisor" in s for s in created[0].last_stderr)
        assert client.call("echo", {"x": 1}).result(timeout=5)["echo"] == {"x": 1}
    finally:
        client.stop()


def test_broker_restart_reconnects_child_and_client(qtbot, tmp_path):
    port = _free_port()
    url = f"nats://127.0.0.1:{port}"
    server = LocalNatsServer(port=port, host="127.0.0.1")
    server.start(timeout_s=10.0)
    client, created, _ = _local(url, tmp_path)
    conn: list[bool] = []
    client.connection_changed.connect(conn.append)
    try:
        client.start()
        _wait_health(qtbot, client, "up")
        proc = created[0]._proc
        with qtbot.waitSignal(
            client.connection_changed, timeout=10000, check_params_cb=lambda c: c is False
        ):
            server.stop()
        server = LocalNatsServer(port=port, host="127.0.0.1")
        server.start(timeout_s=10.0)
        qtbot.waitUntil(lambda: client.broker_connected, timeout=20000)
        _wait_health(qtbot, client, "up", timeout=30000)
        # Right after the reconnect the child may not have re-subscribed yet: retry briefly.
        replies: list[dict] = []

        def echoed() -> bool:
            replies.append(client.call("echo", {"y": 2}).result(timeout=5))
            return replies[-1].get("echo") == {"y": 2}

        qtbot.waitUntil(echoed, timeout=20000)
        _wait_health(qtbot, client, "up", timeout=30000)
        assert created[0]._proc is proc  # the same child survived the broker restart
    finally:
        client.stop()
        server.stop()
    assert conn[:3] == [True, False, True]


def test_public_event_and_session_reset_signals(qtbot, broker_url, tmp_path):
    client, _, scope = _local(broker_url, tmp_path)
    client.watch_public_event("state")
    try:
        client.start()
        _wait_health(qtbot, client, "up")
        root = P.root(scope, "echo")

        async def publish():
            core = NatsCore(broker_url)
            await core.connect()
            await core.publish(f"{root}.events.state", json.dumps({"state": "x"}).encode())
            await core.flush()
            await core.close()

        with qtbot.waitSignal(client.public_event, timeout=5000) as blocker:
            asyncio.run(publish())
        assert blocker.args == ["state", {"state": "x"}]

        inner = client._client
        with qtbot.waitSignal(client.session_reset, timeout=5000) as reset:
            client._loop.call_soon_threadsafe(inner._fire, inner._reset_cbs, ["sid1"])
        assert reset.args == [["sid1"]]
    finally:
        client.stop()


def test_adopt_stream_through_adapter(qtbot, broker_url, tmp_path):
    client, _, _ = _local(broker_url, tmp_path)
    try:
        client.start()
        _wait_health(qtbot, client, "up")
        sid = client.open_stream("bytes").result(timeout=5)
        client.forget_stream(sid).result(timeout=5)
        try:
            client.send_chunk(sid, 0, b"x").result(timeout=5)
            raise AssertionError("expected StreamClosedError")
        except client_mod.StreamClosedError:
            pass
        client.adopt_stream(sid, "bytes").result(timeout=5)
        client.send_chunk(sid, 0, b"x").result(timeout=5)
    finally:
        client.stop()
