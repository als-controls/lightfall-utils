import sys
import threading
import uuid

import pytest

from lightfall_utils.ipc.services.descriptor import ServiceDescriptor
from lightfall_utils.ipc.services.launcher import LocalLauncher
from lightfall_utils.ipc.services.qt import QtServiceClient


def _echo(broker_url: str, tmp_path, *, factory: bool = True):
    scope = f"t{uuid.uuid4().hex[:10]}.local"
    command = (
        sys.executable,
        "-m",
        "lightfall_utils.ipc.services.echo",
        "--scope",
        scope,
        "--nats-url",
        broker_url,
        "--token-stdin",
    )
    desc = ServiceDescriptor(
        name="echo", scope=scope, contract="1.0", launch="local", command=command
    )
    log_path = tmp_path / "echo.log"
    created: list[LocalLauncher] = []

    def make_launcher(token: str) -> LocalLauncher:
        la = LocalLauncher(command, session_token=token, log_path=log_path)
        created.append(la)
        return la

    client = QtServiceClient(
        desc,
        broker_url,
        scope=scope,
        app_name="test",
        app_version="0",
        launcher=make_launcher,
    )
    return client, created, log_path


def _wait_up(qtbot, client, timeout=20000):
    if client.health == "up":
        return
    with qtbot.waitSignal(
        client.state_changed, timeout=timeout, check_params_cb=lambda s: s == "up"
    ):
        pass


@pytest.fixture
def started(qtbot, broker_url, tmp_path):
    client, created, log_path = _echo(broker_url, tmp_path)
    yield client, created, log_path
    client.stop()


def test_state_signal_on_main_thread(qtbot, broker_url, tmp_path):
    client, created, _ = _echo(broker_url, tmp_path)
    seen: list[tuple[str, bool]] = []
    client.state_changed.connect(
        lambda s: seen.append((s, threading.current_thread() is threading.main_thread()))
    )
    try:
        with qtbot.waitSignal(
            client.state_changed, timeout=20000, check_params_cb=lambda s: s == "up"
        ):
            client.start()
        assert seen and seen[-1] == ("up", True)
        assert all(on_main for _, on_main in seen)
        assert client.health == "up"
        # The minted token reached both the launcher and the client.
        assert len(created) == 1
        assert client.session_token and created[0].session_token == client.session_token
        names = [t.name for t in threading.enumerate()]
        assert "svc-echo" in names
    finally:
        client.stop()
    assert "svc-echo" not in [t.name for t in threading.enumerate()]


def test_call_future(qtbot, started):
    client, _, _ = started
    client.start()
    _wait_up(qtbot, client)
    fut = client.call("echo", {"x": 1})
    reply = fut.result(timeout=5)
    assert reply["status"] == "ok"
    assert reply["echo"] == {"x": 1}
    # Errors come back as svc_error dicts, not exceptions.
    bad = client.call("nope", {}).result(timeout=5)
    assert bad["status"] == "error"


def test_event_signal_and_streams(qtbot, started):
    client, _, _ = started
    client.watch_event("stream_total")
    client.start()
    _wait_up(qtbot, client)
    sid = client.open_stream("bytes").result(timeout=5)
    client.send_chunk(sid, 0, b"abc").result(timeout=5)
    with qtbot.waitSignal(client.event, timeout=5000) as blocker:
        client.send_chunk(sid, 1, b"de", final=True).result(timeout=5)
    name, payload = blocker.args
    assert name == "stream_total"
    assert payload["stream_id"] == sid and payload["bytes"] == 5 and payload["chunks"] == 2


def test_stop_shuts_down_child(qtbot, broker_url, tmp_path):
    client, created, log_path = _echo(broker_url, tmp_path)
    client.start()
    _wait_up(qtbot, client)
    la = created[0]
    proc = la._proc
    assert proc is not None and proc.returncode is None
    client.stop()
    assert la.state == "stopped"
    assert proc.returncode == 0
    assert "shutdown requested by test" in log_path.read_text(errors="replace")
    # Calls after stop fail fast.
    with pytest.raises(RuntimeError):
        client.call("echo", {}).result(timeout=1)


def test_launcher_instance_uses_its_token(broker_url, tmp_path):
    desc = ServiceDescriptor(
        name="echo", scope="x.local", contract="1.0", launch="local", command=("x",)
    )
    la = LocalLauncher(["x"], session_token="tok-abc", log_path=tmp_path / "l.txt")
    client = QtServiceClient(
        desc, broker_url, scope="x.local", app_name="a", app_version="0", launcher=la
    )
    assert client.session_token == "tok-abc"
    with pytest.raises(ValueError):
        QtServiceClient(
            desc,
            broker_url,
            scope="x.local",
            app_name="a",
            app_version="0",
            launcher=la,
            session_token="other",
        )


def test_broker_unreachable_reports_down(qtbot, tmp_path):
    desc = ServiceDescriptor(name="ext", scope="x.local", contract="1.0", launch="external")
    client = QtServiceClient(
        desc, "nats://127.0.0.1:1", scope="x.local", app_name="a", app_version="0"
    )
    try:
        with qtbot.waitSignal(
            client.state_changed, timeout=15000, check_params_cb=lambda s: s == "down"
        ):
            client.start()
        assert client.last_error
    finally:
        client.stop()


def test_launcher_gave_up_reports_down_with_stderr(qtbot, broker_url, tmp_path):
    async def fast_sleep(d):
        pass

    cmd = [sys.executable, "-c", "import sys; sys.stderr.write('boom-xyz\n'); sys.exit(1)"]
    desc = ServiceDescriptor(
        name="crashy", scope="crash.local", contract="1.0", launch="local", command=tuple(cmd)
    )
    client = QtServiceClient(
        desc,
        broker_url,
        scope="crash.local",
        app_name="a",
        app_version="0",
        launcher=lambda tok: LocalLauncher(
            cmd, session_token=tok, log_path=tmp_path / "c.log", sleep=fast_sleep
        ),
    )
    try:
        client.start()
        qtbot.waitUntil(lambda: "boom-xyz" in client.last_error, timeout=20000)
        assert client.health == "down"
        assert client.launcher.state == "gave_up"
    finally:
        client.stop()


def _free_port() -> int:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_broker_started_late_reaches_up(qtbot, tmp_path):
    from lightfall_utils.ipc import LocalNatsServer

    port = _free_port()
    url = f"nats://127.0.0.1:{port}"
    client, created, _ = _echo(url, tmp_path)
    server = LocalNatsServer(port=port, host="127.0.0.1")
    try:
        with qtbot.waitSignal(
            client.state_changed, timeout=15000, check_params_cb=lambda s: s == "down"
        ):
            client.start()
        assert "cannot connect" in client.last_error
        # Queued calls resolve promptly with the "no session" error dict.
        reply = client.call("echo", {}).result(timeout=5)
        assert reply["status"] == "error"
        server.start(timeout_s=10.0)
        _wait_up(qtbot, client, timeout=30000)
        assert client.call("echo", {"a": 1}).result(timeout=5)["echo"] == {"a": 1}
    finally:
        client.stop()
        server.stop()


def test_restarted_signal_after_child_killed(qtbot, started):
    client, created, _ = started
    client.start()
    _wait_up(qtbot, client)
    old_proc = created[0]._proc
    with qtbot.waitSignal(client.restarted, timeout=30000):
        old_proc.kill()
    _wait_up(qtbot, client)
    assert created[0]._proc is not old_proc
    assert client.call("echo", {"b": 2}).result(timeout=5)["echo"] == {"b": 2}
