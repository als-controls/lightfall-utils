import asyncio
import sys
import time

from lightfall_utils.ipc.services.launcher import LocalLauncher


def _py(code: str) -> list[str]:
    return [sys.executable, "-c", code]


async def _wait_for(pred, timeout=15.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        await asyncio.sleep(0.02)
    return False


def test_token_on_stdin(tmp_path):
    out = tmp_path / "tok.txt"
    code = (
        "import sys; line = sys.stdin.readline(); "
        f"open({str(out)!r}, 'w').write(repr(line)); "
        "sys.stdin.read()"
    )

    async def main():
        la = LocalLauncher(_py(code), session_token="tok-123", log_path=tmp_path / "log.txt")
        await la.start()
        assert await _wait_for(lambda: out.exists() and out.read_text())
        assert la.state == "running"
        await la.stop(None)
        assert la.state == "stopped"

    asyncio.run(main())
    assert out.read_text() == repr("tok-123\n")


def test_restart_backoff_sequence(tmp_path):
    delays: list[float] = []
    exits: list[int] = []

    async def fake_sleep(d):
        delays.append(d)
        await asyncio.sleep(0)

    async def main():
        la = LocalLauncher(
            _py("import sys; sys.exit(3)"),
            session_token="t",
            log_path=tmp_path / "log.txt",
            clock=lambda: 100.0,
            sleep=fake_sleep,
        )
        la.on_exit(exits.append)
        await la.start()
        assert await _wait_for(lambda: la.state == "gave_up")
        await la.stop(None)

    asyncio.run(main())
    assert delays == [1, 2, 5, 10]
    assert exits == [3] * 5


def test_gives_up_after_5_in_60s(tmp_path):
    async def fake_sleep(d):
        await asyncio.sleep(0)

    async def main():
        la = LocalLauncher(
            _py("import sys; sys.stderr.write('boom message\\n'); sys.exit(1)"),
            session_token="t",
            log_path=tmp_path / "log.txt",
            clock=lambda: 0.0,
            sleep=fake_sleep,
        )
        await la.start()
        assert await _wait_for(lambda: la.state == "gave_up")
        assert any("boom message" in line for line in la.last_stderr)
        assert len(la.last_stderr) <= 20
        await la.stop(None)
        assert la.state == "stopped"

    asyncio.run(main())
    assert "boom message" in (tmp_path / "log.txt").read_text()


def test_stop_grace_then_terminate(tmp_path):
    called = []
    code = "import time; print('up', flush=True); time.sleep(60)"

    async def shutdown():
        called.append(1)

    async def main():
        la = LocalLauncher(_py(code), session_token="t", log_path=tmp_path / "log.txt")
        await la.start()
        await asyncio.sleep(0.5)
        t0 = time.monotonic()
        await la.stop(shutdown, grace_s=0.3)
        assert time.monotonic() - t0 < 10
        assert la.state == "stopped"
        assert la._proc is None or la._proc.returncode is not None

    asyncio.run(main())
    assert called == [1]


def test_closing_launcher_closes_child_stdin(tmp_path):
    marker = tmp_path / "eof.txt"
    code = f"import sys; sys.stdin.read(); open({str(marker)!r}, 'w').write('eof')"

    async def main():
        la = LocalLauncher(_py(code), session_token="t", log_path=tmp_path / "log.txt")
        await la.start()
        await asyncio.sleep(0.5)
        assert not marker.exists()  # stdin stays open while running
        await la.stop(None, grace_s=10.0)

    asyncio.run(main())
    assert marker.read_text() == "eof"


async def _noop_sleep(d):
    await asyncio.sleep(0)


def test_raising_on_exit_callback_does_not_kill_supervisor(tmp_path):
    seen = []

    def bad(rc):
        seen.append(rc)
        raise RuntimeError("cb boom")

    async def main():
        la = LocalLauncher(
            _py("import sys; sys.exit(2)"),
            session_token="t",
            log_path=tmp_path / "log.txt",
            clock=lambda: 0.0,
            sleep=_noop_sleep,
        )
        la.on_exit(bad)
        await la.start()
        assert await _wait_for(lambda: la.state == "gave_up")
        await la.stop(None)

    asyncio.run(main())
    assert seen == [2] * 5


def test_restart_spawn_failure_counts_and_gives_up(tmp_path):
    async def main():
        la = LocalLauncher(
            _py("import sys; sys.exit(1)"),
            session_token="t",
            log_path=tmp_path / "log.txt",
            clock=lambda: 0.0,
            sleep=_noop_sleep,
        )
        await la.start()
        la._command = [str(tmp_path / "no-such-executable")]
        assert await _wait_for(lambda: la.state == "gave_up")
        assert any("spawn failed" in line for line in la.last_stderr)
        await la.stop(None)

    asyncio.run(main())


def test_stop_during_backoff_is_prompt(tmp_path):
    async def long_sleep(d):
        await asyncio.sleep(30)

    async def main():
        la = LocalLauncher(
            _py("import sys; sys.exit(1)"),
            session_token="t",
            log_path=tmp_path / "log.txt",
            sleep=long_sleep,
        )
        await la.start()
        assert await _wait_for(lambda: la.state == "backoff")
        t0 = time.monotonic()
        await la.stop(None)
        assert time.monotonic() - t0 < 2.0
        assert la.state == "stopped"

    asyncio.run(main())


def test_stop_during_restart_spawn_leaves_no_orphan(tmp_path, monkeypatch):
    real = asyncio.create_subprocess_exec
    procs = []
    in_spawn = asyncio.Event
    state = {}

    async def slow_exec(*a, **kw):
        if procs:  # restart spawn, not the first
            state["ev"].set()
            await asyncio.sleep(0.5)
        p = await real(*a, **kw)
        procs.append(p)
        return p

    monkeypatch.setattr(asyncio, "create_subprocess_exec", slow_exec)

    async def main():
        state["ev"] = in_spawn()
        la = LocalLauncher(
            _py("import sys; sys.exit(1)"),
            session_token="t",
            log_path=tmp_path / "log.txt",
            clock=lambda: 0.0,
            sleep=_noop_sleep,
        )
        await la.start()
        await asyncio.wait_for(state["ev"].wait(), 10)
        await la.stop(None, grace_s=1.0)
        await asyncio.sleep(0.2)
        assert la.state == "stopped"

    asyncio.run(main())
    assert len(procs) >= 2
    assert all(p.returncode is not None for p in procs)


def test_exits_outside_window_do_not_count(tmp_path):
    ticks = iter(range(0, 10_000, 30))  # 30 s apart; window 60 -> at most 2 in window
    exits = []

    async def main():
        la = LocalLauncher(
            _py("import sys; sys.exit(1)"),
            session_token="t",
            log_path=tmp_path / "log.txt",
            clock=lambda: next(ticks),
            sleep=_noop_sleep,
        )
        la.on_exit(exits.append)
        await la.start()
        assert await _wait_for(lambda: len(exits) >= 7)
        assert la.state != "gave_up"
        await la.stop(None)

    asyncio.run(main())


def test_restart_child_kills_and_respawns(tmp_path):
    spawns: list[int] = []
    exits: list[int] = []

    async def main():
        la = LocalLauncher(
            _py("import sys; sys.stdin.read()"),  # alive until stdin closes (a hung child)
            session_token="t",
            log_path=tmp_path / "log.txt",
            sleep=_noop_sleep,
        )
        la.on_spawn(lambda: spawns.append(1))
        la.on_exit(exits.append)
        await la.start()
        first = la._proc
        assert await la.restart_child("heartbeats stopped")
        assert await _wait_for(lambda: len(spawns) == 2 and la.state == "running")
        second = la._proc
        assert second is not first and first.returncode is not None
        assert any("heartbeats stopped" in line for line in la.last_stderr)
        await la.stop(None)
        assert not await la.restart_child()  # nothing running

    asyncio.run(main())
    assert len(exits) == 1
