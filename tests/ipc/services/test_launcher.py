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
        assert la.state == "gave_up"

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
