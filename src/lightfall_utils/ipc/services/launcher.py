"""Local service launcher: spawn a service child, hand it a session token, restart on crash.

The token is written as one line to the child's stdin and stdin is then kept open; the
child (see ``cli.run_service --token-stdin``) exits when stdin reaches EOF, so closing it is
a clean stop signal. Qt-free.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Literal

State = Literal["running", "backoff", "gave_up", "stopped"]
_TAIL = 20
_LINE_LIMIT = 1 << 20
log = logging.getLogger(__name__)


class LocalLauncher:
    def __init__(
        self,
        command: Sequence[str],
        *,
        session_token: str,
        log_path: Path,
        backoff: Sequence[float] = (1, 2, 5, 10),
        give_up_after: int = 5,
        window_s: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._command = list(command)
        self._token = session_token
        self._log_path = Path(log_path)
        self._backoff = tuple(backoff)
        self._give_up_after = give_up_after
        self._window_s = window_s
        self._clock = clock
        self._sleep = sleep
        self.state: State = "stopped"
        self._stderr: deque[str] = deque(maxlen=_TAIL)
        self._callbacks: list[Callable[[int], None]] = []
        self._exits: list[float] = []
        self._proc: asyncio.subprocess.Process | None = None
        self._task: asyncio.Task | None = None
        self._stopping = False
        self._spawning = False
        self._log = None

    @property
    def last_stderr(self) -> list[str]:
        return list(self._stderr)

    def on_exit(self, cb: Callable[[int], None]) -> None:
        self._callbacks.append(cb)

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._exits.clear()
        self._stderr.clear()
        self._log_path.parent.mkdir(parents=True, exist_ok=True)
        self._log = open(self._log_path, "ab")  # noqa: SIM115 - closed in _supervise/stop
        try:
            spawned = await self._spawn()
        except BaseException:
            self._close_log()
            raise
        if not spawned:
            self._close_log()
            return
        self._task = asyncio.create_task(self._supervise())

    def _close_log(self) -> None:
        if self._log is not None and not self._log.closed:
            self._log.close()

    async def _spawn(self) -> bool:
        """Spawn the child; False if a stop() arrived meanwhile (child is torn down)."""
        self._spawning = True
        try:
            proc = await asyncio.create_subprocess_exec(
                *self._command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                limit=_LINE_LIMIT,
            )
        finally:
            self._spawning = False
        self._proc = proc
        if self._stopping:
            await self._kill(proc)
            return False
        self.state = "running"
        assert proc.stdin is not None
        proc.stdin.write(self._token.encode() + b"\n")
        with contextlib.suppress(ConnectionError, OSError):
            await proc.stdin.drain()
        return True

    @staticmethod
    async def _kill(proc: asyncio.subprocess.Process) -> None:
        if proc.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            await proc.wait()

    async def _pump(self, stream: asyncio.StreamReader | None, is_err: bool) -> None:
        if stream is None:
            return
        while True:
            try:
                raw = await stream.readline()
            except ValueError:  # over-long line; the oversized chunk is discarded
                continue
            if not raw:
                return
            if self._log is not None and not self._log.closed:
                self._log.write(raw)
                self._log.flush()
            if is_err:
                self._stderr.append(raw.decode("utf-8", "replace").rstrip("\r\n"))

    async def _run_once(self, proc: asyncio.subprocess.Process) -> int:
        _, _, rc = await asyncio.gather(
            self._pump(proc.stdout, False), self._pump(proc.stderr, True), proc.wait()
        )
        return rc

    async def _supervise(self) -> None:
        try:
            assert self._proc is not None
            rc = await self._run_once(self._proc)
            while True:
                if self._stopping:
                    return
                now = self._clock()
                self._exits = [t for t in self._exits if now - t < self._window_s]
                self._exits.append(now)
                for cb in list(self._callbacks):
                    try:
                        cb(rc)
                    except Exception:
                        log.exception("on_exit callback failed")
                n = len(self._exits)
                if n >= self._give_up_after:
                    self.state = "gave_up"
                    return
                self.state = "backoff"
                await self._sleep(self._backoff[min(n, len(self._backoff)) - 1])
                if self._stopping:
                    return
                try:
                    if not await self._spawn():
                        return
                except Exception as e:
                    self._stderr.append(f"spawn failed: {e}")
                    rc = -1
                    continue
                assert self._proc is not None
                rc = await self._run_once(self._proc)
        finally:
            self._close_log()

    async def stop(
        self, shutdown: Callable[[], Awaitable[None]] | None = None, grace_s: float = 3.0
    ) -> None:
        self._stopping = True
        proc = self._proc
        task = self._task
        if self._spawning and task is not None and not task.done():
            # a restart is mid-spawn; let it finish (it tears the new child down itself)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(asyncio.shield(task), 10.0)
            proc = self._proc
        if shutdown is not None and proc is not None and proc.returncode is None:
            with contextlib.suppress(Exception):
                await shutdown()
        if proc is not None and proc.returncode is None:
            if proc.stdin is not None:
                with contextlib.suppress(ConnectionError, OSError):
                    proc.stdin.close()
            try:
                await asyncio.wait_for(proc.wait(), grace_s)
            except TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), grace_s)
                except TimeoutError:
                    await self._kill(proc)
        if task is not None:
            if not task.done():
                # child is dead or the supervisor is sleeping in backoff: stop it now
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            self._task = None
        if self._proc is not None:
            await self._kill(self._proc)
        self._close_log()
        self.state = "stopped"
