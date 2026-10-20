"""Qt adapter for service clients -- the only module in ``ipc.services`` that imports PySide6.

:class:`QtServiceClient` runs a dedicated asyncio loop on a daemon thread named
``svc-<name>`` that owns one :class:`NatsCore` and one :class:`ServiceClient` (and, for a
local service, drives its :class:`LocalLauncher`).  Everything the client reports is
re-emitted as Qt signals on the main thread via
:func:`lightfall_utils.threads.invoke_in_main_thread`:

- ``state_changed(str)``: the client's health (``unknown``, ``starting``, ``up``,
  ``degraded``, ``down``, ``incompatible``, ``unauthorized``).  ``health`` and
  ``last_error`` are main-thread mirrors, updated just before the signal fires.  While
  health stays ``down``, the signal is re-emitted (same value) whenever ``last_error``
  changes, so a UI can refresh the reason.
- ``restarted()``: the service came back as a new instance and the session was redone.
- ``event(str, dict)``: a per-session event.  Only names registered with
  :meth:`QtServiceClient.watch_event` are delivered (before or after :meth:`start`).
- ``stream_chunk(str, int, object, bool)``: a service-to-client stream chunk
  ``(stream_id, seq, data: bytes, final)``.

``scope`` is the *resolved* scope (e.g. ``descriptor.resolved_scope(prefix, hostname,
client_instance)``); ``descriptor.scope`` is only the template and is not used for subjects.

The broker connect is retried until :meth:`stop` with a 1, 2, 5, 10 s backoff, reporting
``down`` (with the error in ``last_error``) between attempts; a broker that comes up late is
picked up without recreating the adapter.

Commands (:meth:`call`, :meth:`open_stream`, :meth:`send_chunk`, :meth:`close_stream`)
are thread-safe and return :class:`concurrent.futures.Future` objects resolved on the loop
thread; never block the GUI thread on them without a timeout.

Local services: pass ``launcher`` either as a factory ``Callable[[str], LocalLauncher]``,
which receives the session token the adapter mints with ``capability.new_token()`` (or the
``session_token`` given), or as a ready :class:`LocalLauncher`, whose token is then used
(an explicit, different ``session_token`` is an error).  :meth:`start` starts the launcher
and then the client with that same token; :meth:`stop` runs
``launcher.stop(shutdown=<rpc.shutdown>)``.  If the launcher gives up (too many crashes),
health goes ``down`` and ``last_error`` carries the last stderr lines.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import logging
import ssl
import threading
from collections.abc import Callable, Coroutine
from typing import Any

from PySide6.QtCore import QObject, Signal

from lightfall_utils.ipc.capability import new_token
from lightfall_utils.ipc.services import protocol as P
from lightfall_utils.ipc.services.client import ServiceClient
from lightfall_utils.ipc.services.core import NatsCore
from lightfall_utils.ipc.services.descriptor import ServiceDescriptor
from lightfall_utils.ipc.services.launcher import LocalLauncher
from lightfall_utils.threads import initialize_main_thread_invoker, invoke_in_main_thread

log = logging.getLogger(__name__)

CONNECT_TIMEOUT_S = 10.0
STOP_TIMEOUT_S = 15.0
# Backoff (s) between broker connect attempts; retried until stop().
_CONNECT_BACKOFF_S = (1.0, 2.0, 5.0, 10.0)
_STDERR_TAIL = 5

LauncherArg = LocalLauncher | Callable[[str], LocalLauncher] | None


class QtServiceClient(QObject):
    state_changed = Signal(str)
    restarted = Signal()
    event = Signal(str, dict)
    stream_chunk = Signal(str, int, object, bool)

    def __init__(
        self,
        descriptor: ServiceDescriptor,
        url: str,
        *,
        scope: str,
        app_name: str,
        app_version: str,
        launcher: LauncherArg = None,
        session_token: str | None = None,
        user: str | None = None,
        tls: ssl.SSLContext | None = None,
        parent: QObject | None = None,
    ) -> None:
        super().__init__(parent)
        self.descriptor = descriptor
        self.url = url
        self.scope = scope
        self.app_name = app_name
        self.app_version = app_version
        self.user = user
        self._tls = tls

        if isinstance(launcher, LocalLauncher):
            tok = launcher.session_token  # the launcher was built with its token; reuse it
            if session_token is not None and session_token != tok:
                raise ValueError("session_token differs from the launcher's session token")
            session_token = tok
        elif launcher is not None:
            if session_token is None:
                session_token = new_token()
            launcher = launcher(session_token)
            if not isinstance(launcher, LocalLauncher):
                raise TypeError("launcher factory must return a LocalLauncher")
        self._launcher: LocalLauncher | None = launcher
        self._token = session_token

        # main-thread mirrors
        self._health = "unknown"
        self._last_error = ""

        self._event_names: set[str] = set()  # main thread
        self._registered: set[str] = set()  # loop thread
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready: asyncio.Event | None = None
        self._start_task: asyncio.Task | None = None
        self._core: NatsCore | None = None
        self._client: ServiceClient | None = None
        self._gave_up_error: str | None = None
        self._stopped = False

    # -- properties ---------------------------------------------------------------------

    @property
    def health(self) -> str:
        return self._health

    @property
    def last_error(self) -> str:
        return self._last_error

    @property
    def session_token(self) -> str | None:
        return self._token

    @property
    def launcher(self) -> LocalLauncher | None:
        return self._launcher

    @property
    def info(self) -> dict | None:
        c = self._client
        return c.info if c is not None else None

    # -- main-thread emitters (guarded: the QObject may be gone by the time they run) ---

    def _apply_state(self, health: str, error: str | None) -> None:
        old_error = self._last_error
        if error is not None:
            self._last_error = error
        if health == self._health and not (health == "down" and self._last_error != old_error):
            return
        self._health = health
        with contextlib.suppress(RuntimeError):
            self.state_changed.emit(health)

    def _emit_restarted(self) -> None:
        with contextlib.suppress(RuntimeError):
            self.restarted.emit()

    def _emit_event(self, name: str, payload: dict) -> None:
        with contextlib.suppress(RuntimeError):
            self.event.emit(name, payload)

    def _emit_chunk(self, sid: str, seq: int, data: bytes, final: bool) -> None:
        with contextlib.suppress(RuntimeError):
            self.stream_chunk.emit(sid, seq, data, final)

    # -- loop-thread callbacks ----------------------------------------------------------

    def _report(self, health: str, error: str | None) -> None:
        invoke_in_main_thread(self._apply_state, health, error)

    def _on_client_state(self, health: str) -> None:
        c = self._client
        error = c.last_error if c is not None else None
        if self._gave_up_error is not None and health == "down":
            error = self._gave_up_error
        self._report(health, error)

    def _on_child_exit(self, rc: int) -> None:
        # The launcher sets ``state`` right after its exit callbacks, before awaiting.
        asyncio.get_running_loop().call_soon(self._check_gave_up, rc)

    def _check_gave_up(self, rc: int) -> None:
        la = self._launcher
        if la is None or la.state != "gave_up":
            return
        tail = " | ".join(la.last_stderr[-_STDERR_TAIL:])
        self._gave_up_error = f"service gave up after repeated exits (last rc={rc}): {tail}"
        self._report("down", self._gave_up_error)

    def _register_event(self, name: str) -> None:
        c = self._client
        if c is None or name in self._registered:
            return
        self._registered.add(name)
        c.on_event(name, lambda ev, n=name: invoke_in_main_thread(self._emit_event, n, ev))

    # -- lifecycle ----------------------------------------------------------------------

    def watch_event(self, name: str) -> None:
        """Deliver the per-session event ``name`` through the ``event`` signal."""
        if name in self._event_names:
            return
        self._event_names.add(name)
        loop = self._loop
        if loop is not None and not loop.is_closed():
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(self._register_event, name)

    def start(self) -> concurrent.futures.Future:
        """Start the loop thread, the launcher (local services) and the client.

        Must be called from the main thread.  Returns a Future that resolves once the
        start sequence has run; failures are reported through ``state_changed``.
        """
        if self._thread is not None:
            raise RuntimeError("QtServiceClient already started")
        if self._stopped:
            raise RuntimeError("QtServiceClient was stopped; create a new one")
        initialize_main_thread_invoker()
        loop = asyncio.new_event_loop()
        self._loop = loop
        self._ready = asyncio.Event()
        self._thread = threading.Thread(
            target=self._run_loop, args=(loop,), name=f"svc-{self.descriptor.name}", daemon=True
        )
        self._thread.start()
        return asyncio.run_coroutine_threadsafe(self._astart(), loop)

    @staticmethod
    def _run_loop(loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        try:
            loop.run_forever()
        finally:
            try:
                pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
                for t in pending:
                    t.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                loop.close()

    async def _astart(self) -> None:
        self._start_task = asyncio.current_task()
        assert self._ready is not None
        try:
            core = NatsCore(self.url, tls=self._tls, name=f"{self.app_name}:{self.descriptor.name}")
            client = ServiceClient(
                self.descriptor,
                core,
                scope=self.scope,
                app_name=self.app_name,
                app_version=self.app_version,
                user=self.user,
                session_token=self._token,
            )
            client.on_state(self._on_client_state)
            client.on_restarted(lambda: invoke_in_main_thread(self._emit_restarted))
            client.on_stream(
                lambda sid, seq, data, final: invoke_in_main_thread(
                    self._emit_chunk, sid, seq, data, final
                )
            )
            self._core = core
            self._client = client
            for name in list(self._event_names):
                self._register_event(name)
            if self._launcher is not None:
                self._launcher.on_exit(self._on_child_exit)
                self._report("starting", None)
                await self._launcher.start()
            attempt = 0
            while True:
                if self._stopped:
                    return
                try:
                    await asyncio.wait_for(core.connect(), CONNECT_TIMEOUT_S)
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._report("down", f"cannot connect to {self.url}: {e!r}")
                # Queued calls resolve now (with the "no session" error dict).
                self._ready.set()
                await asyncio.sleep(_CONNECT_BACKOFF_S[min(attempt, len(_CONNECT_BACKOFF_S) - 1)])
                attempt += 1
            await client.start()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("QtServiceClient start failed")
            self._report("down", f"start failed: {e!r}")
        finally:
            self._ready.set()

    async def _astop(self) -> None:
        task = self._start_task
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        client, core, launcher = self._client, self._core, self._launcher

        async def shutdown() -> None:
            if client is not None and client.connected:
                await client.call("shutdown", {}, P.DEFAULT_TIMEOUT_S)

        if launcher is not None:
            try:
                await launcher.stop(shutdown=shutdown, grace_s=3.0)
            except Exception:
                log.exception("launcher stop failed")
        if client is not None:
            try:
                await client.close()
            except Exception:
                log.exception("client close failed")
        if core is not None:
            with contextlib.suppress(Exception):
                await core.close()
        self._client = None

    def stop(self, timeout: float = STOP_TIMEOUT_S) -> None:
        """Shut down the service (local), close the client and join the loop thread.

        Blocks for up to ``timeout`` seconds (plus a short join).  Idempotent.
        """
        self._stopped = True
        thread, loop = self._thread, self._loop
        if thread is None or loop is None:
            return
        try:
            fut = asyncio.run_coroutine_threadsafe(self._astop(), loop)
            fut.result(timeout)
        except Exception:
            log.exception("QtServiceClient stop failed")
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(loop.stop)
        thread.join(5.0)
        self._thread = None
        self._loop = None

    # -- commands -----------------------------------------------------------------------

    def _submit(
        self,
        fn: Callable[[ServiceClient], Coroutine[Any, Any, Any]],
        wait_s: float = P.DEFAULT_TIMEOUT_S,
        on_wait_timeout: Callable[[], Any] | None = None,
    ) -> concurrent.futures.Future:
        loop = self._loop
        if self._stopped or loop is None or loop.is_closed():
            f: concurrent.futures.Future = concurrent.futures.Future()
            f.set_exception(RuntimeError("QtServiceClient is not running"))
            return f

        async def run() -> Any:
            assert self._ready is not None
            try:
                await asyncio.wait_for(self._ready.wait(), wait_s)
            except TimeoutError:
                if on_wait_timeout is None:
                    raise
                return on_wait_timeout()
            c = self._client
            if c is None:
                raise RuntimeError("service client is not connected")
            return await fn(c)

        return asyncio.run_coroutine_threadsafe(run(), loop)

    def call(
        self, action: str, payload: dict, timeout: float = P.DEFAULT_TIMEOUT_S
    ) -> concurrent.futures.Future:
        """``rpc.<action>``; resolves to the reply dict (``svc_error`` dict on failure)."""
        return self._submit(
            lambda c: c.call(action, payload, timeout),
            wait_s=timeout,
            on_wait_timeout=lambda: P.svc_error(
                self.descriptor.contract, "timeout", f"{action}: client still starting"
            ),
        )

    def open_stream(self, kind: str, params: dict | None = None) -> concurrent.futures.Future:
        """Resolves to the new ``stream_id``; raises ``ServiceCallError`` if refused."""
        return self._submit(lambda c: c.open_stream(kind, params))

    def send_chunk(
        self, stream_id: str, seq: int, data: bytes, final: bool = False
    ) -> concurrent.futures.Future:
        return self._submit(lambda c: c.send_chunk(stream_id, seq, data, final))

    def close_stream(self, stream_id: str) -> concurrent.futures.Future:
        return self._submit(lambda c: c.close_stream(stream_id))
