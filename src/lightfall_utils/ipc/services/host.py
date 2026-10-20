"""Qt-free service side of the services pattern: discovery, heartbeat, handshake, sessions,
rpc, streams and per-session delivery.

A :class:`ServiceHost` runs on one :class:`NatsCore`.  It answers ``{root}.meta.info`` and
``{scope}.svc.discover`` with its info payload, publishes the same payload on
``{root}.heartbeat`` every ``heartbeat_s``, serves the ``{priv_root}.auth.request`` handshake
and dispatches ``{priv_root}.session.{token}.rpc.<action>`` requests to registered handlers.
Sessions expire after ``LEASE_S`` without a ``rpc.session.ping``.

Built-in session actions: ``session.ping``, ``session.close``, ``shutdown`` (reply, then
stop), ``stream.open {kind, params}`` -> ``{stream_id}`` and ``stream.close {stream_id}``.
Client-to-service chunks arrive on ``{session_subject}.stream.{stream_id}`` and reach the
handler registered with :meth:`ServiceHost.stream`.

Concurrency: nothing user-supplied runs inline on the shared session subscription.  rpc
handlers run as tasks; each stream has its own ordered queue drained by its own task, so a
slow stream handler (e.g. a transcription on the final chunk) delays only later chunks of
*that* stream, never another session's ``session.ping`` or rpc.  rpc requests and chunks
share one subscription, so a ``stream.close`` is queued after every chunk published before it
on the same connection; its reply is sent as soon as the final call is queued.

Stream lifecycle hooks for services whose own rpcs manage streams (e.g. ``utterance.start``):
:meth:`ServiceHost.open_stream` opens a stream from service code and returns its id,
:meth:`ServiceHost.end_stream` finalizes or drops it, ``stream(kind, on_open=...)`` may refuse
a client ``stream.open`` by raising :class:`ServiceError` (e.g. ``busy``), and
:meth:`ServiceHost.on_stream_end` is told once per stream how it ended (``final``, ``close``,
``cancel``, ``idle``, ``session_end`` or ``stop``).  rpc handlers may raise
:class:`ServiceError` too.

Public broadcasts go to ``{root}.events.<name>`` with :meth:`ServiceHost.publish_event`.
Anyone on the broker can read or forge them; never put session data there.

Startup: start the host first (so clients see a heartbeat and report ``starting``/``up``
rather than ``down``), then load heavy resources in the background and report progress with
:meth:`ServiceHost.set_state` (``summary``).  Pass ``auto_ready=False`` to keep ``state`` at
``starting`` after :func:`~lightfall_utils.ipc.services.cli.run_service` starts the host.
"""

from __future__ import annotations

import asyncio
import functools
import hmac
import inspect
import json
import logging
import math
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from lightfall_utils.ipc.capability import new_token
from lightfall_utils.ipc.services import protocol as P
from lightfall_utils.ipc.services.core import NatsCore

log = logging.getLogger(__name__)

Handler = Callable[["Session", dict], "dict | Awaitable[dict]"]
StreamHandler = Callable[["Session", str, int, bytes, bool], Any]
OpenValidator = Callable[["Session", dict], Any]
StreamEndCallback = Callable[["Session", str, str], Any]

_PING = "session.ping"
_CLOSE = "session.close"
_SHUTDOWN = "shutdown"
_STREAM_OPEN = "stream.open"
_STREAM_CLOSE = "stream.close"
_BUILTIN = frozenset({_PING, _CLOSE, _SHUTDOWN, _STREAM_OPEN, _STREAM_CLOSE})
_RESERVED_REPLY_KEYS = frozenset({"contract", "status", "contract_version"})
_SWEEP_MAX_S = 1.0
# On stop, streams already finalizing (final chunk / stream.close queued) get this long to
# finish their handler before they are cancelled.
_STOP_DRAIN_S = 2.0

END_FINAL = "final"  # a chunk with Svc-Final "1"
END_CLOSE = "close"  # rpc.stream.close, or end_stream(final=True)
END_CANCEL = "cancel"  # end_stream() from service code, without a final call
END_IDLE = "idle"  # no chunk for STREAM_IDLE_S
END_SESSION = "session_end"  # the session closed or its lease expired
END_STOP = "stop"  # the host stopped


class InstanceConflictError(RuntimeError):
    """Another live instance of a single-instance service already serves this root."""


class ServiceError(Exception):
    """Raise from an rpc handler or a stream ``on_open`` validator to send a structured error
    reply with ``code`` (one of :data:`protocol.ERROR_CODES`, e.g. ``busy``)."""

    def __init__(self, code: str, message: str = "") -> None:
        if code not in P.ERROR_CODES:
            raise ValueError(f"Unknown error code: {code!r}")
        super().__init__(message or code)
        self.code = code
        self.message = message or code


@dataclass
class Session:
    token: str
    app_name: str
    user: str | None
    delivery_root: str
    last_ping: float
    event_seq: int = 0


@dataclass
class StreamState:
    stream_id: str
    kind: str
    token: str
    params: dict
    next_seq: int
    last_chunk: float
    # Ordered (seq, data, final, end_reason) items for this stream, drained by ``task``.
    queue: asyncio.Queue | None = field(default=None, repr=False, compare=False)
    task: asyncio.Task | None = field(default=None, repr=False, compare=False)
    ended: bool = field(default=False, repr=False, compare=False)
    drop_reason: str | None = field(default=None, repr=False, compare=False)


def allow_apps(names: Iterable[str]) -> Callable[[dict], bool]:
    """A ``trust`` callable approving only ``auth.request`` payloads whose app_name is listed."""
    allowed = frozenset(names)
    return lambda req: req.get("app_name") in allowed


def _valid_subject_token(tok: Any) -> bool:
    return isinstance(tok, str) and bool(tok) and not any(c in tok for c in ".*> \t\r\n")


def _check_stdin_token(tok: Any) -> None:
    if not _valid_subject_token(tok):
        raise ValueError("stdin_token must be a single non-empty subject token")


def _check_event_name(name: Any, *, allow_stream: bool) -> None:
    if not isinstance(name, str) or not all(_valid_subject_token(t) for t in name.split(".")):
        raise ValueError(f"invalid event name {name!r}")
    if not allow_stream and name.split(".", 1)[0] == "stream":
        raise ValueError(f"event name {name!r} may not start with 'stream' (reserved for chunks)")


async def _maybe_await(r: Any) -> Any:
    if inspect.isawaitable(r):
        return await r
    return r


class ServiceHost:
    def __init__(
        self,
        name: str,
        scope: str,
        *,
        contract: str,
        version: str,
        single_instance: bool = True,
        heartbeat_s: float = P.HEARTBEAT_S,
        trust: Callable[[dict], bool] = lambda req: True,
        stdin_token: str | None = None,
        clock: Callable[[], float] = time.monotonic,
        auto_ready: bool = True,
    ) -> None:
        if stdin_token is not None:
            _check_stdin_token(stdin_token)
        if (
            isinstance(heartbeat_s, bool)
            or not isinstance(heartbeat_s, (int, float))
            or not math.isfinite(heartbeat_s)
            or heartbeat_s <= 0
        ):
            raise ValueError(f"heartbeat_s must be a finite number > 0, got {heartbeat_s!r}")
        self.name = name
        self.scope = scope
        self.contract = contract
        self.version = version
        self.single_instance = single_instance
        self.heartbeat_s = heartbeat_s
        self.auto_ready = auto_ready
        self._trust = trust
        self._stdin_token = stdin_token
        self._clock = clock
        self._instance_id = uuid.uuid4().hex
        self._root = P.root(scope, name)
        self.state = "starting"
        self.summary: dict = {}
        self._handlers: dict[str, Handler] = {}
        self._stream_handlers: dict[str, StreamHandler] = {}
        self._open_validators: dict[str, OpenValidator] = {}
        self._streams: dict[str, StreamState] = {}
        self._finishing: set[asyncio.Task] = set()  # workers of streams with a final queued
        self._public_seq = 0
        self._end_cbs: list[Callable[[Session], Any]] = []
        self._stream_end_cbs: list[StreamEndCallback] = []
        self._beat_now: asyncio.Event | None = None
        self._sessions: dict[str, Session] = {}
        self._core: NatsCore | None = None
        self._subs: list[Any] = []
        self._tasks: list[asyncio.Task] = []
        self._rpc_tasks: set[asyncio.Task] = set()
        self._stopped: asyncio.Event | None = None
        self._stop_task: asyncio.Task | None = None

    # -- properties ---------------------------------------------------------------------

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def root(self) -> str:
        return self._root

    @property
    def priv_root(self) -> str:
        return self._conn().priv_root

    @property
    def sessions(self) -> dict[str, Session]:
        return dict(self._sessions)

    @property
    def streams(self) -> dict[str, StreamState]:
        return dict(self._streams)

    def configure(self, *, scope: str | None = None, stdin_token: str | None = None) -> None:
        """Override the scope and/or the stdin token before :meth:`start` (used by the CLI)."""
        if self._core is not None:
            raise RuntimeError("ServiceHost already started")
        if stdin_token is not None:
            _check_stdin_token(stdin_token)
            self._stdin_token = stdin_token
        if scope is not None:
            self.scope = scope
            self._root = P.root(scope, self.name)

    # -- registration -------------------------------------------------------------------

    def rpc(self, action: str) -> Callable[[Handler], Handler]:
        if action in _BUILTIN:
            raise ValueError(f"{action!r} is reserved")

        def deco(fn: Handler) -> Handler:
            self._handlers[action] = fn
            return fn

        return deco

    def stream(
        self, kind: str, *, on_open: OpenValidator | None = None
    ) -> Callable[[StreamHandler], StreamHandler]:
        """Register ``handler(session, stream_id, seq, data, final)`` for stream ``kind``.

        Calls for one stream are made in order, one at a time, on that stream's own task
        (sync handlers run in the default executor).  The handler sees ``final=True``
        exactly once per stream that ends normally: on a chunk whose ``Svc-Final`` is
        ``"1"``, or on ``rpc.stream.close`` / :meth:`end_stream` ``(final=True)`` (then with
        ``data=b""`` and the next expected seq).  Streams dropped for idleness, session end,
        :meth:`end_stream` or host stop get no final call; :meth:`on_stream_end` reports
        every ending.

        ``on_open(session, params)`` (sync or async) runs for each client ``stream.open`` of
        this kind; raise :class:`ServiceError` (e.g. ``ServiceError("busy", ...)``) to refuse
        it.  :meth:`open_stream` from service code does not call it.
        """
        if not _valid_subject_token(kind):
            raise ValueError(f"invalid stream kind {kind!r}")

        def deco(fn: StreamHandler) -> StreamHandler:
            self._stream_handlers[kind] = fn
            if on_open is not None:
                self._open_validators[kind] = on_open
            else:
                self._open_validators.pop(kind, None)
            return fn

        return deco

    def on_session_end(self, cb: Callable[[Session], Any]) -> None:
        self._end_cbs.append(cb)

    def on_stream_end(self, cb: StreamEndCallback) -> None:
        """Register ``cb(session, stream_id, reason)`` (sync or async), called once per stream
        after its last handler call.  ``reason`` is one of ``final``, ``close``, ``cancel``,
        ``idle``, ``session_end`` or ``stop``."""
        self._stream_end_cbs.append(cb)

    def set_state(self, state: str | None = None, summary: dict | None = None) -> None:
        """Update ``state`` and/or replace ``summary``, and publish a heartbeat right away.

        Call on the host's loop thread (from another thread use
        ``loop.call_soon_threadsafe(host.set_state, ...)``).
        """
        if state is not None:
            self.state = state
        if summary is not None:
            self.summary = summary
        if self._beat_now is not None:
            self._beat_now.set()

    # -- lifecycle ----------------------------------------------------------------------

    def info(self) -> dict:
        return {
            "instance_id": self._instance_id,
            "name": self.name,
            "state": self.state,
            "version": self.version,
            "svc_protocol": P.SVC_PROTOCOL,
            "contract": self.contract,
            "priv_root": self.priv_root,
            "single_instance": self.single_instance,
            "summary": self.summary,
            "ts": time.time(),
        }

    async def start(self, core: NatsCore) -> None:
        """Begin serving on ``core``.

        A single-instance host first listens on ``{root}.heartbeat`` for
        ``1.5 * heartbeat_s`` and raises :class:`InstanceConflictError` if a beat from
        another live instance arrives.  This is best effort: two hosts starting within one
        heartbeat interval can race.  The check is skipped for a locally launched host
        (``stdin_token`` set): its scope contains the launching client's instance id, so no
        other copy can share it.
        """
        if self._core is not None:
            raise RuntimeError("ServiceHost already started")
        if self.single_instance and self._stdin_token is None:
            await self._check_single_instance(core)
        self._core = core
        self._stopped = asyncio.Event()
        self._beat_now = asyncio.Event()
        pr = core.priv_root
        self._subs = [
            await core.subscribe(P.meta_info(self._root), self._on_info),
            await core.subscribe(P.discover(self.scope), self._on_info),
            await core.subscribe(P.auth_subject(pr), self._on_auth),
            await core.subscribe(f"{P.session_subject(pr, '*')}.>", self._on_session),
        ]
        await core.flush()
        self._tasks = [
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._sweep_loop()),
        ]

    async def _check_single_instance(self, core: NatsCore) -> None:
        seen: asyncio.Future = asyncio.get_running_loop().create_future()

        async def on_beat(msg: Any) -> None:
            try:
                beat = json.loads(msg.data)
            except (ValueError, UnicodeDecodeError):
                return
            if (
                isinstance(beat, dict)
                and beat.get("instance_id") not in (None, self._instance_id)
                and beat.get("state") != "stopping"
                and not seen.done()
            ):
                seen.set_result(beat["instance_id"])

        sub = await core.subscribe(P.heartbeat(self._root), on_beat)
        try:
            await core.flush()
            await asyncio.wait_for(asyncio.shield(seen), 1.5 * self.heartbeat_s)
        except TimeoutError:
            pass
        finally:
            try:
                await sub.unsubscribe()
            except Exception:
                log.debug("unsubscribe failed", exc_info=True)
        if seen.done():
            raise InstanceConflictError(f"another instance is already serving {self._root}")

    def request_stop(self) -> None:
        """Schedule :meth:`stop` on the running loop.  Idempotent; call on the loop thread."""
        if self._stop_task is None and self._core is not None:
            self._stop_task = asyncio.get_running_loop().create_task(self.stop())

    async def stop(self) -> None:
        core = self._core
        if core is None or self._stopped is None or self._stopped.is_set():
            return
        self.state = "stopping"
        for t in [*self._tasks, *self._rpc_tasks]:
            t.cancel()
        await asyncio.gather(*self._tasks, *self._rpc_tasks, return_exceptions=True)
        self._tasks.clear()
        self._rpc_tasks.clear()
        for sid in list(self._streams):
            await self._drop_stream(sid, END_STOP)
        finishing = list(self._finishing)
        if finishing:
            _, pending = await asyncio.wait(finishing, timeout=_STOP_DRAIN_S)
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        self._finishing.clear()
        try:
            await core.publish_json(P.heartbeat(self._root), self.info())
        except Exception:
            log.debug("final heartbeat failed", exc_info=True)
        for sub in self._subs:
            try:
                await sub.unsubscribe()
            except Exception:
                log.debug("unsubscribe failed", exc_info=True)
        self._subs.clear()
        for tok in list(self._sessions):
            await self._end_session(tok)
        self._streams.clear()
        self._stopped.set()

    async def serve_forever(self) -> None:
        if self._stopped is None:
            raise RuntimeError("ServiceHost not started")
        await self._stopped.wait()

    # -- background loops ---------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        subject = P.heartbeat(self._root)
        wake = self._beat_now
        assert wake is not None
        while True:
            try:
                await self._conn().publish_json(subject, self.info())
            except Exception:
                log.warning("heartbeat publish failed", exc_info=True)
            try:
                await asyncio.wait_for(wake.wait(), self.heartbeat_s)
            except TimeoutError:
                pass
            wake.clear()

    async def _sweep_loop(self) -> None:
        interval = min(_SWEEP_MAX_S, self.heartbeat_s)
        while True:
            await asyncio.sleep(interval)
            await self._sweep()

    async def _sweep(self) -> None:
        now = self._clock()
        for sid, st in list(self._streams.items()):
            if now - st.last_chunk > P.STREAM_IDLE_S:
                log.info("stream %s… (%s) idle, dropped", sid[:6], st.kind)
                await self._drop_stream(sid, END_IDLE)
        for tok, s in list(self._sessions.items()):
            if now - s.last_ping > P.LEASE_S:
                log.info("session %s… (%s) lease expired", tok[:6], s.app_name)
                await self._end_session(tok)

    async def _end_session(self, token: str) -> None:
        s = self._sessions.pop(token, None)
        if s is not None:
            await self._session_ended(s)

    def _end_session_soon(self, token: str) -> None:
        """Detach the session now; run its stream/session-end callbacks on a task."""
        s = self._sessions.pop(token, None)
        if s is not None:  # chunks for it are refused from now on (_live_session)
            self._track(asyncio.create_task(self._session_ended(s)))

    async def _session_ended(self, s: Session) -> None:
        for sid, st in list(self._streams.items()):
            if st.token == s.token:
                await self._drop_stream(sid, END_SESSION)
        for cb in list(self._end_cbs):
            try:
                await _maybe_await(cb(s))
            except Exception:
                log.exception("on_session_end callback failed")

    def _track(self, task: asyncio.Task) -> None:
        self._rpc_tasks.add(task)
        task.add_done_callback(self._rpc_tasks.discard)

    # -- message handlers ---------------------------------------------------------------

    def _conn(self) -> NatsCore:
        if self._core is None:
            raise RuntimeError("ServiceHost not started")
        return self._core

    async def _respond(self, msg: Any, obj: dict) -> None:
        if not msg.reply:
            return
        await self._conn().publish(msg.reply, json.dumps(obj).encode())

    async def _on_info(self, msg: Any) -> None:
        await self._respond(msg, self.info())

    def _deny(self, why: str) -> dict:
        return P.svc_error(self.contract, "denied", why)

    def _bad(self, why: str) -> dict:
        return P.svc_error(self.contract, "bad_request", why)

    async def _on_auth(self, msg: Any) -> None:
        try:
            payload = json.loads(msg.data)
        except (ValueError, UnicodeDecodeError):
            payload = None
        if not isinstance(payload, dict):
            await self._respond(msg, self._deny("malformed auth.request"))
            return
        cpr = payload.get("client_priv_root")
        try:
            client_p = P.prefix_of_priv_root(cpr)
        except ValueError:
            await self._respond(msg, self._deny("invalid client_priv_root"))
            return
        if client_p != P.prefix_of_inbox(msg.reply or ""):
            await self._respond(msg, self._deny("client_priv_root does not match reply inbox"))
            return
        if self._stdin_token is not None:
            given = payload.get("session_token")
            if not isinstance(given, str) or not hmac.compare_digest(
                given.encode(), self._stdin_token.encode()
            ):
                await self._respond(msg, self._deny("session token required"))
                return
        try:
            trusted = bool(self._trust(payload))
        except Exception:
            log.exception("trust callable failed")
            trusted = False
        if not trusted:
            await self._respond(msg, self._deny("not trusted"))
            return
        delivery_token = payload.get("delivery_token")
        if not _valid_subject_token(delivery_token):
            await self._respond(msg, self._bad("invalid delivery_token"))
            return
        user = payload.get("user")
        token = self._stdin_token or new_token()
        if token in self._sessions:
            await self._end_session(token)  # quick: a new session for this token follows
        self._sessions[token] = Session(
            token=token,
            app_name=str(payload.get("app_name", "")),
            user=user if isinstance(user, str) else None,
            delivery_root=P.delivery_root(cpr, delivery_token),
            last_ping=self._clock(),
        )
        await self._respond(
            msg,
            P.svc_reply(
                self.contract,
                status="approved",
                session_token=token,
                session_subject=P.session_subject(self.priv_root, token),
            ),
        )

    def _live_session(self, token: str) -> Session | None:
        s = self._sessions.get(token)
        if s is None:
            return None
        if self._clock() - s.last_ping > P.LEASE_S:
            return None  # expired; the sweeper ends it
        return s

    async def _on_session(self, msg: Any) -> None:
        head = P.session_subject(self.priv_root, "")
        token, _, rest = msg.subject[len(head) :].partition(".")
        what, _, tail = rest.partition(".")
        if what not in ("rpc", "stream") or not tail:
            return
        session = self._live_session(token)
        if session is None:
            if token in self._sessions:
                self._end_session_soon(token)
            return
        if what == "stream":
            self._on_chunk(session, tail, msg)
            return
        if tail == _PING:
            session.last_ping = self._clock()
            await self._respond(msg, P.svc_reply(self.contract))
            return
        if tail == _CLOSE:
            await self._respond(msg, P.svc_reply(self.contract))
            self._end_session_soon(token)
            return
        if tail == _SHUTDOWN:
            log.info("shutdown requested by %s", session.app_name)
            await self._respond(msg, P.svc_reply(self.contract))
            try:
                await self._conn().flush()
            except Exception:
                log.debug("flush before shutdown failed", exc_info=True)
            self.request_stop()
            return
        if tail in (_STREAM_OPEN, _STREAM_CLOSE):
            payload = _json_object(msg)
            if payload is None:
                await self._respond(msg, self._bad("payload must be a JSON object"))
            elif tail == _STREAM_OPEN:
                # The validator is service code: run it off the shared subscription.
                self._track(asyncio.create_task(self._stream_open(session, payload, msg)))
            else:
                # Inline, so the final call is queued behind every chunk received before it.
                await self._stream_close(session, payload, msg)
            return
        self._track(asyncio.create_task(self._dispatch(session, tail, msg)))

    # -- streams ------------------------------------------------------------------------

    def open_stream(self, session: Session, kind: str, params: dict | None = None) -> str:
        """Open a client-to-service stream for ``session`` from service code; returns its id.

        For services whose own rpc starts a stream (e.g. ``utterance.start`` replying
        ``{"stream_id": host.open_stream(session, "audio", payload)}``).  Chunks then arrive
        on ``{session_subject}.stream.{stream_id}`` exactly as for ``rpc.stream.open``; the
        client registers the id with ``ServiceClient.adopt_stream``.  The ``on_open``
        validator is not called.  Call on the host's loop thread (e.g. from an async rpc
        handler).
        """
        if kind not in self._stream_handlers:
            raise ValueError(f"no stream handler registered for kind {kind!r}")
        if params is not None and not isinstance(params, dict):
            raise TypeError("params must be a dict")
        if self._sessions.get(session.token) is not session:
            raise ValueError("session is not live")
        return self._new_stream(session, kind, dict(params or {}))

    async def end_stream(
        self, stream_id: str, *, final: bool = False, reason: str | None = None
    ) -> bool:
        """End an open stream from service code; False if it is not open.

        ``final=True`` queues the handler's final call (``data=b""``) behind any chunks
        already received, like ``rpc.stream.close`` (default reason ``close``).  Otherwise the
        stream is dropped at once, pending chunks are discarded and the handler gets no final
        call (default reason ``cancel``).  Either way :meth:`on_stream_end` runs once.  Safe to
        call from inside the stream's own handler.
        """
        st = self._streams.get(stream_id)
        if st is None:
            return False
        if final:
            self._queue_final(st, reason or END_CLOSE)
        else:
            await self._drop_stream(stream_id, reason or END_CANCEL)
        return True

    def _new_stream(self, session: Session, kind: str, params: dict) -> str:
        sid = new_token()
        st = StreamState(sid, kind, session.token, params, 0, self._clock())
        st.queue = asyncio.Queue()
        st.task = asyncio.create_task(self._stream_worker(session, st))
        self._streams[sid] = st
        return sid

    def _queue_final(
        self, st: StreamState, reason: str, seq: int | None = None, data: bytes = b""
    ) -> None:
        """Stop accepting chunks for ``st`` and queue its final call behind pending ones."""
        self._streams.pop(st.stream_id, None)
        assert st.queue is not None and st.task is not None
        st.queue.put_nowait((st.next_seq if seq is None else seq, data, True, reason))
        if not st.task.done():
            self._finishing.add(st.task)
            st.task.add_done_callback(self._finishing.discard)

    async def _stream_worker(self, session: Session, st: StreamState) -> None:
        assert st.queue is not None
        while True:
            seq, data, final, reason = await st.queue.get()
            await self._call_stream_handler(session, st, seq, data, final)
            if final:
                await self._stream_ended(session, st, reason)
                return
            if st.drop_reason is not None:  # end_stream() from inside this handler
                await self._stream_ended(session, st, st.drop_reason)
                return

    async def _drop_stream(self, stream_id: str, reason: str) -> None:
        st = self._streams.pop(stream_id, None)
        if st is None:
            return
        task = st.task
        if task is not None and task is asyncio.current_task():
            st.drop_reason = reason  # the worker ends itself after this handler call
            return
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self._stream_ended(None, st, reason)

    async def _stream_ended(self, session: Session | None, st: StreamState, reason: str) -> None:
        if st.ended:
            return
        st.ended = True
        if session is None:
            session = self._sessions.get(st.token) or Session(st.token, "", None, "", 0.0)
        for cb in list(self._stream_end_cbs):
            try:
                await _maybe_await(cb(session, st.stream_id, reason))
            except Exception:
                log.exception("on_stream_end callback failed")

    async def _stream_open(self, session: Session, payload: dict, msg: Any) -> None:
        kind = payload.get("kind")
        params = payload.get("params", {})
        if not isinstance(kind, str) or kind not in self._stream_handlers:
            await self._respond(msg, self._bad(f"unknown stream kind {kind!r}"))
            return
        if not isinstance(params, dict):
            await self._respond(msg, self._bad("params must be an object"))
            return
        validator = self._open_validators.get(kind)
        if validator is not None:
            try:
                await _maybe_await(validator(session, params))
            except asyncio.CancelledError:
                raise
            except ServiceError as e:
                await self._respond(msg, P.svc_error(self.contract, e.code, e.message))
                return
            except Exception as e:
                log.exception("stream %r on_open failed", kind)
                await self._respond(msg, P.svc_error(self.contract, "unknown", str(e)))
                return
        if self._sessions.get(session.token) is not session:
            return  # the session ended while the validator ran
        sid = self._new_stream(session, kind, params)
        await self._respond(msg, P.svc_reply(self.contract, stream_id=sid))

    async def _stream_close(self, session: Session, payload: dict, msg: Any) -> None:
        sid = payload.get("stream_id")
        st = self._streams.get(sid) if isinstance(sid, str) else None
        if st is None or st.token != session.token:
            await self._respond(msg, self._bad(f"unknown stream {sid!r}"))
            return
        self._queue_final(st, END_CLOSE)
        await self._respond(msg, P.svc_reply(self.contract))

    def _on_chunk(self, session: Session, sid: str, msg: Any) -> None:
        st = self._streams.get(sid)
        if st is None or st.token != session.token:
            log.debug("chunk for unknown stream %s… dropped", sid[:6])
            return
        headers = msg.headers or {}
        try:
            seq = int(headers.get(P.HDR_SEQ, ""))
        except ValueError:
            log.warning("stream %s… chunk without a valid %s dropped", sid[:6], P.HDR_SEQ)
            return
        kind = headers.get(P.HDR_KIND)
        if kind and kind != st.kind:  # absent or empty: the sender did not know the kind
            log.warning("stream %s… chunk kind %r != %r dropped", sid[:6], kind, st.kind)
            return
        final = headers.get(P.HDR_FINAL) == "1"
        st.last_chunk = self._clock()
        if seq < st.next_seq:
            log.warning("stream %s… stale seq %d (< %d) dropped", sid[:6], seq, st.next_seq)
            return
        if seq > st.next_seq:
            log.warning("stream %s… gap: expected seq %d, got %d", sid[:6], st.next_seq, seq)
            self.summary["stream_gaps"] = self.summary.get("stream_gaps", 0) + 1
        st.next_seq = seq + 1
        assert st.queue is not None
        if final:
            self._queue_final(st, END_FINAL, seq, bytes(msg.data))
        else:
            st.queue.put_nowait((seq, bytes(msg.data), False, END_FINAL))

    async def _call_stream_handler(
        self, session: Session, st: StreamState, seq: int, data: bytes, final: bool
    ) -> None:
        handler = self._stream_handlers[st.kind]
        try:
            if inspect.iscoroutinefunction(handler):
                await handler(session, st.stream_id, seq, data, final)
            else:
                loop = asyncio.get_running_loop()
                r = await loop.run_in_executor(
                    None, functools.partial(handler, session, st.stream_id, seq, data, final)
                )
                if inspect.isawaitable(r):
                    await r
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("stream %r handler failed", st.kind)

    # -- per-session delivery -----------------------------------------------------------

    async def emit(self, session: Session, name: str, payload: dict) -> None:
        """Publish a per-session event to ``{session.delivery_root}.{name}``.

        ``name`` is one or more subject tokens (e.g. ``utterance.transcript``) and may not
        start with ``stream`` (that suffix carries chunks).  The event carries
        ``instance_id`` and a ``seq`` that is monotonic per session, so the receiving client
        can detect gaps in *its* events.
        """
        _check_event_name(name, allow_stream=False)
        core = self._conn()
        session.event_seq += 1
        body = {**payload, "instance_id": self._instance_id, "seq": session.event_seq}
        await core.publish_json(f"{session.delivery_root}.{name}", body)

    async def publish_event(self, name: str, payload: dict) -> None:
        """Broadcast a public event on ``{root}.events.{name}``.

        Public: anyone on the broker may read it, and anyone may forge one, so use it only for
        non-sensitive notices (e.g. state changes) and let clients fetch authoritative state
        through an rpc.  The event carries ``instance_id`` and a per-instance ``seq``.
        """
        _check_event_name(name, allow_stream=True)
        core = self._conn()
        self._public_seq += 1
        body = {**payload, "instance_id": self._instance_id, "seq": self._public_seq}
        await core.publish_json(P.event(self._root, name), body)

    async def emit_chunk(
        self,
        session: Session,
        stream_id: str,
        seq: int,
        data: bytes,
        final: bool = False,
        *,
        kind: str | None = None,
    ) -> None:
        """Publish a binary chunk to ``{session.delivery_root}.stream.{stream_id}``.

        ``Svc-Kind`` is ``kind`` if given, else the kind of open stream ``stream_id``, else "".
        """
        core = self._conn()
        if kind is None:
            st = self._streams.get(stream_id)
            kind = st.kind if st is not None else ""
        headers = {P.HDR_SEQ: str(seq), P.HDR_KIND: kind, P.HDR_FINAL: "1" if final else "0"}
        await core.publish(f"{session.delivery_root}.stream.{stream_id}", bytes(data), headers)

    # -- rpc dispatch -------------------------------------------------------------------

    async def _dispatch(self, session: Session, action: str, msg: Any) -> None:
        handler = self._handlers.get(action)
        if handler is None:
            await self._respond(
                msg, P.svc_error(self.contract, "unknown", f"unknown action {action!r}")
            )
            return
        try:
            payload = json.loads(msg.data) if msg.data else {}
        except (ValueError, UnicodeDecodeError):
            await self._respond(msg, self._bad("invalid JSON"))
            return
        if not isinstance(payload, dict):
            await self._respond(msg, self._bad("payload must be an object"))
            return
        try:
            if inspect.iscoroutinefunction(handler):
                result = await handler(session, payload)
            else:
                loop = asyncio.get_running_loop()
                result = await loop.run_in_executor(
                    None, functools.partial(handler, session, payload)
                )
                if inspect.isawaitable(result):
                    result = await result
            if result is None:
                result = {}
            if not isinstance(result, dict):
                raise TypeError(f"handler for {action!r} returned {type(result).__name__}")
            clash = _RESERVED_REPLY_KEYS & result.keys()
            if clash:
                raise ValueError(f"handler for {action!r} returned reserved keys {sorted(clash)}")
            reply = P.svc_reply(self.contract, **result)
        except asyncio.CancelledError:
            raise
        except ServiceError as e:
            await self._respond(msg, P.svc_error(self.contract, e.code, e.message))
            return
        except Exception as e:
            log.exception("rpc %r failed", action)
            await self._respond(msg, P.svc_error(self.contract, "unknown", str(e)))
            return
        await self._respond(msg, reply)


def _json_object(msg: Any) -> dict | None:
    try:
        payload = json.loads(msg.data) if msg.data else {}
    except (ValueError, UnicodeDecodeError):
        return None
    return payload if isinstance(payload, dict) else None
