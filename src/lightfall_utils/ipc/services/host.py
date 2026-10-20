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
handler registered with :meth:`ServiceHost.stream`.  rpc requests and chunks share one
subscription, so a ``stream.close`` is handled after every chunk published before it on the
same connection; stream handlers are awaited in order on that subscription, so keep them
quick.
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
from dataclasses import dataclass
from typing import Any

from lightfall_utils.ipc.capability import new_token
from lightfall_utils.ipc.services import protocol as P
from lightfall_utils.ipc.services.core import NatsCore

log = logging.getLogger(__name__)

Handler = Callable[["Session", dict], "dict | Awaitable[dict]"]
StreamHandler = Callable[["Session", str, int, bytes, bool], Any]

_PING = "session.ping"
_CLOSE = "session.close"
_SHUTDOWN = "shutdown"
_STREAM_OPEN = "stream.open"
_STREAM_CLOSE = "stream.close"
_BUILTIN = frozenset({_PING, _CLOSE, _SHUTDOWN, _STREAM_OPEN, _STREAM_CLOSE})
_RESERVED_REPLY_KEYS = frozenset({"contract", "status", "contract_version"})
_SWEEP_MAX_S = 1.0


class InstanceConflictError(RuntimeError):
    """Another live instance of a single-instance service already serves this root."""


@dataclass
class Session:
    token: str
    app_name: str
    user: str | None
    delivery_root: str
    last_ping: float


@dataclass
class StreamState:
    stream_id: str
    kind: str
    token: str
    params: dict
    next_seq: int
    last_chunk: float


def allow_apps(names: Iterable[str]) -> Callable[[dict], bool]:
    """A ``trust`` callable approving only ``auth.request`` payloads whose app_name is listed."""
    allowed = frozenset(names)
    return lambda req: req.get("app_name") in allowed


def _valid_subject_token(tok: Any) -> bool:
    return isinstance(tok, str) and bool(tok) and not any(c in tok for c in ".*> \t\r\n")


def _check_stdin_token(tok: Any) -> None:
    if not _valid_subject_token(tok):
        raise ValueError("stdin_token must be a single non-empty subject token")


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
        self._trust = trust
        self._stdin_token = stdin_token
        self._clock = clock
        self._instance_id = uuid.uuid4().hex
        self._root = P.root(scope, name)
        self.state = "starting"
        self.summary: dict = {}
        self._handlers: dict[str, Handler] = {}
        self._stream_handlers: dict[str, StreamHandler] = {}
        self._streams: dict[str, StreamState] = {}
        self._event_seq = 0
        self._end_cbs: list[Callable[[Session], Any]] = []
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

    def stream(self, kind: str) -> Callable[[StreamHandler], StreamHandler]:
        """Register ``handler(session, stream_id, seq, data, final)`` for stream ``kind``.

        The handler sees ``final=True`` exactly once per stream that ends normally: on a
        chunk whose ``Svc-Final`` is ``"1"``, or on ``rpc.stream.close`` (then with
        ``data=b""`` and the next expected seq).  Streams dropped for idleness, session end
        or host stop are discarded without a final call.
        """
        if not _valid_subject_token(kind):
            raise ValueError(f"invalid stream kind {kind!r}")

        def deco(fn: StreamHandler) -> StreamHandler:
            self._stream_handlers[kind] = fn
            return fn

        return deco

    def on_session_end(self, cb: Callable[[Session], Any]) -> None:
        self._end_cbs.append(cb)

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
        heartbeat interval can race.
        """
        if self._core is not None:
            raise RuntimeError("ServiceHost already started")
        if self.single_instance:
            await self._check_single_instance(core)
        self._core = core
        self._stopped = asyncio.Event()
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
        while True:
            try:
                await self._conn().publish_json(subject, self.info())
            except Exception:
                log.warning("heartbeat publish failed", exc_info=True)
            await asyncio.sleep(self.heartbeat_s)

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
                self._streams.pop(sid, None)
        for tok, s in list(self._sessions.items()):
            if now - s.last_ping > P.LEASE_S:
                log.info("session %s… (%s) lease expired", tok[:6], s.app_name)
                await self._end_session(tok)

    async def _end_session(self, token: str) -> None:
        s = self._sessions.pop(token, None)
        if s is None:
            return
        for sid, st in list(self._streams.items()):
            if st.token == token:
                self._streams.pop(sid, None)
        for cb in list(self._end_cbs):
            try:
                r = cb(s)
                if inspect.isawaitable(r):
                    await r
            except Exception:
                log.exception("on_session_end callback failed")

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
            await self._end_session(token)
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
                await self._end_session(token)
            return
        if what == "stream":
            await self._on_chunk(session, tail, msg)
            return
        if tail == _PING:
            session.last_ping = self._clock()
            await self._respond(msg, P.svc_reply(self.contract))
            return
        if tail == _CLOSE:
            await self._respond(msg, P.svc_reply(self.contract))
            await self._end_session(token)
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
                await self._stream_open(session, payload, msg)
            else:
                await self._stream_close(session, payload, msg)
            return
        task = asyncio.create_task(self._dispatch(session, tail, msg))
        self._rpc_tasks.add(task)
        task.add_done_callback(self._rpc_tasks.discard)

    # -- streams ------------------------------------------------------------------------

    async def _stream_open(self, session: Session, payload: dict, msg: Any) -> None:
        kind = payload.get("kind")
        params = payload.get("params", {})
        if not isinstance(kind, str) or kind not in self._stream_handlers:
            await self._respond(msg, self._bad(f"unknown stream kind {kind!r}"))
            return
        if not isinstance(params, dict):
            await self._respond(msg, self._bad("params must be an object"))
            return
        sid = new_token()
        self._streams[sid] = StreamState(sid, kind, session.token, params, 0, self._clock())
        await self._respond(msg, P.svc_reply(self.contract, stream_id=sid))

    async def _stream_close(self, session: Session, payload: dict, msg: Any) -> None:
        sid = payload.get("stream_id")
        st = self._streams.get(sid) if isinstance(sid, str) else None
        if st is None or st.token != session.token:
            await self._respond(msg, self._bad(f"unknown stream {sid!r}"))
            return
        self._streams.pop(st.stream_id, None)
        await self._call_stream_handler(session, st, st.next_seq, b"", True)
        await self._respond(msg, P.svc_reply(self.contract))

    async def _on_chunk(self, session: Session, sid: str, msg: Any) -> None:
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
        if kind is not None and kind != st.kind:
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
        if final:
            self._streams.pop(sid, None)
        await self._call_stream_handler(session, st, seq, bytes(msg.data), final)

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

        The event carries ``instance_id`` and a per-instance monotonic ``seq``.
        """
        core = self._conn()
        self._event_seq += 1
        body = {**payload, "instance_id": self._instance_id, "seq": self._event_seq}
        await core.publish_json(f"{session.delivery_root}.{name}", body)

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
