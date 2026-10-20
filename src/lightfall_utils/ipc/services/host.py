"""Qt-free service side of the services pattern: discovery, heartbeat, handshake, sessions, rpc.

A :class:`ServiceHost` runs on one :class:`NatsCore`.  It answers ``{root}.meta.info`` and
``{scope}.svc.discover`` with its info payload, publishes the same payload on
``{root}.heartbeat`` every ``heartbeat_s``, serves the ``{priv_root}.auth.request`` handshake
and dispatches ``{priv_root}.session.{token}.rpc.<action>`` requests to registered handlers.
Sessions expire after ``LEASE_S`` without a ``rpc.session.ping``.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import json
import logging
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

_PING = "session.ping"
_CLOSE = "session.close"
_SWEEP_MAX_S = 1.0


@dataclass
class Session:
    token: str
    app_name: str
    user: str | None
    delivery_root: str
    last_ping: float


def allow_apps(names: Iterable[str]) -> Callable[[dict], bool]:
    """A ``trust`` callable approving only ``auth.request`` payloads whose app_name is listed."""
    allowed = frozenset(names)
    return lambda req: req.get("app_name") in allowed


def _valid_subject_token(tok: Any) -> bool:
    return isinstance(tok, str) and bool(tok) and not any(c in tok for c in ".*> \t\r\n")


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
        self._end_cbs: list[Callable[[Session], Any]] = []
        self._sessions: dict[str, Session] = {}
        self._core: NatsCore | None = None
        self._subs: list[Any] = []
        self._tasks: list[asyncio.Task] = []
        self._rpc_tasks: set[asyncio.Task] = set()
        self._stopped: asyncio.Event | None = None

    # -- properties ---------------------------------------------------------------------

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def priv_root(self) -> str:
        if self._core is None:
            raise RuntimeError("ServiceHost not started")
        return self._core.priv_root

    @property
    def sessions(self) -> dict[str, Session]:
        return dict(self._sessions)

    # -- registration -------------------------------------------------------------------

    def rpc(self, action: str) -> Callable[[Handler], Handler]:
        if action in (_PING, _CLOSE):
            raise ValueError(f"{action!r} is reserved")

        def deco(fn: Handler) -> Handler:
            self._handlers[action] = fn
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
        if self._core is not None:
            raise RuntimeError("ServiceHost already started")
        self._core = core
        self._stopped = asyncio.Event()
        pr = core.priv_root
        self._subs = [
            await core.subscribe(P.meta_info(self._root), self._on_info),
            await core.subscribe(P.discover(self.scope), self._on_info),
            await core.subscribe(P.auth_subject(pr), self._on_auth),
            await core.subscribe(f"{P.session_subject(pr, '*')}.rpc.>", self._on_session),
        ]
        await core.flush()
        self._tasks = [
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._sweep_loop()),
        ]

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
        self._stopped.set()

    async def serve_forever(self) -> None:
        if self._stopped is None:
            raise RuntimeError("ServiceHost not started")
        await self._stopped.wait()

    # -- background loops ---------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        assert self._core is not None
        subject = P.heartbeat(self._root)
        while True:
            try:
                await self._core.publish_json(subject, self.info())
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
        for tok, s in list(self._sessions.items()):
            if now - s.last_ping > P.LEASE_S:
                log.info("session %s… (%s) lease expired", tok[:6], s.app_name)
                await self._end_session(tok)

    async def _end_session(self, token: str) -> None:
        s = self._sessions.pop(token, None)
        if s is None:
            return
        for cb in list(self._end_cbs):
            try:
                r = cb(s)
                if inspect.isawaitable(r):
                    await r
            except Exception:
                log.exception("on_session_end callback failed")

    # -- message handlers ---------------------------------------------------------------

    async def _respond(self, msg: Any, obj: dict) -> None:
        if not msg.reply:
            return
        assert self._core is not None
        await self._core.publish(msg.reply, json.dumps(obj).encode())

    async def _on_info(self, msg: Any) -> None:
        await self._respond(msg, self.info())

    def _deny(self, why: str) -> dict:
        return P.svc_error(self.contract, "denied", why)

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
        if self._stdin_token is not None and payload.get("session_token") != self._stdin_token:
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
            await self._respond(
                msg, P.svc_error(self.contract, "bad_request", "invalid delivery_token")
            )
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
        rest = msg.subject[len(head) :]
        token, _, tail = rest.partition(".rpc.")
        if not tail:
            return
        session = self._live_session(token)
        if session is None:
            if token in self._sessions:
                await self._end_session(token)
            return
        if tail == _PING:
            session.last_ping = self._clock()
            await self._respond(msg, P.svc_reply(self.contract))
            return
        if tail == _CLOSE:
            await self._respond(msg, P.svc_reply(self.contract))
            await self._end_session(token)
            return
        task = asyncio.create_task(self._dispatch(session, tail, msg))
        self._rpc_tasks.add(task)
        task.add_done_callback(self._rpc_tasks.discard)

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
            await self._respond(msg, P.svc_error(self.contract, "bad_request", "invalid JSON"))
            return
        if not isinstance(payload, dict):
            await self._respond(
                msg, P.svc_error(self.contract, "bad_request", "payload must be an object")
            )
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
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("rpc %r failed", action)
            await self._respond(msg, P.svc_error(self.contract, "unknown", str(e)))
            return
        await self._respond(msg, P.svc_reply(self.contract, **result))
